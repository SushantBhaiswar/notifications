const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const { getQueuesName } = require('../rabbitmq/store/queues')
const { getExchangesName } = require('../rabbitmq/store/exchanges')
const queuesname = getQueuesName('org')
const exchangesname = getExchangesName('org')
const config = require('../config/config')
class RabbitMQManager {
    constructor() {
        this.connection = null;
        this.channel = null;
        this.connectionUrl = 'amqp://rabbitmq';
        this.exchanges = {};
        this.queues = {};
        this.socketIO = null;
    }

    // Initialize connection and setup
    async connect() {
        if (this.connection) return this.connection;

        try {
            this.connection = await amqp.connect(this.connectionUrl);
            this.channel = await this.connection.createChannel();

            // Handle connection closure
            this.connection.on('close', () => {
                console.error('RabbitMQ connection closed');
                this.resetConnection();
            });

            return this.connection;
        } catch (error) {
            console.error('RabbitMQ connection error:', error);
            throw error;
        }
    }

    // Reset connection state
    resetConnection() {
        this.connection = null;
        this.channel = null;
        this.exchanges = {};
        this.queues = {};
    }


    // Create an exchange
    async createExchange(name, type = 'topic', options = {}) {
        if (!this.channel) await this.connect();

        await this.channel.assertExchange(name, type, {
            durable: true,
            ...options
        });

        this.exchanges[name] = { name, type, options };
        return this.channel;
    }

    // Create a queue
    async createQueue(name, options = {}) {
        if (!this.channel) await this.connect();

        const queueOptions = {
            durable: true,
            autoDelete: false,
            ...options
        };

        const { queue } = await this.channel.assertQueue(name, queueOptions);
        this.queues[name] = { name: queue, options: queueOptions };
        return queue;
    }

    async retry(msg, callback, error) {
        const data = msg.content

        if (!this.channel) await this.connect();

        // Increment retry count
        const updatedRetryCount = data.retryCount + 1;

        if (updatedRetryCount > 3) {
            await callback({ ...data, status: 'failed' })
        } else {
            // Calculate exponential backoff delay
            const delay = 1000 * Math.pow(2, updatedRetryCount);
            data.retryData.push({ retry: updatedRetryCount, reason: error.message })
            const updatedData = {
                ...data, retryCount: updatedRetryCount, retriedAt: new Date(),

            }
            // update db about retry
            try {
                await callback(updatedData);
            } catch (callbackError) {
                console.error('Callback execution failed:', callbackError);
            }
            // Send message to the retry queue with expiration
            this.channel.publish(
                exchangesname['retry'],
                msg.fields.routingKey,
                Buffer.from(JSON.stringify(updatedData)),
                {
                    expiration: delay.toString(),
                    persistent: true,
                    messageId: msg.properties.messageId || undefined,
                    headers: {
                        'x-delay': delay,
                    },
                }
            );

        }
    }
    // Bind queue to exchange
    async bindQueue(queueName, exchangeName, routingKey = '#') {
        if (!this.channel) await this.connect();

        await this.channel.bindQueue(queueName, exchangeName, routingKey);
    }

    // Publish message to an exchange
    async publish(exchangeName, routingKey, message, options = {}) {
        if (!this.channel) await this.connect();

        const content = Buffer.from(
            typeof message === 'string'
                ? message
                : JSON.stringify(message)
        );

        const publishOptions = {
            persistent: true,
            correlationId: uuidv4(),
            timestamp: Date.now(),
            ...options
        };

        const result = this.channel.publish(
            exchangeName,
            routingKey,
            content,
            publishOptions
        );


        return result;
    }

    // Consume messages from a queue
    async consume(queueName, callback, options = {}) {
        if (!this.channel) await this.connect();

        const consumerOptions = {
            //noAck: false,
            ...options
        };

        await this.channel.consume(queueName, async (msg) => {
            if (msg !== null) {
                try {
                    const content = msg.content.toString();
                    const parsedContent = this.tryParseJSON(content);

                    // Execute callback
                    const response = await callback({
                        content: parsedContent || content,
                        fields: msg.fields,
                        properties: msg.properties,
                        channel: this.channel
                    });

                    // Acknowledge message
                    this.channel.ack(msg);

                } catch (error) {
                    console.error('Message processing error:', error);
                }
            }
        }, consumerOptions);
    }


    // RPC (Remote Procedure Call) pattern
    async rpcClient(exchangeName, routingKey, message, timeout = 5000) {
        if (!this.channel) await this.connect();

        // Create a temporary queue for responses
        const { queue: replyQueue } = await this.channel.assertQueue('', {
            exclusive: true
        });

        const correlationId = uuidv4();

        return new Promise((resolve, reject) => {
            // Set up response listener
            this.channel.consume(replyQueue, (msg) => {
                if (msg.properties.correlationId === correlationId) {
                    const content = msg.content.toString();
                    resolve(this.tryParseJSON(content) || content);
                    this.channel.ack(msg);
                }
            });

            // Publish RPC request
            this.channel.publish(
                exchangeName,
                routingKey,
                Buffer.from(JSON.stringify(message)),
                {
                    correlationId,
                    replyTo: replyQueue,
                    timestamp: Date.now()
                }
            );

            // // Timeout handling
            // setTimeout(() => {
            //     reject(new Error('RPC request timed out'));
            // }, timeout);
        });
    }

    async getChannel() {
        if (!this.channel) {
            await this.connect();
        }
        return this.channel;
    }
    // Utility method to safely parse JSON
    tryParseJSON(jsonString) {
        try {
            return JSON.parse(jsonString);
        } catch (error) {
            return null;
        }
    }

    // Close connection
    async close() {
        if (this.channel) {
            await this.channel.close();
        }
        if (this.connection) {
            await this.connection.close();
        }
        this.resetConnection();
    }
}

module.exports = { RabbitMQManager }

