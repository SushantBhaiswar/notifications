const mongoose = require('mongoose');
const app = require('./app');
const config = require('./config/config');
const logger = require('./config/logger');
const rabbitmq = require('./rabbitmq')
const consumer = require('./consumer')
let server;
mongoose.connect(config.mongoose.url, config.mongoose.options).then(() => {
  logger.info(`Connected to MongoDB : ${config.env}`);
  server = app.listen(config.port, () => {
    logger.info(`Listening to port ${config.port}`);
  });
});

// connecting to message queue
rabbitmq.initializeRabbitMQ(config)
  .then((res) => {
    logger.info('Connected to RabbitMQ');
    consumer.initializeConsumer()
  })
  .catch((error) => {
    console.log("🚀 ~ error:", error)
  });

const exitHandler = () => {
  if (server) {
    server.close(() => {
      logger.info('Server closed');
      process.exit(1);
    });
  } else {
    process.exit(1);
  }
};

const unexpectedErrorHandler = (error) => {
  logger.error(error);
  exitHandler();
};

process.on('uncaughtException', unexpectedErrorHandler);
process.on('unhandledRejection', unexpectedErrorHandler);

process.on('SIGTERM', () => {
  logger.info('SIGTERM received');
  if (server) {
    server.close();
  }
});
module.exports = { mongoose }