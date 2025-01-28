const dotenv = require('dotenv');
const path = require('path');
const Joi = require('joi');

// Load environment variables

dotenv.config({ path: path.join(__dirname, `../../${'.env'}`) });

// validate env with given schema
const envVarsSchema = Joi.object()
    .keys({
        NODE_ENV: Joi.string().required(),
        SERVICE: Joi.string().required(),
        PORT: Joi.number().default(4000),
        MONGODB_URL: Joi.string().required(),
        RABBITMQ_URL: Joi.string().required(),
    })
    .unknown();

//load errors or environment variables 
const { value: envVars, error } = envVarsSchema.prefs({ errors: { label: 'key' } }).validate(process.env);

// throw error after validating env
if (error) {
    throw new Error(`Config validation error: ${error.message}`);
}

// export variables for accessing it in application
module.exports = {
    env: envVars.NODE_ENV,
    port: envVars.PORT,
    service: envVars.SERVICE,
    rabbitmq: envVars.RABBITMQ_URL,
    verifyEmailExpirationMinutes: envVars.EMAIL_VERIFY_OTP_EXPIRATION_MINUTES,
    mongoose: {
        url: envVars.MONGODB_URL,
        options: {
        },
    }
};
