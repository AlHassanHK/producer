require('dotenv').config();
const { Kafka } = require('kafkajs')
const validate = require('../validation/kafka');
const messages = require('../constants/messages');

const kafka = new Kafka({
  clientId: `${process.env.CLIENT_ID}-${process.env.ENV}`,
  brokers: [process.env.KAFKA_BROKERS],
  ssl: true,
  logLevel: 2,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_SASL_PASSWORD
  },
});

const topic = `${process.env.TOPIC_FIFA_TICKET_SALES}-${process.env.ENV}`;
const producer = kafka.producer();

const startKafkaProducer = async () => {
  try {
    await producer.connect()
  } catch (e) {
    console.log('Unable to connect producer with Kafka:', e.message);
  }
};

const sendKafkaMessage = async (messageType, message) => {
  console.log("<function call>: sendKafkaMessage - line 29 | paremeters: messageType:", messageType, " | message:", message );
  // validate kafka message against schema prior to sending
  const validationError = validate.kafkaMessage(message);
  if (validationError) {
    console.log("validtion error- line 29")
    return Promise.reject(validationError);
  }

  // send message to kafka broker
  let attempts = 0;
  do {
    try {
      await producer.send({ topic, messages: [{ value: JSON.stringify(message) }] });
      console.log("<function call>: producer.send - line 41 | paremeters: topic:", topic, "message: ", messages,"value:", messages[value] );
    } catch (e) {
      console.log('e.message:', e.message)
      if (e.message === 'The producer is disconnected') {
        console.log("producer disconnected - line 45 - ", e.message);
        await startKafkaProducer();
      }
      attempts++;
      console.log("attempt count:", attempts);
      continue; // try and send message again
    }
    break; // message was sent so exit the loop
  } while (attempts > 3);

  // exit
  return attempts > 3 ? Promise.reject('could not send message') : Promise.resolve();
};

module.exports = {
  startKafkaProducer,
  sendKafkaMessage,
};