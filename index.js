const { Kafka } = require("kafkajs");

// Kafka client
const kafka = new Kafka({
  clientId: "simple-producer-consumer-application",
  brokers: ["localhost:9092"],
});

// Kafka Producer
const producerRun = async () => {
  const producer = kafka.producer();

  await producer.connect();

  await producer.send({
    topic: "simple-topic",
    messages: [
      {
        value: "My First Nodejs Message",
      },
    ],
  });

  await producer.send({
    topic: "simple-topic",
    messages: [
      {
        value: "My Second Nodejs Message",
      },
    ],
  });

  await producer.disconnect();
};

// Kafka Consumer
const startConsumer = async () => {
  const consumer = kafka.consumer({
    groupId: "simple-group",
  });

  await consumer.connect();

  await consumer.subscribe({ topic: "simple-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({ topic, partition, message: message.value.toString() });
    },
  });
};

producerRun().then(() => {
  startConsumer();
});
