const { Kafka } = require("kafkajs");
const kafka = new Kafka({
  brokers: ["localhost:9092"],
});

async function run() {
  const consumer = kafka.consumer({ groupId: "sales_events_roii_default" });
  await consumer.connect();
  await consumer.subscribe({ topic: "promotions_stat_60mins", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
}

run().catch(console.log);
