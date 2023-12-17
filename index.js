const { Kafka } = require("kafkajs");
const { uuidv7 } = require("uuidv7");

const kafka = new Kafka({
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const createTopic = async () => {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic: "sale_events",
        numPartitions: 1,
        replicationFactor: 1,
      },
    ],
  });
  await admin.disconnect();
};

const run = async () => {
  try {
    await createTopic();
  } catch (e) {
    console.log(e);
  }
  // Producing
  await producer.connect();
  const prices = {
    1: 100,
    2: 200,
    3: 300,
    4: 400,
    5: 500,
    6: 600,
    7: 700,
    8: 800,
    9: 900,
    10: 1000,
  };

  while (true) {
    const product_id = getRandomInt(1, 10);
    const price = prices[product_id];

    const value = JSON.stringify({
      id: uuidv7(),
      created_at: new Date(),
      customer_id: getRandomInt(1, 10),
      product_id,
      promotion_id: product_id,
      price,
      quantity: getRandomInt(1, 3),
    });
    console.log(value);
    producer.send({
      topic: "sale_events",
      messages: [
        {
          value,
        },
      ],
    });

    await new Promise((resolve) => {
      setTimeout(() => {
        resolve();
      }, getRandomInt(100, 3000));
    });
  }
  // await producer.disconnect();
};

async function main() {
  run().catch(console.error);
}

main();
