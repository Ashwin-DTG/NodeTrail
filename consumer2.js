const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "analytics-service",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "analytics-group" });

async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: "order-topic", fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const order = JSON.parse(message.value.toString());

            console.log("Analytics Service:");
            console.log(`Recording order ${order.orderId} worth ${order.price}`);
        },
    });
}

run();