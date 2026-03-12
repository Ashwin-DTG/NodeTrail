const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "email-service",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "email-group" });

async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: "order-topic", fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const order = JSON.parse(message.value.toString());

            console.log("Email Service:");
            console.log(`Sending order confirmation to ${order.user}`);
        },
    });
}

run();