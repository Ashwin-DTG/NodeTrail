const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "order-service",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function run() {
    await producer.connect();

    const order = {
        orderId: 101,
        user: "Ashwin",
        product: "Laptop",
        price: 75000
    };

    await producer.send({
        topic: "order-topic",
        messages: [
            { value: JSON.stringify(order) }
        ],
    });

    console.log("Order event sent:", order);

    await producer.disconnect();
}

run();