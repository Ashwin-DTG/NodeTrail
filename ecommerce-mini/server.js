const express = require('express');
const { Kafka } = require('kafkajs');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(express.json());
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const kafka = new Kafka({
    clientId: 'order-api',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

// SSE Clients list
let clients = [];

// Standard consumer to stream events to frontend via SSE
const streamConsumer = kafka.consumer({ groupId: 'ui-stream-group' });

async function setupKafka() {
    await producer.connect();
    console.log('✅ API Producer connected');

    await streamConsumer.connect();
    await streamConsumer.subscribe({ topic: 'order-events', fromBeginning: false });
    console.log('✅ UI Stream Consumer connected');

    await streamConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventData = message.value.toString();
            console.log(`[SSE Stream] Broadcasting event to UI: ${eventData}`);
            clients.forEach(client => {
                client.res.write(`data: ${eventData}\n\n`);
            });
        }
    });
}

setupKafka().catch(console.error);

// 1. User Places Order Form API
app.post('/api/orders', async (req, res) => {
    const { userId, productId, quantity } = req.body;

    // Simulate DB save and generate orderId
    const orderId = Math.floor(Math.random() * 10000);
    const price = 50000;

    // Create the event
    const event = {
        event: "OrderPlaced",
        orderId,
        userId,
        productId: productId || 101,
        quantity: quantity || 1,
        price
    };

    // Publish to Kafka
    await producer.send({
        topic: 'order-events',
        messages: [{ value: JSON.stringify(event) }]
    });

    console.log(`[API] Received HTTP Request: Order ${orderId} placed successfully.`);
    res.json({ success: true, orderId, message: "Order placed" });
});

// 2. Real-time Status Stream for UI (Server-Sent Events)
app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const clientId = Date.now();
    clients.push({ id: clientId, res });

    req.on('close', () => {
        clients = clients.filter(c => c.id !== clientId);
    });
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`🚀 API Server running on http://localhost:${PORT}`);
});
