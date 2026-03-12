const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'ecommerce-services',
  brokers: ['localhost:9092'] // Assumes Kafka is running locally
});

const producer = kafka.producer();

// 5 Separate Consumer Groups to simulate 5 independent Microservices
const paymentConsumer = kafka.consumer({ groupId: 'payment-group' });
const inventoryConsumer = kafka.consumer({ groupId: 'inventory-group' });
const emailConsumer = kafka.consumer({ groupId: 'email-group' });
const analyticsConsumer = kafka.consumer({ groupId: 'analytics-group' });
const shippingConsumer = kafka.consumer({ groupId: 'shipping-group' });

async function startServices() {
    await producer.connect();
    console.log("✅ Services Producer connected");

    // 1. Payment Service
    await paymentConsumer.connect();
    await paymentConsumer.subscribe({ topic: 'order-events', fromBeginning: false });
    paymentConsumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            if (data.event === 'OrderPlaced') {
                console.log(`💳 [Payment Service] Processing payment for Order ${data.orderId}...`);
                // Simulate processing time
                setTimeout(async () => {
                    const paymentEvent = { ...data, event: 'PaymentCompleted' };
                    await producer.send({
                        topic: 'order-events',
                        messages: [{ value: JSON.stringify(paymentEvent) }]
                    });
                    console.log(`💳 [Payment Service] Payment completed for Order ${data.orderId}`);
                }, 2000); // 2 second delay
            }
        }
    });

    // 2. Inventory Service
    await inventoryConsumer.connect();
    await inventoryConsumer.subscribe({ topic: 'order-events', fromBeginning: false });
    inventoryConsumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            if (data.event === 'OrderPlaced') {
                console.log(`📦 [Inventory Service] Reducing stock for Product ${data.productId} (Qty: ${data.quantity})`);
            }
        }
    });

    // 3. Email Service
    await emailConsumer.connect();
    await emailConsumer.subscribe({ topic: 'order-events', fromBeginning: false });
    emailConsumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            if (data.event === 'OrderPlaced') {
                console.log(`📧 [Email Service] Sending order confirmation email to User ${data.userId}`);
            }
        }
    });

    // 4. Analytics Service
    await analyticsConsumer.connect();
    await analyticsConsumer.subscribe({ topic: 'order-events', fromBeginning: false });
    analyticsConsumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            if (data.event === 'OrderPlaced') {
                console.log(`📊 [Analytics Service] Updating sales dashboard - Revenue +$${data.price}`);
            }
        }
    });

    // 5. Shipping Service
    await shippingConsumer.connect();
    await shippingConsumer.subscribe({ topic: 'order-events', fromBeginning: false });
    shippingConsumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            
            if (data.event === 'PaymentCompleted') {
                console.log(`🚚 [Shipping Service] Preparing shipment for Order ${data.orderId}...`);
                
                // Simulate Shipped Event
                setTimeout(async () => {
                    const shippedEvent = { ...data, event: 'OrderShipped' };
                    await producer.send({
                        topic: 'order-events',
                        messages: [{ value: JSON.stringify(shippedEvent) }]
                    });
                    console.log(`🚚 [Shipping Service] Order ${data.orderId} Shipped!`);
                    
                    // Simulate Out For Delivery Event
                    setTimeout(async () => {
                         const outEvent = { ...data, event: 'OutForDelivery' };
                         await producer.send({
                             topic: 'order-events',
                             messages: [{ value: JSON.stringify(outEvent) }]
                         });
                         console.log(`🚀 [Shipping Service] Order ${data.orderId} Out for Delivery!`);

                         // Simulate Delivered Event
                         setTimeout(async () => {
                             const delEvent = { ...data, event: 'Delivered' };
                             await producer.send({
                                 topic: 'order-events',
                                 messages: [{ value: JSON.stringify(delEvent) }]
                             });
                             console.log(`✅ [Shipping Service] Order ${data.orderId} Delivered!`);
                         }, 3000); // Delivered in 3s
                    }, 3000); // Out for delivery in 3s
                }, 3000); // Shipped in 3s
            }
        }
    });

    console.log("🚀 All Microservices Started and Listening to Kafka 🚀");
}

startServices().catch(console.error);
