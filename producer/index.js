const {Kafka, Partitioners} = require("kafkajs");

const kafka = new Kafka({
    clientId: "my-app", brokers: ["localhost:39092", "localhost:39093", "localhost:39094"],
});

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
});

async function producerRun() {
    await producer.connect();
    const res = [];
    for (let i = 0; i < 2; i++) {
        const record = producer.send({
            topic: "products",
            messages: [{key: "productChange", value: JSON.stringify({id: i, price: 50.0, quantity: 1})}],
        });
        res.push(record);
    }
    const producerRecords = await Promise.all(res);
    await producer.disconnect();

    console.log(`first message meta: ${JSON.stringify(producerRecords[0])}`);
    console.log(`last message meta: ${JSON.stringify(producerRecords[producerRecords.length - 1])}`);
}

producerRun();
