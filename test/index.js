const {Kafka} = require("kafkajs");

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:39092", "localhost:39093", "localhost:39094"],
});

async function consumerRun() {
    const consumer = kafka.consumer({groupId: 'ggg'})

    await consumer.connect()
    await consumer.subscribe({topic: 'products', fromBeginning: true})

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log({
                topic,
                partition,
                message,
                value: message.value.toString(),
            })
        },
    })
}

consumerRun();

