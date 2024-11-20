const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function consumerStart() {
  let consumer;
  let stopped = false;

  // Initialization
  consumer = new Kafka().consumer({
    'bootstrap.servers': 'localhost:39092,localhost:39093,localhost:39094',
    'group.id': 'test',
    'auto.offset.reset': 'earliest',
  });

  await consumer.connect();
  await consumer.subscribe({ topics: ["products"] });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value.toString(),
      });
    }
  });

  // Update stopped whenever we're done consuming.
  // The update can be in another async function or scheduled with setTimeout etc.
  while(!stopped) {
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  await consumer.disconnect();
}

consumerStart();