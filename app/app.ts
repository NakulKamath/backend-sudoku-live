import express from 'express';
import cors from 'cors';
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'backend',
  brokers: ['localhost:9092']
});

console.log()
const producer = kafka.producer();

(async () => {
  try {
    await producer.connect();
  } catch (error) {
    console.error('Error connecting producer:', error);
    process.exit(1);
  }
  setInterval(async () => {
    try {
      await producer.send({
        topic: 'test-topic',
        messages: [
          { key: 'key1', value: 'Hello KafkaJS' },
        ]
      });
    } catch (error) {
      console.error('Error sending message:', error);
    }
  }, 5000);
})();

const app = express();
app.listen(3000, '127.0.0.1', () => {
  console.log('Server running on http://127.0.0.1:3000');
});
app.use(cors());
app.use(express.json());

export default app;