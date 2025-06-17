import express from 'express';
import cors from 'cors';
import http from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import { getSudoku } from 'sudoku-gen';

const kafka = new Kafka({
  clientId: 'backend',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();
await producer.connect();
const admin = kafka.admin();
await admin.connect();
const roomControl = new Map<string, number>();
const boardState = new Map<string, any>();

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
    allowedHeaders: ['Content-Type'],
    credentials: true
  }
});

io.on('connection', async (socket) => {
  console.log('New client connected', socket.id);

  socket.on('joinRoom', async (data) => {
    if (data.roomId === socket.id) {
      await admin.createTopics({
        topics: [{ topic: socket.id, numPartitions: 1 }],
        validateOnly: false
      });
      boardState.set(socket.id, getSudoku(data.difficulty || 'easy'));
      console.log(data.difficulty)
    }
    console.log(`Client ${socket.id} joined room: ${data.roomId}`);
    roomControl.set(data.roomId, (roomControl.get(data.roomId) || 0) + 1);
    socket.emit('board-state', boardState.get(data.roomId));

    const clientConsumer = kafka.consumer({ groupId: `client-group-${socket.id}` });

    await clientConsumer.connect();
    await clientConsumer.subscribe({ topic: data.roomId });

    clientConsumer.run({
      eachMessage: async ({ message }) => {
        socket.emit('kafka-message', {
          key: message.key?.toString(),
          value: message.value?.toString(),
          timestamp: message.timestamp,
        });
      },
    });

    socket.on('disconnect', async () => {
      console.log(`Client ${socket.id} disconnected`);
      await clientConsumer.disconnect();
      console.log(`Disconnected consumer for client ${socket.id}`);
      roomControl.set(data.roomId, (roomControl.get(data.roomId) || 0) - 1);
      if (roomControl.get(data.roomId) === 0) {
        console.log(`Deleted topic for room ${data.roomId}`);
        roomControl.delete(data.roomId);
        await admin.deleteTopics({ topics: [data.roomId] });
      }
      boardState.delete(data.roomId);
    });
  });
  socket.on('send-message', async (data) => {
    const { roomId, key, value } = data;
    await producer.send({
      topic: roomId,
      messages: [
        { key: key.toString(), value: value.toString() },
      ],
    });
  });
});

server.listen(3000, '127.0.0.1', () => {
  console.log('Server running on http://127.0.0.1:3000');
});
app.use(cors());
app.use(express.json());
app.get('/', (req, res) => {
  res.send('Hello World!');
});

const gracefulShutdown = async () => {
  await admin.listTopics().then(async (topics) => {
    for (const topic of topics) {
      if (topic !== '__consumer_offsets') {
        try {
          await admin.deleteTopics({ topics: [topic] });
        } catch (error) {
          console.error(`Error deleting topic ${topic}:`, error);
        }
      }
    }
  });
  await producer.disconnect();
  await admin.disconnect();
  server.close(() => {
    console.log('HTTP server closed.');
    process.exit(0);
  });
};

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

export default app;