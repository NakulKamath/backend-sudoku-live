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
const roomControl = new Map<string, Set<string>>();
const roomConsumer = new Map<string, ConsumerRunner>();
const boardState = new Map<string, any>();
const prevPosition = new Map<string, number>();

type BoardCell = {
  mutable: boolean;
  value: number | null;
  correct?: boolean | null;
  notes?: number[];
  users?: string[];
}

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

class ConsumerRunner {
  consumer: any;
  private roomId: string

  async init(socket: any, roomId: string) {
    const clientConsumer = kafka.consumer({ groupId: `client-group-${socket.id}` });

    await clientConsumer.connect();
    await clientConsumer.subscribe({ topic: roomId });

    this.consumer = clientConsumer;
    this.roomId = roomId;
  }

  async run() {
    await this.consumer.run({
      eachMessage: async ({message }) => {
        const sockets = roomControl.get(this.roomId);
        if (sockets) {
          for (const socketId of sockets) {
            const clientSocket = io.sockets.sockets.get(socketId);
            if (clientSocket) {
              clientSocket.emit('kafka-message', {
                key: message.key?.toString(),
                value: message.value?.toString(),
                timestamp: message.timestamp,
              });
            }
          }
        }
      },
    });
  }
}

io.on('connection', async (socket) => {
  console.log('New client connected', socket.id);

  socket.on('check-room', (data, callback) => {
    const exists = boardState.has(data.roomId);
    callback({ exists });
  });
  socket.on('reset-room', async (data) => {
    if (boardState.has(data.roomId)) {
      boardState.delete(data.roomId);
      await admin.deleteTopics({ topics: [data.roomId] });
      const consumer = roomConsumer.get(data.roomId);
      if (consumer) {
        await consumer.consumer.disconnect();
        roomConsumer.delete(data.roomId);
        console.log(`Disconnected consumer for room ${data.roomId}`);
      }
      roomControl.delete(data.roomId);
      console.log(`Deleted topic for room ${data.roomId}`);
    }
  });
  socket.on('join-room', async (data) => {
    if (data.roomId === socket.id) {
      if (boardState.has(socket.id)) {
        console.log(`Client ${socket.id} rejoined room: ${data.roomId}`);
      } else {
        await admin.createTopics({
          topics: [{ topic: socket.id, numPartitions: 1 }],
          validateOnly: false
        });
        boardState.set(socket.id, getSudoku(data.difficulty || 'easy'));
        const puzzle: string = boardState.get(socket.id).puzzle;
        const state: BoardCell[] = Array.from(puzzle).map((char) => {
          if (char === '-') {
              return {
              mutable: true,
              value: null,
              correct: null,
              notes: [],
              users: [],
              };
          } else {
            return {
              mutable: false,
              value: Number(char),
            };
          }
        });
        boardState.get(socket.id).state = state;
        boardState.get(socket.id).mistakes = [0, data.difficulty === 'easy' ? 5 : data.difficulty === 'medium' ? 3 : data.difficulty === 'hard' ? 2 : 1];
        boardState.get(socket.id).total = Array.from(puzzle).filter(char => char === '-').length;
      }
    }
    console.log(`Client ${socket.id} joined room: ${data.roomId}`);
    roomControl.set(data.roomId, (roomControl.get(data.roomId) || new Set<string>).add(socket.id));
    if (!roomConsumer.has(data.roomId)) {
      const clientConsumer = new ConsumerRunner();
      await clientConsumer.init(socket, data.roomId);
      roomConsumer.set(data.roomId, clientConsumer);
      await clientConsumer.run();
      console.log(`Created consumer for room ${data.roomId}`);
    }
    socket.emit('board-state', boardState.get(data.roomId).state);
    socket.emit('mistakes', boardState.get(data.roomId).mistakes);

    socket.on('disconnect', async () => {
      console.log(`Client ${socket.id} disconnected`);
      const set = roomControl.get(data.roomId) || new Set<string>();
      set.delete(socket.id);
      roomControl.set(data.roomId, set);
      if (set.size === 0) {
        console.log(`Deleted topic for room ${data.roomId}`);
        roomControl.delete(data.roomId);
        await admin.deleteTopics({ topics: [data.roomId] });
        const consumer = roomConsumer.get(data.roomId);
        if (consumer) {
          await consumer.consumer.disconnect();
          roomConsumer.delete(data.roomId);
          console.log(`Disconnected consumer for room ${data.roomId}`);
        }
        boardState.delete(data.roomId);
      }
      if (prevPosition.has(socket.id)) {
        const state = boardState.get(data.roomId)?.state;
        if (state) {
          for (const cell of state) {
            if (cell.users) {
              cell.users = cell.users.filter(user => user !== socket.id);
            }
          }
        }
        producer.send({
          topic: data.roomId,
          messages: [
            { key: socket.id + '/pos', value: prevPosition.get(socket.id)?.toString() + '/' },
          ],
        });
        prevPosition.delete(socket.id);
      }
    });
  });
  socket.on('send-message', async (data) => {
    const { roomId, value, name } = data;
    await producer.send({
      topic: roomId,
      messages: [
        { key: socket.id + '/' + name + '/msg', value: value },
      ],
    });
  });
  socket.on('send-event', async (data) => {
    const { roomId, event, name } = data;
    if (!boardState.has(roomId) || boardState.get(roomId).total === 0) {
      return;
    }
    if (boardState.has(roomId)) {
      const state = boardState.get(roomId).state;
      if (event.type === 'updateCell') {
        const { index, value } = event;
        const answer = boardState.get(roomId).solution;
        if (state[index].mutable) {
          state[index].value = value;
          state[index].correct = answer[index] === value.toString();
          await producer.send({
            topic: roomId,
            messages: [
              { key: socket.id + '/' + name + '/move', value: index.toString() + '/' + value.toString() + '/' + (state[index].correct ? '1' : '0') },
            ],
          });
          if (state[index].correct) {
            boardState.get(roomId).total--;
            if (boardState.get(roomId).total === 0) {
              const sockets = roomControl.get(roomId);
              if (sockets) {
                for (const socketId of sockets) {
                  const clientSocket = io.sockets.sockets.get(socketId);
                  if (clientSocket) {
                  clientSocket.emit('win');
                  }
                }
              }
            }
          } else {
            boardState.get(roomId).mistakes[0]++;
            if (boardState.get(roomId).mistakes[0] >= boardState.get(roomId).mistakes[1]) {
              const sockets = roomControl.get(roomId);
              if (sockets) {
                for (const socketId of sockets) {
                  const clientSocket = io.sockets.sockets.get(socketId);
                  if (clientSocket) {
                    clientSocket.emit('lose');
                  }
                }
              }
            }
          }
        }
      } else if (event.type === 'addNote') {
        const { index, note } = event;
        if (state[index].mutable) {
          state[index].notes.push(note);
          await producer.send({
            topic: roomId,
            messages: [
              { key: socket.id + '/' + name + '/addNote', value: index.toString() + '/' + note.toString() },
            ],
          });
        }
      } else if (event.type === 'removeNote') {
        const { index, note } = event;
        if (state[index].mutable) {
          state[index].notes = state[index].notes.filter((n: number) => n !== note);
          await producer.send({
            topic: roomId,
            messages: [ { key: socket.id + '/' + name + '/removeNote', value: index.toString() + '/' + note.toString() }],
          });
        }
      } else if (event.type === 'movePosition') {
        const { prev, index } = event;
        if (!index) {
          if (prev) {
            state[prev].users = state[prev].users.filter(user => user !== name);
          }
          await producer.send({
            topic: roomId,
            messages: [ { key: socket.id + '/' + name + '/pos', value: (prev.toString() + '/') }],
          });
        } else if (state[index].mutable) {
          if (prev) {
            state[prev].users = state[prev].users.filter(user => user !== name);
          }
          state[index].users.push(name);
          const prevStr = prev ? prev.toString() : ''; 
          await producer.send({
            topic: roomId,
            messages: [ { key: socket.id + '/' + name + '/pos', value: (prevStr + '/' + index.toString()) }],
          });
          prevPosition.set(socket.id, index);
        }
      } else if (event.type === 'clear') {
        const { index } = event;
        if (state[index].mutable) {
          state[index].value = null;
          state[index].notes = [];
          state[index].users = [];
          await producer.send({
            topic: roomId,
            messages: [ { key: socket.id + '/' + name + '/clear', value: index.toString() }],
          });
        }
      }
    }
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
  io.sockets.sockets.forEach((socket) => {
    socket.disconnect(true);
  });
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