require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const redis = require('redis');
const cors = require('cors');
const Task = require('./Task');

const app = express();
app.use(express.json());
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));

const PORT = process.env.PORT || 3002;

// Connect to MongoDB
mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/content_tasks')
    .then(() => console.log('Processing Service connected to MongoDB'))
    .catch(err => console.error('MongoDB connection error:', err));

// Connect to Redis
const redisClient = redis.createClient({ url: process.env.REDIS_URI || 'redis://localhost:6379' });
redisClient.on('error', (err) => console.log('Redis Client Error', err));
redisClient.connect().then(() => console.log('Processing Service connected to Redis'));

// Function to process text synchronously (Quick task)
const processQuick = (text) => {
    return text.toUpperCase();
};

// Function to process text asynchronously (Heavy task)
const processHeavy = async (text) => {
    return new Promise((resolve) => {
        setTimeout(() => {
            // Fake heavy task: Reverse the text
            const result = text.split('').reverse().join('');
            resolve(result);
        }, 5000); // 5 seconds delay
    });
};

// Start RabbitMQ Consumer
async function startConsumer() {
    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URI || 'amqp://localhost');
        const channel = await connection.createChannel();
        await channel.assertQueue('content_tasks_queue');
        
        console.log('Processing Service waiting for messages in queue');
        
        channel.consume('content_tasks_queue', async (msg) => {
            if (msg !== null) {
                const { taskId, text } = JSON.parse(msg.content.toString());
                console.log(`Received async task: ${taskId}`);
                
                try {
                    // Update task status
                    await Task.findByIdAndUpdate(taskId, { status: 'processing' });
                    
                    // Simulate heavy processing
                    const result = await processHeavy(text);
                    
                    // Update task in DB
                    await Task.findByIdAndUpdate(taskId, { status: 'completed', result });
                    
                    // Cache result in Redis
                    await redisClient.set(`task_result:${taskId}`, result, { EX: 3600 }); // Expire in 1 hour
                    
                    console.log(`Completed async task: ${taskId}`);
                    channel.ack(msg);
                } catch (err) {
                    console.error('Error processing async task:', err);
                    await Task.findByIdAndUpdate(taskId, { status: 'failed' });
                    channel.nack(msg);
                }
            }
        });
    } catch (err) {
        console.error('RabbitMQ connection error in consumer:', err);
    }
}
startConsumer();

// Sync processing endpoint
app.post('/process/sync', async (req, res) => {
    const { taskId, text } = req.body;
    if (!taskId || !text) {
        return res.status(400).json({ error: 'taskId and text required' });
    }
    
    try {
        const result = processQuick(text);
        
        // Cache result in Redis
        await redisClient.set(`task_result:${taskId}`, result, { EX: 3600 });
        
        return res.json({ result });
    } catch (err) {
        return res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`Processing Service running on http://localhost:${PORT}`);
});
