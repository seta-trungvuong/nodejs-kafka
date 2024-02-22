const express = require('express');
const bodyParser = require('body-parser');
const kafka = require('kafka-node');

// Create an Express application
const app = express();

// Middleware to parse JSON bodies
app.use(bodyParser.json());

// Kafka configuration
const kafkaHost = 'localhost:9092';
const client = new kafka.KafkaClient({ kafkaHost });
const producer = new kafka.Producer(client);
const consumer = new kafka.Consumer(client, [{ topic: 'my-topic-1', partition: 0 }]);

// Producer ready event
producer.on('ready', () => {
  console.log('Kafka producer is ready');
});

// Producer error event
producer.on('error', (err) => {
  console.error('Error initializing Kafka producer:', err);
});

// Consumer message event
consumer.on('message', (message) => {
  console.log('Received message:', message.value);
});

// Send message endpoint
app.post('/send', (req, res) => {
  const { message } = req.body;

  // Create a new payload
  const payloads = [
    {
      topic: 'my-topic-1',
      messages: message,
    },
  ];

  // Send the payload to Kafka
  producer.send(payloads, (err, data) => {
    if (err) {
      console.error('Error sending message to Kafka:', err);
      res.status(500).json({ error: 'Error sending message to Kafka' });
    } else {
      console.log('Message sent to Kafka:', data);
      res.json({ success: true });
    }
  });
});

// Start the Express server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
