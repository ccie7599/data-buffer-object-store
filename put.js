const express = require('express');
const { connect } = require('nats');
const cors = require('cors');
const rawBody = require('raw-body');

const app = express();
const PORT = 8080;
const STREAM_NAME = 'video-in';
const SUBJECT_NAME = 'video-in';

let natsConnection;
let js;
let jsm;

// Setup CORS (optional depending on client)
app.use(cors());

// Middleware: capture raw body for all PUT requests
app.use(async (req, res, next) => {
  if (req.method === 'PUT') {
    try {
      req.rawBody = await rawBody(req);
      next();
    } catch (err) {
      console.error('Error parsing raw body:', err);
      res.status(400).send('Invalid request body');
    }
  } else {
    next();
  }
});

// Setup NATS JetStream and stream if missing
async function setupNATS() {
  natsConnection = await connect({ servers: 'localhost:4222' });
  js = natsConnection.jetstream();
  jsm = await natsConnection.jetstreamManager();

  try {
    await jsm.streams.info(STREAM_NAME);
    console.log(`Stream '${STREAM_NAME}' exists`);
  } catch (err) {
    if (err.code === '404') {
      console.log(`Stream '${STREAM_NAME}' not found. Creating...`);
      await jsm.streams.add({
        name: STREAM_NAME,
        subjects: [SUBJECT_NAME],
        storage: 'file',
        max_msgs: 10000
      });
      console.log(`Stream '${STREAM_NAME}' created`);
    } else {
      throw err;
    }
  }
}

// Handle PUT request with binary payload
app.put('/upload', async (req, res) => {
  try {
    await js.publish(SUBJECT_NAME, req.rawBody);
    res.status(200).json({ status: 'Binary data published to JetStream' });
  } catch (err) {
    console.error('Error publishing to NATS:', err);
    res.status(500).json({ error: 'Failed to publish to JetStream' });
  }
});

// Start server
(async () => {
  try {
    await setupNATS();
    app.listen(PORT, () => {
      console.log(`Binary PUT listener on http://0.0.0.0:${PORT}`);
    });
  } catch (err) {
    console.error('Startup error:', err);
    process.exit(1);
  }
})();
