const { connect } = require('nats');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// === CONFIGURATION ===
const NATS_SERVER = 'localhost:4222';
const STREAM_NAME = 'video-in';
const SUBJECT_NAME = 'video-in';
const CONSUMER_NAME = 'video-consumer';
const BUCKET_NAME = '';
const REGION = 'us-ord';
const ACCESS_KEY = '';
const SECRET_KEY = '';
const ENDPOINT = 'https://us-ord-1.linodeobjects.com';

(async () => {
  const nc = await connect({ servers: NATS_SERVER });
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();

  // Ensure stream exists
  try {
    await jsm.streams.info(STREAM_NAME);
  } catch {
    console.error(`❌ Stream "${STREAM_NAME}" does not exist.`);
    process.exit(1);
  }

  // Ensure consumer exists
  try {
    await jsm.consumers.info(STREAM_NAME, CONSUMER_NAME);
  } catch {
    console.log(`ℹ️ Creating consumer "${CONSUMER_NAME}"...`);
    await jsm.consumers.add(STREAM_NAME, {
      durable_name: CONSUMER_NAME,
      ack_policy: 'explicit',
      deliver_policy: 'all',
      max_ack_pending: 1000,
    });
  }

  const s3 = new S3Client({
    region: REGION,
    endpoint: ENDPOINT,
    credentials: {
      accessKeyId: ACCESS_KEY,
      secretAccessKey: SECRET_KEY,
    },
    forcePathStyle: true,
  });

  const sub = await js.pullSubscribe(SUBJECT_NAME, {
    config: { durable_name: CONSUMER_NAME },
    queue: 'video-consumer',
  });

  console.log('🚀 Waiting for messages...');

  while (true) {
    // Pull a batch and wait up to 5 seconds
    await sub.pull({ batch: 10, expires: 5000 });

    for await (const m of sub) {
      const filename = `${Date.now()}.bin`;

      try {
        await s3.send(new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: filename,
          Body: m.data,
          ContentType: 'application/octet-stream',
        }));

        console.log(`✅ Uploaded ${filename}`);
        m.ack();
      } catch (err) {
        console.error('❌ S3 upload failed:', err);
      }
    }
  }
})();
