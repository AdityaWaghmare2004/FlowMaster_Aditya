const { Kafka } = require('kafkajs');
const { createClient } = require('redis');
const { randomUUID } = require('crypto');

const kafka = new Kafka({ clientId: 'verification', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'verification-group' });
const producer = kafka.producer();
const redisClient = createClient({
  url: process.env.REDIS_URL || 'redis://127.0.0.1:6380',
});

redisClient.on('error', error => console.error('[Verification] Redis error:', error));

const claimScript = `
  local current = redis.call('GET', KEYS[1])
  if not current then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
  end
  if current == 'DONE' then
    return 0
  end
  return -1
`;

const completeScript = `
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SET', KEYS[1], 'DONE', 'EX', ARGV[2])
    return 1
  end
  return 0
`;

const releaseScript = `
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
  end
  return 0
`;

const delay = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

async function processOnce(topic, workflowId, handler) {
  const cacheKey = `idempotency:${topic}:${workflowId}`;
  const claimToken = randomUUID();

  for (;;) {
    const claim = await redisClient.eval(claimScript, {
      keys: [cacheKey],
      arguments: [claimToken, '300'],
    });

    if (claim === 0) {
      console.log(`[Verification] Duplicate event detected for ${workflowId} on ${topic}. Ignoring.`);
      return;
    }

    if (claim === 1) {
      break;
    }

    await delay(100);
  }

  try {
    await handler();
    const completed = await redisClient.eval(completeScript, {
      keys: [cacheKey],
      arguments: [claimToken, '86400'],
    });

    if (completed !== 1) {
      throw new Error(`Lost Redis idempotency claim for ${workflowId} on ${topic}`);
    }
  } catch (error) {
    await redisClient.eval(releaseScript, {
      keys: [cacheKey],
      arguments: [claimToken],
    });
    throw error;
  }
}

async function handleMessage(topic, message) {
  const data = JSON.parse(message.value.toString());
  if (typeof data.workflowId !== 'string' || !data.workflowId) {
    throw new Error(`Invalid workflowId on ${topic}`);
  }

  await processOnce(topic, data.workflowId, async () => {
    if (topic === 'start_verification') {
      console.log(`[Verification] Verifying ID for workflow: ${data.workflowId}`);
      await delay(100);
      await producer.send({
        topic: 'verification_result',
        messages: [{ value: JSON.stringify({ workflowId: data.workflowId, status: 'SUCCESS' }) }]
      });
    } else if (topic === 'compensate_verification') {
      console.log(`[Verification] ⚠️ ROLLBACK ⚠️: Undoing ID verification for workflow: ${data.workflowId}`);
    } else {
      throw new Error(`Unexpected topic "${topic}"`);
    }
  });
}

async function init() {
  try {
    await redisClient.connect();
    console.log('Redis connected for idempotency caching.');

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'start_verification', fromBeginning: true });
    await consumer.subscribe({ topic: 'compensate_verification', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, message }) => handleMessage(topic, message),
    });
    console.log('Verification service listening for events...');
  } catch (error) {
    console.error('[Verification] Startup failed:', error);
    process.exit(1);
  }
}

init();