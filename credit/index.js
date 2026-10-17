const { Kafka } = require('kafkajs');
const { createClient } = require('redis');
const { randomUUID } = require('crypto');

const kafka = new Kafka({ clientId: 'credit', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'credit-group' });
const producer = kafka.producer();
const redisClient = createClient({
  url: process.env.REDIS_URL || 'redis://127.0.0.1:6380',
});

redisClient.on('error', error => console.error('[Credit] Redis error:', error));

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
      console.log(`[Credit] Duplicate event detected for ${workflowId} on ${topic}. Ignoring.`);
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

async function init() {
  try {
    await redisClient.connect();
    console.log('Redis connected for idempotency caching.');

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'start_credit_check', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        if (typeof data.workflowId !== 'string' || !data.workflowId) {
          throw new Error(`Invalid workflowId on ${topic}`);
        }

        await processOnce(topic, data.workflowId, async () => {
          if (topic !== 'start_credit_check') {
            throw new Error(`Unexpected topic "${topic}"`);
          }

          console.log(`[Credit] Processing credit check for workflow: ${data.workflowId}`);
          console.log(`[Credit] DELIBERATE FAILURE: Rejecting user credit.`);

          await producer.send({
            topic: 'credit_result',
            messages: [{ value: JSON.stringify({ workflowId: data.workflowId, status: 'FAILED' }) }]
          });
        });
      },
    });
    console.log('Credit service listening for events...');
  } catch (error) {
    console.error('[Credit] Startup failed:', error);
    process.exit(1);
  }
}

init();