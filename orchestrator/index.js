const express = require('express');
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const yaml = require('js-yaml');
const fs = require('fs');
const path = require('path');

const workflowDef = yaml.load(
  fs.readFileSync(path.join(__dirname, 'workflow.yaml'), 'utf8')
);

if (
  !workflowDef ||
  typeof workflowDef.name !== 'string' ||
  !Array.isArray(workflowDef.steps) ||
  workflowDef.steps.length === 0 ||
  workflowDef.steps.some(step =>
    !step ||
    typeof step.id !== 'string' ||
    typeof step.action !== 'string' ||
    typeof step.result_topic !== 'string' ||
    (step.compensation !== undefined && typeof step.compensation !== 'string')
  )
) {
  throw new Error('Invalid workflow definition in orchestrator/workflow.yaml');
}

const resultTopics = workflowDef.steps.map(step => step.result_topic);

if (
  new Set(workflowDef.steps.map(step => step.action)).size !== workflowDef.steps.length ||
  new Set(resultTopics).size !== resultTopics.length
) {
  throw new Error('Workflow step actions and result topics must be unique');
}

console.log(`[Orchestrator] Loaded YAML Workflow: ${workflowDef.name}`);

const app = express();
app.use(express.json());

const pgPool = new Pool({
  user: 'user',
  host: 'localhost',
  database: 'flowmaster',
  password: 'password',
  port: 5432,
});

const kafka = new Kafka({ clientId: 'orchestrator', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'orchestrator-group' });

async function withTransaction(callback) {
  const client = await pgPool.connect();

  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      console.error('[Postgres] Transaction rollback failed:', rollbackError);
    }
    throw error;
  } finally {
    client.release();
  }
}

app.post('/workflows', async (req, res) => {
  const workflowId = uuidv4();
  const firstStep = workflowDef.steps[0];

  try {
    await withTransaction(async client => {
      await client.query(
        'INSERT INTO workflows (id, status, current_step) VALUES ($1, $2, $3)',
        [workflowId, 'PENDING', firstStep.action]
      );
      await client.query(
        'INSERT INTO events (workflow_id, topic, payload) VALUES ($1, $2, $3)',
        [workflowId, firstStep.action, { workflowId }]
      );
    });

    console.log(`[Orchestrator] Transaction committed for workflow ${workflowId}`);
    res.status(202).json({ workflowId, message: 'Workflow queued via Outbox' });
  } catch (error) {
    console.error('[Orchestrator] Failed to queue workflow:', error);
    res.status(500).json({ error: 'Internal system error' });
  }
});

async function init() {
  try {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS workflows (id UUID PRIMARY KEY, status VARCHAR(50), current_step VARCHAR(50));
      CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        workflow_id UUID,
        topic VARCHAR(100),
        payload JSONB,
        published BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      ALTER TABLE events ADD COLUMN IF NOT EXISTS topic VARCHAR(100);
      ALTER TABLE events ADD COLUMN IF NOT EXISTS published BOOLEAN DEFAULT FALSE;
    `);
    console.log('Postgres connected & State tables verified.');

    const admin = kafka.admin();
    await admin.connect();
    const workflowTopics = workflowDef.steps.flatMap(step => [
      step.action,
      step.result_topic,
      ...(step.compensation ? [step.compensation] : [])
    ]);
    await admin.createTopics({
      topics: [...new Set(workflowTopics)].map(topic => ({ topic })),
      waitForLeaders: true,
    });
    console.log('Kafka topics explicitly created.');
    await admin.disconnect();

    await producer.connect();
    console.log('Kafka Producer connected.');

    console.log('[Orchestrator] Scanning for incomplete workflows...');
    const pendingWorkflows = await pgPool.query(
      "SELECT * FROM workflows WHERE status = 'PENDING'"
    );

    if (pendingWorkflows.rows.length > 0) {
      console.log(`[Orchestrator] Found ${pendingWorkflows.rows.length} incomplete workflows. Resuming...`);
      await withTransaction(async client => {
        for (const wf of pendingWorkflows.rows) {
          if (!workflowDef.steps.some(step => step.action === wf.current_step)) {
            throw new Error(`Unknown workflow step "${wf.current_step}" for workflow ${wf.id}`);
          }
          console.log(`[Orchestrator] Queuing recovery event ${wf.current_step} for workflow: ${wf.id}`);
          await client.query(
            'INSERT INTO events (workflow_id, topic, payload) VALUES ($1, $2, $3)',
            [wf.id, wf.current_step, { workflowId: wf.id }]
          );
        }
      });
    } else {
      console.log('[Orchestrator] No incomplete workflows found. State is clean.');
    }

    await consumer.connect();
    for (const topic of resultTopics) {
      await consumer.subscribe({ topic, fromBeginning: true });
    }

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        const currentStepIndex = resultTopics.indexOf(topic);

        if (currentStepIndex === -1) {
          throw new Error(`Received result on unconfigured topic "${topic}"`);
        }

        const currentStep = workflowDef.steps[currentStepIndex];
        await withTransaction(async client => {
          const workflowResult = await client.query(
            'SELECT status, current_step FROM workflows WHERE id = $1 FOR UPDATE',
            [data.workflowId]
          );
          const workflow = workflowResult.rows[0];

          if (
            !workflow ||
            workflow.status !== 'PENDING' ||
            workflow.current_step !== currentStep.action
          ) {
            console.log(`[Orchestrator] Ignoring stale result for workflow ${data.workflowId} on ${topic}.`);
            return;
          }

          if (data.status === 'SUCCESS') {
            const nextStep = workflowDef.steps[currentStepIndex + 1];
            if (nextStep) {
              console.log(`[Orchestrator] Step ${topic} successful for ${data.workflowId}. Transitioning to ${nextStep.action}...`);
              await client.query(
                'UPDATE workflows SET current_step = $1 WHERE id = $2 AND status = $3',
                [nextStep.action, data.workflowId, 'PENDING']
              );
              await client.query(
                'INSERT INTO events (workflow_id, topic, payload) VALUES ($1, $2, $3)',
                [data.workflowId, nextStep.action, { workflowId: data.workflowId }]
              );
            } else {
              console.log(`[Orchestrator] Workflow ${data.workflowId} fully COMPLETED.`);
              await client.query(
                "UPDATE workflows SET status = 'COMPLETED' WHERE id = $1 AND status = 'PENDING'",
                [data.workflowId]
              );
            }
          } else if (data.status === 'FAILED') {
            console.log(`[Orchestrator] 🚨 Step ${topic} failed for ${data.workflowId}. Initiating Saga Compensation...`);
            const previousStep = workflowDef.steps[currentStepIndex - 1];

            if (previousStep && previousStep.compensation) {
              await client.query(
                'INSERT INTO events (workflow_id, topic, payload) VALUES ($1, $2, $3)',
                [data.workflowId, previousStep.compensation, { workflowId: data.workflowId }]
              );
            }
            await client.query(
              "UPDATE workflows SET status = 'ROLLED_BACK' WHERE id = $1 AND status = 'PENDING'",
              [data.workflowId]
            );
          } else {
            throw new Error(`Unknown workflow status "${data.status}" on topic "${topic}"`);
          }
        });
      }
    });

    let relayRunning = false;
    setInterval(async () => {
      if (relayRunning) {
        return;
      }

      relayRunning = true;
      try {
        await withTransaction(async client => {
          const { rows } = await client.query(
            'SELECT id, topic, payload FROM events WHERE published = FALSE AND topic IS NOT NULL ORDER BY id ASC LIMIT 50 FOR UPDATE SKIP LOCKED'
          );

          for (const event of rows) {
            await producer.send({
              topic: event.topic,
              messages: [{ value: JSON.stringify(event.payload) }]
            });
            await client.query(
              'UPDATE events SET published = TRUE WHERE id = $1',
              [event.id]
            );
            console.log(`[Outbox] Relayed event ${event.id} to topic ${event.topic}`);
          }
        });
      } catch (error) {
        console.error('[Outbox] Relay encountered an error:', error);
      } finally {
        relayRunning = false;
      }
    }, 2000);

    app.listen(3000, () => console.log('Orchestrator API running on port 3000'));
  } catch (error) {
    console.error('Boot sequence failed:', error);
    process.exit(1);
  }
}

init();
