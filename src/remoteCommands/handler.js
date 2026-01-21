"use strict";

const { Kafka } = require("kafkajs");
const { validateAgainstSchema, schemas } = require("@connected-car/shared");

/**
 * Sleep helper.
 * @param {number} ms
 */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * PUBLIC_INTERFACE
 * Create remote-commands handler for the gateway:
 * - consumes RemoteCommandV1 (Kafka optional)
 * - accepts dev HTTP POST /v1/dev/commands
 * - emits CommandAckV1 via Kafka or via HTTP callback to remote-commands service (/acks)
 *
 * IMPORTANT: This is an MVP simulator. It does not contact a real vehicle.
 *
 * @param {any} logger
 * @param {{
 *  enabled: boolean,
 *  useKafka: boolean,
 *  kafka: { brokers: string[], clientId: string, consumerGroupId: string, topicRemoteCommand: string, topicCommandAck: string },
 *  http: { remoteCommandsServiceUrl: string }
 * }} cfg
 * @returns {{
 *  start: () => Promise<void>,
 *  stop: () => Promise<void>,
 *  handleIncomingCommand: (payload: any) => Promise<{ ok: true, acceptedAck: any } | { ok: false, error: string, details?: any }>
 * }}
 */
function createRemoteCommandHandler(logger, cfg) {
  /** @type {import("kafkajs").Kafka | undefined} */
  let kafka;
  /** @type {import("kafkajs").Producer | undefined} */
  let producer;
  /** @type {import("kafkajs").Consumer | undefined} */
  let consumer;
  let kafkaHealthy = false;

  function buildAck(command, status, reason) {
    return {
      schemaVersion: "v1",
      commandId: String(command.commandId),
      vehicleId: String(command.vehicleId),
      status,
      timestamp: new Date().toISOString(),
      ...(reason ? { reason: String(reason) } : {}),
    };
  }

  async function publishAckKafka(ack) {
    if (!producer || !kafkaHealthy) return { ok: false, error: "kafka_not_ready" };
    await producer.send({ topic: cfg.kafka.topicCommandAck, messages: [{ value: JSON.stringify(ack) }] });
    return { ok: true };
  }

  async function publishAckHttp(ack) {
    const base = cfg.http.remoteCommandsServiceUrl.replace(/\/+$/, "");
    const url = `${base}/acks`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(ack),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      return { ok: false, error: `ack_http_failed:${resp.status}`, details: text };
    }
    return { ok: true };
  }

  async function publishAck(ack) {
    // Prefer Kafka if enabled and healthy; else HTTP callback.
    if (cfg.useKafka && kafkaHealthy) {
      try {
        return await publishAckKafka(ack);
      } catch (e) {
        logger.warn("Ack Kafka publish failed; falling back to HTTP", { error: String(e?.message || e) });
        return await publishAckHttp(ack);
      }
    }
    return await publishAckHttp(ack);
  }

  async function simulateDeliverToVehicle(command) {
    // Accept immediately, then "complete" shortly after.
    const accepted = buildAck(command, "accepted");
    await publishAck(accepted);

    // Delay and then complete. Keep it short for MVP.
    await sleep(750);

    // For MVP, always succeed unless commandType is not supported.
    const completed = buildAck(command, "completed");
    await publishAck(completed);
  }

  async function handleIncomingCommand(payload) {
    if (!cfg.enabled) return { ok: false, error: "remote_commands_disabled" };

    const v = validateAgainstSchema(schemas.remoteCommand.v1, payload);
    if (!v.ok) return { ok: false, error: "schema_validation_failed", details: v.errors };

    const commandType = String(payload.commandType || "");
    if (commandType !== "unlock") {
      const rejected = buildAck(payload, "rejected", "unsupported_commandType");
      await publishAck(rejected);
      return { ok: false, error: "unsupported_commandType", details: { acceptedAck: rejected } };
    }

    // Start delivery simulation asynchronously, but we can still return an immediate accepted ack.
    const accepted = buildAck(payload, "accepted");
    simulateDeliverToVehicle(payload).catch((e) => {
      logger.warn("Vehicle delivery simulation failed", { error: String(e?.message || e) });
      const failed = buildAck(payload, "failed", "delivery_simulation_error");
      publishAck(failed).catch(() => {});
    });

    return { ok: true, acceptedAck: accepted };
  }

  async function startKafkaConsumerAndProducer() {
    kafka = new Kafka({ brokers: cfg.kafka.brokers, clientId: cfg.kafka.clientId });
    producer = kafka.producer();
    consumer = kafka.consumer({ groupId: cfg.kafka.consumerGroupId });

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: cfg.kafka.topicRemoteCommand, fromBeginning: false });

    await consumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        const rawValue = message.value ? message.value.toString("utf8") : "";
        let payload;
        try {
          payload = JSON.parse(rawValue);
        } catch (_) {
          logger.warn("RemoteCommand Kafka message parse failed; skipping", { topic, partition, offset: message.offset });
          return;
        }

        const result = await handleIncomingCommand(payload);
        if (!result.ok) {
          logger.warn("Remote command handling failed", { error: result.error, details: result.details });
        }
      },
    });

    kafkaHealthy = true;
    logger.info("Gateway remote-commands Kafka consumer started", {
      topicRemoteCommand: cfg.kafka.topicRemoteCommand,
      topicCommandAck: cfg.kafka.topicCommandAck,
      groupId: cfg.kafka.consumerGroupId,
    });
  }

  async function start() {
    if (!cfg.enabled) {
      logger.info("Gateway remote-commands handler disabled via config");
      return;
    }

    if (!cfg.useKafka) {
      logger.info("Gateway remote-commands Kafka disabled (RC_USE_KAFKA=false); using HTTP ack callback");
      return;
    }

    // Non-blocking optional Kafka. If unavailable, gateway still runs for dev HTTP.
    startKafkaConsumerAndProducer().catch((e) => {
      kafkaHealthy = false;
      logger.warn("Gateway remote-commands Kafka unavailable; dev HTTP endpoint still works", {
        error: String(e?.message || e),
      });
    });
  }

  async function stop() {
    kafkaHealthy = false;
    if (consumer) {
      try {
        await consumer.disconnect();
      } catch (e) {
        logger.warn("Remote-commands Kafka consumer disconnect failed", { error: String(e?.message || e) });
      }
    }
    if (producer) {
      try {
        await producer.disconnect();
      } catch (e) {
        logger.warn("Remote-commands Kafka producer disconnect failed", { error: String(e?.message || e) });
      }
    }
    consumer = undefined;
    producer = undefined;
    kafka = undefined;
  }

  return { start, stop, handleIncomingCommand };
}

module.exports = { createRemoteCommandHandler };
