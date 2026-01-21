"use strict";

const { Kafka } = require("kafkajs");
const { validateAgainstSchema, schemas } = require("@connected-car/shared");

/**
 * PUBLIC_INTERFACE
 * Create a telematics publisher stub for Phase 3.
 *
 * This is intentionally minimal:
 * - Validates payload against shared TelematicsV1 schema
 * - Publishes to Kafka OR calls ingestion HTTP batch endpoint (fallback mode)
 *
 * @param {any} logger
 * @param {{
 *  enabled: boolean,
 *  mode: "kafka"|"http",
 *  defaultDeviceId: string,
 *  kafka: { brokers: string[], topic: string, clientId: string },
 *  http: { ingestionUrl: string, authToken?: string }
 * }} cfg
 * @returns {{
 *  start: () => Promise<void>,
 *  stop: () => Promise<void>,
 *  publishTelemetry: (payload: any) => Promise<{ ok: true } | { ok: false, error: string, details?: any }>
 * }}
 */
function createTelematicsPublisher(logger, cfg) {
  /** @type {import("kafkajs").Producer | undefined} */
  let producer;
  /** @type {import("kafkajs").Kafka | undefined} */
  let kafka;

  async function start() {
    if (!cfg.enabled) return;
    if (cfg.mode !== "kafka") return;

    kafka = new Kafka({ brokers: cfg.kafka.brokers, clientId: cfg.kafka.clientId });
    producer = kafka.producer();
    await producer.connect();
    logger.info("Telematics publisher connected to Kafka", { topic: cfg.kafka.topic });
  }

  async function stop() {
    if (producer) {
      try {
        await producer.disconnect();
      } catch (e) {
        logger.warn("Kafka producer disconnect failed", { error: String(e && e.message ? e.message : e) });
      }
    }
    producer = undefined;
    kafka = undefined;
  }

  async function publishKafka(payload) {
    if (!producer) return { ok: false, error: "kafka producer not started" };
    await producer.send({
      topic: cfg.kafka.topic,
      messages: [{ value: JSON.stringify(payload) }],
    });
    return { ok: true };
  }

  async function publishHttp(payload) {
    const body = {
      deviceId: payload.deviceId || cfg.defaultDeviceId,
      items: [payload],
    };

    /** @type {Record<string,string>} */
    const headers = { "content-type": "application/json" };
    if (cfg.http.authToken) headers.authorization = `Bearer ${cfg.http.authToken}`;

    const resp = await fetch(cfg.http.ingestionUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      return { ok: false, error: `http ingest failed: ${resp.status}`, details: text };
    }
    return { ok: true };
  }

  async function publishTelemetry(payload) {
    if (!cfg.enabled) return { ok: false, error: "publisher disabled" };

    const result = validateAgainstSchema(schemas.telematics.v1, payload);
    if (!result.ok) return { ok: false, error: "schema validation failed", details: result.errors };

    if (cfg.mode === "http") return await publishHttp(payload);
    return await publishKafka(payload);
  }

  return { start, stop, publishTelemetry };
}

module.exports = { createTelematicsPublisher };
