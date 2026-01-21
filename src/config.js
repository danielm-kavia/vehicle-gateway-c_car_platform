"use strict";

/**
 * PUBLIC_INTERFACE
 * Load gateway config from environment variables.
 *
 * @returns {{
 *  serviceName: string,
 *  port: number,
 *  logLevel: "debug"|"info"|"warn"|"error",
 *  publish: {
 *    enabled: boolean,
 *    mode: "kafka"|"http",
 *    defaultDeviceId: string,
 *    kafka: { brokers: string[], topic: string, clientId: string },
 *    http: { ingestionUrl: string, authToken?: string }
 *  }
 * }}
 */
function loadConfig() {
  const serviceName = process.env.SERVICE_NAME || "vehicle-gateway";
  const port = Number(process.env.PORT || 3004);
  const logLevel = /** @type {any} */ (process.env.LOG_LEVEL || "info");

  const enabled = String(process.env.TELEMATICS_PUBLISH_ENABLED || "false").toLowerCase() === "true";
  const mode = /** @type {any} */ (process.env.TELEMATICS_PUBLISH_MODE || "kafka");
  const defaultDeviceId = process.env.TELEMATICS_DEFAULT_DEVICE_ID || "sim-device";

  const brokers = String(process.env.KAFKA_BROKERS || "localhost:9092")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const topic = process.env.KAFKA_TOPIC_TELEMATICS || "telematics.v1";
  const clientId = process.env.KAFKA_CLIENT_ID || "vehicle-gateway";

  const ingestionUrl = process.env.TELEMATICS_INGESTION_HTTP_URL || "http://localhost:3002/v1/telematics/batch";
  const authToken = process.env.TELEMATICS_INGESTION_AUTH_TOKEN || undefined;

  return {
    serviceName,
    port,
    logLevel,
    publish: {
      enabled,
      mode: mode === "http" ? "http" : "kafka",
      defaultDeviceId,
      kafka: { brokers, topic, clientId },
      http: { ingestionUrl, authToken },
    },
  };
}

module.exports = { loadConfig };
