"use strict";

/**
 * PUBLIC_INTERFACE
 * Load gateway config from environment variables.
 *
 * @returns {{
 *  serviceName: string,
 *  host: string,
 *  port: number,
 *  logLevel: "debug"|"info"|"warn"|"error",
 *  publish: {
 *    enabled: boolean,
 *    mode: "kafka"|"http",
 *    defaultDeviceId: string,
 *    kafka: { brokers: string[], topic: string, clientId: string },
 *    http: { ingestionUrl: string, authToken?: string }
 *  },
 *  remoteCommands: {
 *    enabled: boolean,
 *    useKafka: boolean,
 *    kafka: {
 *      brokers: string[],
 *      clientId: string,
 *      consumerGroupId: string,
 *      topicRemoteCommand: string,
 *      topicCommandAck: string
 *    },
 *    http: {
 *      remoteCommandsServiceUrl: string
 *    }
 *  }
 * }}
 */
function loadConfig() {
  const serviceName = process.env.SERVICE_NAME || "vehicle-gateway";
  const port = Number(process.env.PORT || 3004);
  const host = process.env.HOST || "0.0.0.0";
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

  // Remote Commands (Phase 6)
  const rcEnabled = String(process.env.RC_ENABLED || "true").toLowerCase() === "true";
  const rcUseKafka = String(process.env.RC_USE_KAFKA || "false").toLowerCase() === "true";

  const rcConsumerGroupId = process.env.KAFKA_CONSUMER_GROUP_ID_REMOTE_COMMANDS || "vehicle-gateway-remote-commands-v1";
  const topicRemoteCommand = process.env.KAFKA_TOPIC_REMOTE_COMMAND || "remote-command.v1";
  const topicCommandAck = process.env.KAFKA_TOPIC_COMMAND_ACK || "command-ack.v1";

  const remoteCommandsServiceUrl = process.env.RC_REMOTE_COMMANDS_HTTP_URL || "http://localhost:3020";

  return {
    serviceName,
    host,
    port,
    logLevel,
    publish: {
      enabled,
      mode: mode === "http" ? "http" : "kafka",
      defaultDeviceId,
      kafka: { brokers, topic, clientId },
      http: { ingestionUrl, authToken },
    },
    remoteCommands: {
      enabled: rcEnabled,
      useKafka: rcUseKafka,
      kafka: {
        brokers,
        clientId,
        consumerGroupId: rcConsumerGroupId,
        topicRemoteCommand,
        topicCommandAck,
      },
      http: {
        remoteCommandsServiceUrl,
      },
    },
  };
}

module.exports = { loadConfig };
