"use strict";

const express = require("express");
const {
  createLogger,
  withCorrelationId,
  getCorrelationId,
  createSecurityHeadersMiddleware,
  createRateLimitMiddleware,
} = require("@connected-car/shared");
const { loadConfig } = require("./config");
const { createTelematicsPublisher } = require("./telematics/publisher");
const { createRemoteCommandHandler } = require("./remoteCommands/handler");

const cfg = loadConfig();
const logger = createLogger({ serviceName: cfg.serviceName, level: cfg.logLevel });

const publisher = createTelematicsPublisher(logger, cfg.publish);
const remoteCommandHandler = createRemoteCommandHandler(logger, cfg.remoteCommands);

const app = express();

// Hardening (Phase 9): security headers + optional rate limiting (disabled by default).
app.use(
  createSecurityHeadersMiddleware({
    serviceName: cfg.serviceName,
    enabled: true,
    enableCsp: String(process.env.SECURITY_ENABLE_CSP || "false").toLowerCase() === "true",
    csp: process.env.SECURITY_CSP || undefined,
    enableHsts: String(process.env.SECURITY_ENABLE_HSTS || "false").toLowerCase() === "true",
  })
);
app.use(
  createRateLimitMiddleware({
    enabled: String(process.env.RATE_LIMIT_ENABLED || "false").toLowerCase() === "true",
    windowSeconds: Number(process.env.RATE_LIMIT_WINDOW_S || 60),
    maxRequests: Number(process.env.RATE_LIMIT_MAX || 100),
    logger,
  })
);

app.use(express.json({ limit: "256kb" }));

/**
 * PUBLIC_INTERFACE
 * Health endpoint for gateway stub.
 */
app.get(
  "/health",
  withCorrelationId(logger, async (req, res) => {
    res.json({
      ok: true,
      service: cfg.serviceName,
      correlationId: getCorrelationId(),
      telematicsPublish: { enabled: cfg.publish.enabled, mode: cfg.publish.mode },
    });
  })
);

/**
 * PUBLIC_INTERFACE
 * Dev-only telemetry publish endpoint (MVP).
 *
 * POST /v1/dev/telematics/publish
 * Body: TelematicsV1 JSON.
 *
 * If TELEMATICS_PUBLISH_ENABLED=true, forwards payload to Kafka or ingestion HTTP.
 */
app.post(
  "/v1/dev/telematics/publish",
  withCorrelationId(logger, async (req, res) => {
    const payload = req.body;
    const result = await publisher.publishTelemetry(payload);
    if (!result.ok) {
      return res.status(400).json({ ok: false, error: result.error, details: result.details });
    }
    return res.status(200).json({ ok: true });
  })
);

/**
 * PUBLIC_INTERFACE
 * Dev-only remote command ingress endpoint (Phase 6 MVP).
 *
 * POST /v1/dev/commands
 * Body: RemoteCommandV1 JSON.
 *
 * This simulates "deliver-to-vehicle" and triggers ack flow:
 * - accepted immediately
 * - completed after a short delay
 *
 * Acks are emitted via Kafka (if RC_USE_KAFKA=true and Kafka is available),
 * otherwise they are POSTed to remote-commands service at /acks.
 */
app.post(
  "/v1/dev/commands",
  withCorrelationId(logger, async (req, res) => {
    const payload = req.body;
    const result = await remoteCommandHandler.handleIncomingCommand(payload);

    if (!result.ok) {
      // For schema failures, we treat it as 400. For unsupported commands, 400 is fine for dev.
      return res.status(400).json({ ok: false, error: result.error, details: result.details });
    }

    // Respond with accepted ack payload for convenience in the HTTP fallback path.
    return res.status(200).json(result.acceptedAck);
  })
);

async function main() {
  // Start HTTP server; publisher connectivity is optional and should not block preview startup.
  app.listen(cfg.port, cfg.host || "0.0.0.0", () => {
    logger.info("Vehicle gateway stub listening", { port: cfg.port });
  });

  // If kafka mode, connect in background.
  if (cfg.publish.enabled && cfg.publish.mode === "kafka") {
    publisher.start().catch((e) => {
      logger.warn("Telematics publisher failed to start; will remain disabled until restart", {
        error: String(e && e.message ? e.message : e),
      });
    });
  }

  // Remote commands Kafka consumer/producer (optional) in background.
  if (cfg.remoteCommands.enabled && cfg.remoteCommands.useKafka) {
    remoteCommandHandler.start().catch((e) => {
      logger.warn("Remote command handler failed to start; dev HTTP endpoint still available", {
        error: String(e && e.message ? e.message : e),
      });
    });
  }

  process.on("SIGINT", async () => {
    try {
      await publisher.stop();
      await remoteCommandHandler.stop();
    } catch (_) {}
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    try {
      await publisher.stop();
      await remoteCommandHandler.stop();
    } catch (_) {}
    process.exit(0);
  });
}

main().catch((e) => {
  logger.error("Fatal startup error", { error: String(e && e.message ? e.message : e) });
});
