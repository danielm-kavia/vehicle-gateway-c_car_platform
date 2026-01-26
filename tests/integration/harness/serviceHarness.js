"use strict";

const path = require("path");
const fs = require("fs");
const os = require("os");
const { spawn } = require("child_process");
const { httpJson } = require("../helpers/httpClient");

/**
 * We start both services from the integration test container (vehicle-gateway),
 * but spawn them with explicit working directories pointing to each service.
 *
 * We persist child PIDs to a temp file so globalTeardown can reliably kill them
 * even though it runs in a separate Node process.
 */

const DEFAULT_TIMEOUT_MS = 20_000;
const POLL_INTERVAL_MS = 250;

/**
 * Resolve repository root from this file location:
 * vehicle-gateway-c_car_platform/tests/integration/harness/serviceHarness.js
 * -> repo root is 3 levels up from vehicle-gateway-c_car_platform
 */
function repoRootFromHere() {
  return path.resolve(__dirname, "../../../..");
}

function pidFilePath() {
  return path.join(os.tmpdir(), "connected-car-integration-harness-pids.vehicle-gateway-remote-commands.json");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * PUBLIC_INTERFACE
 * Compute the environment variables used by the harness and integration tests.
 *
 * @returns {{
 *  VEHICLE_GATEWAY_BASE_URL: string,
 *  REMOTE_COMMANDS_BASE_URL: string,
 *  gatewayPort: number,
 *  remoteCommandsPort: number
 * }}
 */
function buildHarnessEnv() {
  const gatewayPort = Number(process.env.GATEWAY_TEST_PORT || 3204);
  const remoteCommandsPort = Number(process.env.REMOTE_COMMANDS_TEST_PORT || 3206);

  return {
    VEHICLE_GATEWAY_BASE_URL: `http://127.0.0.1:${gatewayPort}`,
    REMOTE_COMMANDS_BASE_URL: `http://127.0.0.1:${remoteCommandsPort}`,
    gatewayPort,
    remoteCommandsPort,
  };
}

async function waitForHealthy(baseUrl, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  let lastErr = null;

  while (Date.now() < deadline) {
    try {
      const res = await httpJson(`${baseUrl}/health`, { method: "GET", timeoutMs: 1500 });
      if (res.status === 200) return;
      lastErr = new Error(`Non-200 health status: ${res.status}`);
    } catch (e) {
      lastErr = e;
    }
    await sleep(POLL_INTERVAL_MS);
  }

  const msg = lastErr ? String(lastErr && lastErr.message ? lastErr.message : lastErr) : "unknown error";
  throw new Error(`Service at ${baseUrl} did not become healthy within ${timeoutMs}ms (${msg})`);
}

function spawnService({ name, cwd, env, nodeArgs }) {
  const child = spawn(process.execPath, nodeArgs, {
    cwd,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });

  // Prefix logs so failures in CI are debuggable.
  child.stdout.on("data", (d) => process.stdout.write(`[${name}] ${String(d)}`));
  child.stderr.on("data", (d) => process.stderr.write(`[${name}] ${String(d)}`));

  child.on("exit", (code, signal) => {
    process.stderr.write(`[${name}] exited (code=${code}, signal=${signal})\n`);
  });

  return child;
}

/**
 * PUBLIC_INTERFACE
 * Start vehicle-gateway + remote-commands and wait until both are healthy.
 *
 * NOTE: We intentionally disable Kafka and auth in this harness to keep CI self-contained.
 *
 * @returns {Promise<{ pids: number[], envForTests: Record<string,string> }>}
 */
async function startServicesForIntegrationTests() {
  const repoRoot = repoRootFromHere();

  const gwRoot = path.join(repoRoot, "vehicle-gateway-c_car_platform");
  const rcRoot = path.join(repoRoot, "remote-commands-c_car_platform");

  const { VEHICLE_GATEWAY_BASE_URL, REMOTE_COMMANDS_BASE_URL, gatewayPort, remoteCommandsPort } = buildHarnessEnv();

  // Base environment: inherit, then override for test profile.
  const baseEnv = {
    ...process.env,
    NODE_ENV: process.env.NODE_ENV || "test",

    // Integration test base URLs consumed by test HTTP client.
    VEHICLE_GATEWAY_BASE_URL,
    REMOTE_COMMANDS_BASE_URL,

    // Disable external deps (safe to set even if unused).
    KAFKA_ENABLED: "false",
    TIMESCALE_ENABLED: "false",
  };

  // Remote-commands:
  // - Use in-memory store to avoid file persistence & cross-test contamination.
  // - Disable auth so tests don't need JWT.
  // - Ensure dispatch uses HTTP to gateway test port.
  const remoteCommandsEnv = {
    ...baseEnv,
    SERVICE_NAME: "remote-commands-integration",
    HOST: "127.0.0.1",
    PORT: String(remoteCommandsPort),
    RC_STORE: "memory",
    AUTH_REQUIRED: "false",
    RC_USE_KAFKA: "false",
    RC_GATEWAY_HTTP_URL: VEHICLE_GATEWAY_BASE_URL,
    RC_PUBLIC_HTTP_URL: REMOTE_COMMANDS_BASE_URL,
    DOCS_ENABLED: "false",
  };

  // Vehicle-gateway:
  // - Disable telematics publishing (not needed).
  // - Force remote-commands callback URL to remote-commands test port.
  // - Force RC_USE_KAFKA=false to use HTTP ack callback.
  const gatewayEnv = {
    ...baseEnv,
    SERVICE_NAME: "vehicle-gateway-integration",
    HOST: "127.0.0.1",
    PORT: String(gatewayPort),
    TELEMATICS_PUBLISH_ENABLED: "false",
    RC_ENABLED: "true",
    RC_USE_KAFKA: "false",
    RC_REMOTE_COMMANDS_HTTP_URL: REMOTE_COMMANDS_BASE_URL,
    DOCS_ENABLED: "false",
  };

  const rc = spawnService({
    name: "remote-commands",
    cwd: rcRoot,
    env: remoteCommandsEnv,
    nodeArgs: [path.join(rcRoot, "src", "index.js")],
  });

  const gw = spawnService({
    name: "vehicle-gateway",
    cwd: gwRoot,
    env: gatewayEnv,
    nodeArgs: [path.join(gwRoot, "src", "index.js")],
  });

  fs.writeFileSync(
    pidFilePath(),
    JSON.stringify(
      {
        createdAt: new Date().toISOString(),
        pids: [rc.pid, gw.pid].filter(Boolean),
      },
      null,
      2
    ),
    "utf-8"
  );

  // Ensure readiness before running tests.
  await waitForHealthy(REMOTE_COMMANDS_BASE_URL, DEFAULT_TIMEOUT_MS);
  await waitForHealthy(VEHICLE_GATEWAY_BASE_URL, DEFAULT_TIMEOUT_MS);

  const envForTests = {
    VEHICLE_GATEWAY_BASE_URL,
    REMOTE_COMMANDS_BASE_URL,
    AUTH_REQUIRED: "false",
    RC_USE_KAFKA: "false",
    TELEMATICS_PUBLISH_ENABLED: "false",
  };

  return { pids: [rc.pid, gw.pid].filter(Boolean), envForTests };
}

/**
 * PUBLIC_INTERFACE
 * Stop services started by startServicesForIntegrationTests() using persisted PIDs.
 *
 * @returns {Promise<void>}
 */
async function stopServicesForIntegrationTests() {
  if (!fs.existsSync(pidFilePath())) return;

  /** @type {{pids?: number[]}} */
  let data;
  try {
    data = JSON.parse(fs.readFileSync(pidFilePath(), "utf-8"));
  } catch (_) {
    data = {};
  }

  const pids = Array.isArray(data.pids) ? data.pids : [];

  // SIGTERM first (graceful), then SIGKILL as a last resort.
  for (const pid of pids) {
    try {
      process.kill(pid, "SIGTERM");
    } catch (_) {}
  }

  // Give processes time to exit.
  await sleep(750);

  for (const pid of pids) {
    try {
      process.kill(pid, 0); // check if still alive
      process.kill(pid, "SIGKILL");
    } catch (_) {}
  }

  try {
    fs.unlinkSync(pidFilePath());
  } catch (_) {}
}

module.exports = {
  startServicesForIntegrationTests,
  stopServicesForIntegrationTests,
  buildHarnessEnv,
};
