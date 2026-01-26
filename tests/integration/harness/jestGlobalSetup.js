"use strict";

const { startServicesForIntegrationTests } = require("./serviceHarness");

/**
 * PUBLIC_INTERFACE
 * Jest globalSetup for the integration test suite.
 *
 * Starts vehicle-gateway and remote-commands on dedicated test ports, waits for /health,
 * and sets process.env base URLs so tests target the correct services.
 *
 * @returns {Promise<void>}
 */
module.exports = async function globalSetup() {
  const { envForTests } = await startServicesForIntegrationTests();

  for (const [k, v] of Object.entries(envForTests)) {
    process.env[k] = v;
  }
};
