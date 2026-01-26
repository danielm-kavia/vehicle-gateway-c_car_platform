"use strict";

const { loadConfig } = require("../src/config");

describe("vehicle-gateway baseline", () => {
  test("loadConfig returns expected shape", () => {
    const cfg = loadConfig();

    expect(cfg).toBeTruthy();
    expect(typeof cfg.serviceName).toBe("string");
    expect(typeof cfg.port).toBe("number");
    expect(cfg.publish).toBeTruthy();
    expect(["kafka", "http"]).toContain(cfg.publish.mode);
    expect(cfg.remoteCommands).toBeTruthy();
    expect(typeof cfg.remoteCommands.enabled).toBe("boolean");
  });
});
