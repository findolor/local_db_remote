import { EventEmitter } from "events";
import { afterEach, describe, expect, it, vi } from "vitest";

const { getMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
}));

vi.mock("https", () => ({
  default: { get: getMock },
  get: getMock,
}));

import { defaultHttpClient } from "../http";

afterEach(() => {
  getMock.mockReset();
});

describe("defaultHttpClient.fetchText", () => {
  it("resolves when the request succeeds", async () => {
    getMock.mockImplementation((_url, _options, callback) => {
      const response = new EventEmitter() as EventEmitter & { statusCode?: number };
      response.statusCode = 200;
      callback(response);
      response.emit("data", Buffer.from("hello"));
      response.emit("data", Buffer.from(" world"));
      response.emit("end");
      return new EventEmitter();
    });

    await expect(defaultHttpClient.fetchText("https://example.com"))
      .resolves.toBe("hello world");
  });

  it("rejects when a non-200 status is returned", async () => {
    getMock.mockImplementation((_url, _options, callback) => {
      const response = new EventEmitter() as EventEmitter & { statusCode?: number; resume: () => void };
      response.statusCode = 500;
      response.resume = () => undefined;
      callback(response);
      return new EventEmitter();
    });

    await expect(defaultHttpClient.fetchText("https://example.com"))
      .rejects.toThrow("Request to https://example.com failed with status 500");
  });

  it("rejects when the request errors", async () => {
    getMock.mockImplementation(() => {
      const request = new EventEmitter();
      queueMicrotask(() => {
        request.emit("error", new Error("network"));
      });
      return request;
    });

    await expect(defaultHttpClient.fetchText("https://example.com"))
      .rejects.toThrow("network");
  });
});

describe("defaultHttpClient.fetchBinary", () => {
  it("collects buffers into a single result", async () => {
    getMock.mockImplementation((_url, _options, callback) => {
      const response = new EventEmitter() as EventEmitter & { statusCode?: number };
      response.statusCode = 200;
      callback(response);
      response.emit("data", Buffer.from([1, 2]));
      response.emit("data", Buffer.from([3]));
      response.emit("end");
      return new EventEmitter();
    });

    await expect(defaultHttpClient.fetchBinary("https://example.com"))
      .resolves.toEqual(Buffer.from([1, 2, 3]));
  });
});
