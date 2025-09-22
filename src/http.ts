import https from "https";

export interface HttpClient {
  fetchText(url: string): Promise<string>;
  fetchBinary(url: string): Promise<Buffer>;
}

export const defaultHttpClient: HttpClient = {
  fetchText: (url: string) =>
    new Promise((resolvePromise, rejectPromise) => {
      const request = https.get(
        url,
        {
          headers: {
            "User-Agent": "rain-local-db-sync/1.0",
          },
        },
        (response) => {
          if (response.statusCode !== 200) {
            response.resume();
            rejectPromise(
              new Error(
                `Request to ${url} failed with status ${response.statusCode ?? "unknown"}`,
              ),
            );
            return;
          }

          const chunks: Buffer[] = [];
          response.on("data", (chunk: Buffer) => {
            chunks.push(chunk);
          });
          response.on("end", () => {
            resolvePromise(Buffer.concat(chunks).toString("utf-8"));
          });
        },
      );

      request.on("error", (error) => {
        rejectPromise(error);
      });
    }),
  fetchBinary: (url: string) =>
    new Promise((resolvePromise, rejectPromise) => {
      const request = https.get(
        url,
        {
          headers: {
            "User-Agent": "rain-local-db-sync/1.0",
          },
        },
        (response) => {
          if (response.statusCode !== 200) {
            response.resume();
            rejectPromise(
              new Error(
                `Request to ${url} failed with status ${response.statusCode ?? "unknown"}`,
              ),
            );
            return;
          }

          const chunks: Buffer[] = [];
          response.on("data", (chunk: Buffer) => {
            chunks.push(chunk);
          });
          response.on("end", () => {
            resolvePromise(Buffer.concat(chunks));
          });
        },
      );

      request.on("error", (error) => {
        rejectPromise(error);
      });
    }),
};
