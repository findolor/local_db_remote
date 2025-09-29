use anyhow::{Context, Result};
use reqwest::blocking::Client;

pub trait HttpClient {
    fn fetch_text(&self, url: &str) -> Result<String>;
    fn fetch_binary(&self, url: &str) -> Result<Vec<u8>>;
}

#[derive(Clone, Debug)]
pub struct DefaultHttpClient {
    client: Client,
}

impl Default for DefaultHttpClient {
    fn default() -> Self {
        let client = Client::builder()
            .user_agent("rain-local-db-sync/1.0")
            .build()
            .expect("failed to construct reqwest client");
        Self { client }
    }
}

impl HttpClient for DefaultHttpClient {
    fn fetch_text(&self, url: &str) -> Result<String> {
        let response = self
            .client
            .get(url)
            .send()
            .with_context(|| format!("request to {url} failed"))?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("request to {url} failed with status {status}");
        }
        response
            .text()
            .with_context(|| format!("failed to read body from {url}"))
    }

    fn fetch_binary(&self, url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .with_context(|| format!("request to {url} failed"))?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("request to {url} failed with status {status}");
        }
        response
            .bytes()
            .map(|bytes| bytes.to_vec())
            .with_context(|| format!("failed to read body from {url}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::matchers::*;
    use httptest::responders::*;
    use httptest::{Expectation, Server};

    #[test]
    fn fetch_text_returns_server_response() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/text"))
                .respond_with(status_code(200).body("hello")),
        );

        let client = DefaultHttpClient::default();
        let url = server.url("/text").to_string();
        let body = client.fetch_text(&url).unwrap();
        assert_eq!(body, "hello");
    }

    #[test]
    fn fetch_binary_returns_bytes() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::path("/bin"))
                .respond_with(status_code(200).body(vec![1, 2, 3])),
        );

        let client = DefaultHttpClient::default();
        let url = server.url("/bin").to_string();
        let bytes = client.fetch_binary(&url).unwrap();
        assert_eq!(bytes, vec![1, 2, 3]);
    }

    #[test]
    fn fetch_text_fails_on_error_status() {
        let server = Server::run();
        server.expect(Expectation::matching(request::path("/fail")).respond_with(status_code(500)));

        let client = DefaultHttpClient::default();
        let url = server.url("/fail").to_string();
        let err = client.fetch_text(&url).unwrap_err();
        assert!(err.to_string().contains("status 500"));
    }
}
