use anyhow::{anyhow, Context, Result};
use reqwest::{Certificate, Identity};
use std::env;
use std::fs;
use std::time::Duration;

const BACKTEST_API_MTLS_CA_CERT_ENV: &str = "BACKTEST_API_MTLS_CA_CERT";
const BACKTEST_API_MTLS_CLIENT_CERT_ENV: &str = "BACKTEST_API_MTLS_CLIENT_CERT";
const BACKTEST_API_MTLS_CLIENT_KEY_ENV: &str = "BACKTEST_API_MTLS_CLIENT_KEY";

fn env_path(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn load_mtls_ca_certificate() -> Result<Option<Certificate>> {
    let Some(path) = env_path(BACKTEST_API_MTLS_CA_CERT_ENV) else {
        return Ok(None);
    };
    let pem = fs::read(&path)
        .with_context(|| format!("failed to read mTLS CA certificate from {}", path))?;
    let certificate = Certificate::from_pem(&pem)
        .with_context(|| format!("failed to parse mTLS CA certificate from {}", path))?;
    Ok(Some(certificate))
}

fn load_mtls_identity() -> Result<Option<Identity>> {
    let cert_path = env_path(BACKTEST_API_MTLS_CLIENT_CERT_ENV);
    let key_path = env_path(BACKTEST_API_MTLS_CLIENT_KEY_ENV);

    match (cert_path, key_path) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(anyhow!(
            "{} and {} must both be set when enabling API mTLS",
            BACKTEST_API_MTLS_CLIENT_CERT_ENV,
            BACKTEST_API_MTLS_CLIENT_KEY_ENV
        )),
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = fs::read(&cert_path).with_context(|| {
                format!("failed to read mTLS client certificate from {}", cert_path)
            })?;
            let key_pem = fs::read(&key_path)
                .with_context(|| format!("failed to read mTLS client key from {}", key_path))?;

            let mut identity_pem = cert_pem;
            if !identity_pem.ends_with(b"\n") {
                identity_pem.push(b'\n');
            }
            identity_pem.extend_from_slice(&key_pem);

            let identity = Identity::from_pem(&identity_pem).with_context(|| {
                format!(
                    "failed to parse mTLS client identity from {} and {}",
                    cert_path, key_path
                )
            })?;
            Ok(Some(identity))
        }
    }
}

pub fn build_async_client(timeout: Option<Duration>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    if let Some(certificate) = load_mtls_ca_certificate()? {
        builder = builder.add_root_certificate(certificate);
    }
    if let Some(identity) = load_mtls_identity()? {
        builder = builder.identity(identity);
    }
    builder.build().context("failed to build HTTP client")
}

pub fn build_blocking_client(timeout: Option<Duration>) -> Result<reqwest::blocking::Client> {
    let mut builder = reqwest::blocking::Client::builder();
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    if let Some(certificate) = load_mtls_ca_certificate()? {
        builder = builder.add_root_certificate(certificate);
    }
    if let Some(identity) = load_mtls_identity()? {
        builder = builder.identity(identity);
    }
    builder.build().context("failed to build HTTP client")
}
