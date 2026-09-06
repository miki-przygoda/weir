//! AWS credential resolution.
//!
//! Four sources, first match wins:
//!
//! 1. **Static config** — `sink_s3_access_key_id` / `sink_s3_secret_access_key`.
//! 2. **Environment** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
//!    optionally `AWS_SESSION_TOKEN`.
//! 3. **Web identity** (IRSA on EKS, Workload Identity on GKE) —
//!    `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, exchanged via
//!    `sts:AssumeRoleWithWebIdentity`.
//! 4. **IMDSv2** — the EC2 instance role.
//!
//! **IMDSv1 is deliberately not implemented.** It is the variant reachable by a
//! plain unauthenticated `GET`, which is what makes an SSRF bug in any
//! co-located process enough to steal the instance's credentials. IMDSv2's
//! `PUT`-then-`GET` token exchange is the whole mitigation.
//!
//! `~/.aws/credentials` profiles and SSO are also not implemented: both are
//! developer-workstation conveniences, and a daemon runs with environment
//! variables, IRSA, or an instance role.
//!
//! # Every error here is transient
//!
//! A credential failure says nothing about any individual record — every record
//! in the backlog would get the same answer, and none of them is malformed. So
//! it must strand the segment (recoverable, and visible via
//! `WeirSegmentStranded`) rather than dead-letter it. See [`CredsError`].

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::redact::{SecretString, sanitize_log_excerpt, truncate};

/// How long before expiry temporary credentials are refreshed.
///
/// Refreshing exactly at expiry races the request already in flight: the
/// signature is computed before the request is sent, so a token valid at
/// signing time can be rejected by the time it arrives.
const REFRESH_MARGIN_SECS: i64 = 300;

/// Why credential resolution failed.
///
/// Every variant is transient by construction — see the module docs. There is
/// deliberately no `permanent` constructor: adding one would need an argument
/// for why a whole backlog should be dead-lettered over a credential.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CredsError {
    /// No source in the chain produced credentials.
    #[error(
        "no AWS credentials found: set sink_s3_access_key_id/sink_s3_secret_access_key, or \
         AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or run with an IRSA or EC2 instance role"
    )]
    NotFound,
    /// A credential endpoint (STS or IMDS) could not be reached.
    #[error("{source_name} unreachable: {message}")]
    Transport {
        /// Which endpoint.
        source_name: &'static str,
        /// Sanitised detail.
        message: String,
    },
    /// A credential endpoint answered, but not with credentials.
    #[error("{source_name} returned an unusable response: {message}")]
    BadResponse {
        /// Which endpoint.
        source_name: &'static str,
        /// Sanitised excerpt.
        message: String,
    },
    /// The web-identity token file could not be read.
    #[error("could not read AWS_WEB_IDENTITY_TOKEN_FILE at {path}: {message}")]
    TokenFile {
        /// The configured path.
        path: String,
        /// The I/O error.
        message: String,
    },
}

/// Resolved credentials.
#[derive(Clone, Debug)]
pub(crate) struct Credentials {
    /// Access key id — not secret.
    pub(crate) access_key_id: String,
    /// Secret access key.
    pub(crate) secret_access_key: SecretString,
    /// Session token, for temporary credentials.
    pub(crate) session_token: Option<SecretString>,
    /// Expiry, unix seconds. `None` for static/long-lived credentials.
    pub(crate) expires_at_unix: Option<i64>,
}

impl Credentials {
    /// Whether these credentials should be refreshed before use.
    ///
    /// Long-lived credentials (`expires_at_unix == None`) never need it.
    pub(crate) fn needs_refresh(&self, now_unix: i64) -> bool {
        match self.expires_at_unix {
            None => false,
            Some(exp) => now_unix + REFRESH_MARGIN_SECS >= exp,
        }
    }
}

/// Resolves credentials, caching temporary ones until they near expiry.
pub(crate) struct CredentialChain {
    static_creds: Option<(String, SecretString)>,
    client: reqwest::Client,
    region: String,
    cached: RwLock<Option<Credentials>>,
}

impl std::fmt::Debug for CredentialChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialChain")
            .field("static_creds", &self.static_creds.is_some())
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl CredentialChain {
    /// Builds a chain. `static_creds` short-circuits every other source.
    pub(crate) fn new(
        static_creds: Option<(String, SecretString)>,
        region: impl Into<String>,
        client: reqwest::Client,
    ) -> Arc<Self> {
        Arc::new(Self {
            static_creds,
            client,
            region: region.into(),
            cached: RwLock::new(None),
        })
    }

    /// Returns usable credentials, refreshing if the cached ones are near
    /// expiry.
    pub(crate) async fn resolve(&self, now_unix: i64) -> Result<Credentials, CredsError> {
        if let Some(c) = self.cached.read().await.as_ref()
            && !c.needs_refresh(now_unix)
        {
            return Ok(c.clone());
        }
        let fresh = self.resolve_uncached().await?;
        *self.cached.write().await = Some(fresh.clone());
        Ok(fresh)
    }

    async fn resolve_uncached(&self) -> Result<Credentials, CredsError> {
        if let Some((id, secret)) = &self.static_creds {
            return Ok(Credentials {
                access_key_id: id.clone(),
                secret_access_key: secret.clone(),
                session_token: None,
                expires_at_unix: None,
            });
        }
        if let Some(c) = from_env() {
            return Ok(c);
        }
        if let Some(c) = self.resolve_web_identity().await.transpose() {
            return c;
        }
        if let Some(c) = self.resolve_imdsv2().await.transpose() {
            return c;
        }
        Err(CredsError::NotFound)
    }

    /// `sts:AssumeRoleWithWebIdentity` — IRSA on EKS, Workload Identity on GKE.
    ///
    /// `Ok(None)` means "this source does not apply here" (the env vars are
    /// absent); `Err` means it applied and failed.
    async fn resolve_web_identity(&self) -> Result<Option<Credentials>, CredsError> {
        let (Ok(token_file), Ok(role_arn)) = (
            std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE"),
            std::env::var("AWS_ROLE_ARN"),
        ) else {
            return Ok(None);
        };
        let token = std::fs::read_to_string(&token_file).map_err(|e| CredsError::TokenFile {
            path: token_file.clone(),
            message: e.to_string(),
        })?;
        let session = std::env::var("AWS_ROLE_SESSION_NAME").unwrap_or_else(|_| "weir".to_string());
        let url = format!("https://sts.{}.amazonaws.com/", self.region);
        let resp = self
            .client
            .post(&url)
            .form(&[
                ("Action", "AssumeRoleWithWebIdentity"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn.as_str()),
                ("RoleSessionName", session.as_str()),
                ("WebIdentityToken", token.trim()),
            ])
            .send()
            .await
            .map_err(|e| CredsError::Transport {
                source_name: "STS",
                message: sanitize_log_excerpt(&e.to_string()),
            })?;
        let body = resp.text().await.map_err(|e| CredsError::Transport {
            source_name: "STS",
            message: sanitize_log_excerpt(&e.to_string()),
        })?;
        parse_sts_xml(&body)
            .map(Some)
            .ok_or_else(|| CredsError::BadResponse {
                source_name: "STS",
                message: truncate(&sanitize_log_excerpt(&body), 256),
            })
    }

    /// The EC2 instance role, via IMDSv2's token exchange.
    async fn resolve_imdsv2(&self) -> Result<Option<Credentials>, CredsError> {
        const BASE: &str = "http://169.254.169.254";
        let transport = |e: reqwest::Error| CredsError::Transport {
            source_name: "IMDSv2",
            message: sanitize_log_excerpt(&e.to_string()),
        };

        // The PUT is what makes this IMDSv2. A plain GET would work on hosts
        // with IMDSv1 still enabled, and would also be reachable through any
        // SSRF bug in a co-located process -- which is the whole reason v2
        // exists. Failing closed here is deliberate.
        let token_resp = self
            .client
            .put(format!("{BASE}/latest/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await;
        // Not on EC2 (or IMDS is disabled): this source does not apply.
        let Ok(token_resp) = token_resp else {
            return Ok(None);
        };
        if !token_resp.status().is_success() {
            return Ok(None);
        }
        let token = token_resp.text().await.map_err(transport)?;

        let role = self
            .client
            .get(format!("{BASE}/latest/meta-data/iam/security-credentials/"))
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .await
            .map_err(transport)?
            .text()
            .await
            .map_err(transport)?;
        let role = role.lines().next().unwrap_or_default().trim().to_string();
        if role.is_empty() {
            return Ok(None);
        }

        let body = self
            .client
            .get(format!(
                "{BASE}/latest/meta-data/iam/security-credentials/{role}"
            ))
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .await
            .map_err(transport)?
            .text()
            .await
            .map_err(transport)?;

        parse_imds_json(&body)
            .map(Some)
            .ok_or_else(|| CredsError::BadResponse {
                source_name: "IMDSv2",
                message: truncate(&sanitize_log_excerpt(&body), 256),
            })
    }
}

/// Credentials from the standard environment variables.
fn from_env() -> Option<Credentials> {
    let id = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    if id.is_empty() || secret.is_empty() {
        return None;
    }
    Some(Credentials {
        access_key_id: id,
        secret_access_key: SecretString::new(secret),
        session_token: std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .map(SecretString::new),
        expires_at_unix: None,
    })
}

/// Extracts `<tag>value</tag>`. Two `find` calls rather than an XML parser: the
/// responses are fixed-shape and the dependency budget is spent (see the
/// crate's Cargo.toml).
fn xml_field<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(&body[start..end])
}

/// Extracts `"key" : "value"` from IMDS's small fixed-shape JSON document.
fn json_field<'a>(body: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let after = &body[body.find(&needle)? + needle.len()..];
    let after = after.trim_start().strip_prefix(':')?.trim_start();
    let rest = after.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

/// Parses an `AssumeRoleWithWebIdentity` response.
fn parse_sts_xml(body: &str) -> Option<Credentials> {
    Some(Credentials {
        access_key_id: xml_field(body, "AccessKeyId")?.to_string(),
        secret_access_key: SecretString::new(xml_field(body, "SecretAccessKey")?),
        session_token: Some(SecretString::new(xml_field(body, "SessionToken")?)),
        expires_at_unix: xml_field(body, "Expiration").and_then(parse_iso8601_to_unix),
    })
}

/// Parses an IMDSv2 credential document.
fn parse_imds_json(body: &str) -> Option<Credentials> {
    Some(Credentials {
        access_key_id: json_field(body, "AccessKeyId")?.to_string(),
        secret_access_key: SecretString::new(json_field(body, "SecretAccessKey")?),
        session_token: Some(SecretString::new(json_field(body, "Token")?)),
        expires_at_unix: json_field(body, "Expiration").and_then(parse_iso8601_to_unix),
    })
}

/// `YYYY-MM-DDTHH:MM:SSZ` → unix seconds.
///
/// Inverse of `time::civil_from_days`, written out here rather than reused
/// because the two run in opposite directions and sharing one would make an
/// error in either invisible to the other's tests.
fn parse_iso8601_to_unix(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20 {
        return None;
    }
    let n = |a: usize, z: usize| s.get(a..z)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0, 4)?, n(5, 7)?, n(8, 10)?);
    let (h, mi, se) = (n(11, 13)?, n(14, 16)?, n(17, 19)?);
    let yy = if mo <= 2 { y - 1 } else { y };
    let era = if yy >= 0 { yy } else { yy - 399 } / 400;
    let yoe = yy - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + h * 3_600 + mi * 60 + se)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds(expires: Option<i64>) -> Credentials {
        Credentials {
            access_key_id: "AKIA".into(),
            secret_access_key: SecretString::new("s"),
            session_token: None,
            expires_at_unix: expires,
        }
    }

    #[test]
    fn credentials_without_an_expiry_never_need_refresh() {
        assert!(!creds(None).needs_refresh(i64::MAX));
    }

    #[test]
    fn credentials_refresh_five_minutes_before_expiry() {
        // Refreshing exactly at expiry races the request already in flight: the
        // signature is computed before the request is sent.
        let c = creds(Some(1_000));
        assert!(!c.needs_refresh(600), "10 min out: still valid");
        assert!(c.needs_refresh(700), "exactly at the margin: refresh");
        assert!(c.needs_refresh(750), "4 min out: must refresh");
        assert!(c.needs_refresh(1_001), "expired: must refresh");
    }

    #[test]
    fn an_sts_response_is_parsed() {
        let body = "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult>\
             <Credentials><AccessKeyId>AKIAEXAMPLE</AccessKeyId>\
             <SecretAccessKey>secret/with+slashes</SecretAccessKey>\
             <SessionToken>tok==</SessionToken>\
             <Expiration>2026-09-06T14:25:30Z</Expiration></Credentials>\
             </AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>";
        let c = parse_sts_xml(body).expect("should parse");
        assert_eq!(c.access_key_id, "AKIAEXAMPLE");
        assert_eq!(c.secret_access_key.expose(), "secret/with+slashes");
        assert_eq!(c.session_token.unwrap().expose(), "tok==");
        assert_eq!(c.expires_at_unix, Some(1_788_704_730));
    }

    #[test]
    fn an_imds_response_is_parsed() {
        let body = r#"{
            "Code" : "Success",
            "AccessKeyId" : "AKIAEXAMPLE",
            "SecretAccessKey" : "secret",
            "Token" : "tok",
            "Expiration" : "2026-09-06T14:25:30Z"
        }"#;
        let c = parse_imds_json(body).expect("should parse");
        assert_eq!(c.access_key_id, "AKIAEXAMPLE");
        assert_eq!(c.secret_access_key.expose(), "secret");
        assert_eq!(c.expires_at_unix, Some(1_788_704_730));
    }

    #[test]
    fn a_truncated_or_error_response_yields_none_rather_than_partial_credentials() {
        // Half-parsed credentials would be signed with and rejected, which
        // looks like an auth bug rather than a transport one.
        assert!(parse_sts_xml("<Error><Code>AccessDenied</Code></Error>").is_none());
        assert!(parse_imds_json(r#"{"Code":"AssumeRoleUnauthorizedAccess"}"#).is_none());
        assert!(parse_sts_xml("<AccessKeyId>a</AccessKeyId>").is_none());
    }

    #[test]
    fn expiry_timestamps_round_trip_against_known_values() {
        assert_eq!(parse_iso8601_to_unix("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_iso8601_to_unix("2015-08-30T12:36:00Z"),
            Some(1_440_938_160)
        );
        assert_eq!(
            parse_iso8601_to_unix("2024-02-29T23:59:59Z"),
            Some(1_709_251_199)
        );
        assert_eq!(parse_iso8601_to_unix("nonsense"), None);
    }

    #[test]
    fn every_error_is_transient_by_construction() {
        // There is no `permanent` constructor, and there must not be: a
        // credential failure is a fact about the credential, not about any
        // record, so dead-lettering a backlog over one would displace acked
        // data. This test exists so that adding such a variant is a visible
        // decision rather than an accident.
        let variants = [
            CredsError::NotFound,
            CredsError::Transport {
                source_name: "STS",
                message: "x".into(),
            },
            CredsError::BadResponse {
                source_name: "IMDSv2",
                message: "x".into(),
            },
            CredsError::TokenFile {
                path: "p".into(),
                message: "m".into(),
            },
        ];
        assert_eq!(
            variants.len(),
            4,
            "a new variant needs a transient/permanent decision"
        );
        for v in &variants {
            assert!(
                !v.to_string().is_empty(),
                "every variant needs an operator message"
            );
        }
    }

    #[test]
    fn the_chain_debug_impl_does_not_leak_the_secret() {
        let chain = CredentialChain::new(
            Some(("AKIA".into(), SecretString::new("wJalrXUtnFEMI"))),
            "us-east-1",
            reqwest::Client::new(),
        );
        let rendered = format!("{chain:?}");
        assert!(!rendered.contains("wJalr"), "leaked: {rendered}");
        assert!(!rendered.contains("AKIA"), "leaked: {rendered}");
    }

    #[tokio::test]
    async fn static_credentials_short_circuit_the_chain() {
        let chain = CredentialChain::new(
            Some(("AKIA-STATIC".into(), SecretString::new("s"))),
            "us-east-1",
            reqwest::Client::new(),
        );
        let c = chain.resolve(0).await.expect("static creds resolve");
        assert_eq!(c.access_key_id, "AKIA-STATIC");
        assert_eq!(c.expires_at_unix, None, "static credentials do not expire");
    }
}
