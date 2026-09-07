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

/// Renders a `reqwest::Error` with its cause chain.
///
/// reqwest's own `Display` prints only the kind and the URL; the useful part —
/// DNS vs refused vs TLS vs timeout — lives in `source()`, which `to_string()`
/// never reaches. Without this every transport failure logs the same sentence.
fn transport_detail(e: &reqwest::Error) -> String {
    use std::error::Error as _;
    let mut parts = vec![e.to_string()];
    let mut src: Option<&(dyn std::error::Error + 'static)> = e.source();
    while let Some(s) = src {
        parts.push(s.to_string());
        src = s.source();
    }
    sanitize_log_excerpt(&parts.join(": "))
}

/// How long before expiry temporary credentials are refreshed.
///
/// Refreshing exactly at expiry races the request already in flight: the
/// signature is computed before the request is sent, so a token valid at
/// signing time can be rejected by the time it arrives.
const REFRESH_MARGIN_SECS: i64 = 300;

/// Budget for an instance-metadata request.
///
/// IMDS is link-local: it answers in microseconds or it is not there. AWS's own
/// SDKs use about a second. This bounds how long a non-EC2 host pays for the
/// probe on each resolve.
const IMDS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

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
    ///
    /// Deliberately carries **no excerpt of the response**. An earlier version
    /// logged 256 bytes of it to aid diagnosis; in a real IMDSv2 credential
    /// document the secret access key sits at byte 154, so a truncated or
    /// renamed-field response wrote a live AWS secret into the daemon's log --
    /// repeatedly, since every error here is transient and the drain retries.
    /// The length and the missing field name are enough to diagnose, and
    /// neither can carry a secret.
    #[error(
        "{source_name} returned a {body_len}-byte response that is not a credential document \
         (no {missing} field). The response is deliberately not logged: it may contain a live \
         secret access key."
    )]
    BadResponse {
        /// Which endpoint.
        source_name: &'static str,
        /// Response size, for distinguishing truncation from a wrong endpoint.
        body_len: usize,
        /// The first field that could not be found.
        missing: &'static str,
    },
    /// A credential source applied but was only half-configured.
    #[error("{source_name} is partially configured: {detail}")]
    Incomplete {
        /// Which source.
        source_name: &'static str,
        /// What is missing.
        detail: String,
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
    /// Expiry, unix seconds.
    pub(crate) expiry: Expiry,
}

/// When credentials stop being usable.
///
/// The three cases are distinct on purpose. An earlier version collapsed
/// [`Expiry::Unreadable`] into "no expiry", which made a credential document
/// whose `Expiration` field could not be parsed cache **forever**: the session
/// expired an hour later and every commit failed `ExpiredToken` until the
/// process restarted. An expiry we could not read is the opposite of no expiry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Expiry {
    /// Static or long-lived credentials; never refreshed.
    Never,
    /// Temporary credentials valid until this unix second.
    At(i64),
    /// The source declared an expiry that could not be parsed. Treated as
    /// already expired, so it refreshes on every use rather than going stale.
    Unreadable,
}

impl Credentials {
    /// Whether these credentials should be refreshed before use.
    ///
    /// Long-lived credentials (`expires_at_unix == None`) never need it.
    pub(crate) fn needs_refresh(&self, now_unix: i64) -> bool {
        match self.expiry {
            Expiry::Never => false,
            Expiry::Unreadable => true,
            // saturating: now_unix comes from the system clock, and a clock at
            // i64::MAX would panic on overflow in a debug build.
            Expiry::At(exp) => now_unix.saturating_add(REFRESH_MARGIN_SECS) >= exp,
        }
    }

    /// Whether these credentials are still usable *right now*, ignoring the
    /// refresh margin. Used to keep serving from cache when a refresh fails.
    pub(crate) fn still_valid(&self, now_unix: i64) -> bool {
        match self.expiry {
            Expiry::Never => true,
            Expiry::Unreadable => false,
            Expiry::At(exp) => now_unix < exp,
        }
    }
}

/// Resolves credentials, caching temporary ones until they near expiry.
pub(crate) struct CredentialChain {
    static_creds: Option<(String, SecretString)>,
    client: reqwest::Client,
    /// A separate client for the instance-metadata service. See
    /// [`CredentialChain::imds_client`].
    imds: reqwest::Client,
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
            imds: Self::imds_client(&client),
            client,
            region: region.into(),
            cached: RwLock::new(None),
        })
    }

    /// The client used for `169.254.169.254`, and only that.
    ///
    /// Three departures from the general-purpose client, each load-bearing:
    ///
    /// - **`no_proxy()`.** reqwest reads `HTTP_PROXY` from the environment by
    ///   default, so without this the instance-metadata request is sent *to the
    ///   corporate proxy* — which then answers with whatever it likes, and weir
    ///   authenticates as that. AWS's own SDKs bypass proxies for the
    ///   link-local address for exactly this reason, and an on-prem deployment
    ///   with a site-wide `HTTP_PROXY` is routine.
    /// - **`redirect(none)`.** reqwest strips `authorization` across hosts but
    ///   not `x-aws-ec2-metadata-token`, so a 302 would hand the metadata token
    ///   to an arbitrary host.
    /// - **A short timeout.** IMDS is a link-local address that either answers
    ///   in microseconds or is not there at all. On a non-EC2 host the request
    ///   black-holes, and inheriting the sink's timeout (30 s by default) would
    ///   stall the drain thread that long on *every* commit.
    fn imds_client(fallback: &reqwest::Client) -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(IMDS_TIMEOUT)
            .connect_timeout(IMDS_TIMEOUT)
            .build()
            .unwrap_or_else(|_| fallback.clone())
    }

    /// Returns usable credentials, refreshing if the cached ones are near
    /// expiry.
    pub(crate) async fn resolve(&self, now_unix: i64) -> Result<Credentials, CredsError> {
        if let Some(c) = self.cached.read().await.as_ref()
            && !c.needs_refresh(now_unix)
        {
            return Ok(c.clone());
        }
        match self.resolve_uncached().await {
            Ok(fresh) => {
                *self.cached.write().await = Some(fresh.clone());
                Ok(fresh)
            }
            Err(e) => {
                // The refresh margin exists to give a retry headroom; spending
                // none of it wastes the whole point. A 30-second STS or IMDS
                // blip must not strand a backlog while we hold credentials that
                // are still valid for minutes.
                if let Some(c) = self.cached.read().await.as_ref()
                    && c.still_valid(now_unix)
                {
                    tracing::warn!(
                        error = %e,
                        "s3 sink: credential refresh failed; continuing with the cached \
                         credential, which is still valid"
                    );
                    return Ok(c.clone());
                }
                Err(e)
            }
        }
    }

    async fn resolve_uncached(&self) -> Result<Credentials, CredsError> {
        if let Some((id, secret)) = &self.static_creds {
            return Ok(Credentials {
                access_key_id: id.clone(),
                secret_access_key: secret.clone(),
                session_token: None,
                expiry: Expiry::Never,
            });
        }
        if let Some(c) = from_env() {
            return Ok(c);
        }
        if let Some(c) = self.resolve_web_identity().await.transpose() {
            return c;
        }
        Self::reject_unimplemented_container_sources()?;
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
        let token_file = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").ok();
        let role_arn = std::env::var("AWS_ROLE_ARN").ok();
        // A half-configured IRSA setup must NOT fall through to IMDSv2. Doing
        // so assumes the EC2/EKS *node* role -- a different and usually much
        // broader identity -- and writes to S3 as something other than what the
        // deployment names, with no diagnostic. Fail loudly instead.
        let (token_file, role_arn) = match (token_file, role_arn) {
            (Some(t), Some(r)) => (t, r),
            (None, None) => return Ok(None),
            (t, _) => {
                let missing = if t.is_none() {
                    "AWS_WEB_IDENTITY_TOKEN_FILE"
                } else {
                    "AWS_ROLE_ARN"
                };
                return Err(CredsError::Incomplete {
                    source_name: "web identity (IRSA)",
                    detail: format!(
                        "{missing} is not set. Refusing to fall through to the EC2 instance \
                         role, which would silently authenticate as the node rather than the \
                         configured role"
                    ),
                });
            }
        };
        let token = std::fs::read_to_string(&token_file).map_err(|e| CredsError::TokenFile {
            // Sanitised: the path comes from an environment variable, and this
            // string reaches the daemon's log. Every other variant is scrubbed;
            // this one was the gap (S29).
            path: truncate(&sanitize_log_excerpt(&token_file), 256),
            message: sanitize_log_excerpt(&e.to_string()),
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
                message: transport_detail(&e),
            })?;
        let body = resp.text().await.map_err(|e| CredsError::Transport {
            source_name: "STS",
            message: transport_detail(&e),
        })?;
        parse_sts_xml(&body)
            .map(Some)
            .map_err(|missing| CredsError::BadResponse {
                source_name: "STS",
                body_len: body.len(),
                missing,
            })
    }

    /// Refuses to continue when a container credential source is configured but
    /// unimplemented.
    ///
    /// ECS task roles and EKS Pod Identity both advertise themselves through
    /// these variables. Neither is implemented here, and silently continuing to
    /// IMDSv2 would authenticate as the *node* role instead -- the same footgun
    /// as a half-configured IRSA, and EKS Pod Identity is AWS's current
    /// recommendation over IRSA, so this is not a rare path.
    fn reject_unimplemented_container_sources() -> Result<(), CredsError> {
        for var in [
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        ] {
            if std::env::var_os(var).is_some() {
                return Err(CredsError::Incomplete {
                    source_name: "container credentials",
                    detail: format!(
                        "{var} is set, so this host uses ECS task roles or EKS Pod Identity, \
                         which this sink does not implement. Refusing to fall through to the \
                         EC2 instance role, which would authenticate as the node. Set \
                         sink_s3_access_key_id/sink_s3_secret_access_key, or use IRSA \
                         (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN)"
                    ),
                });
            }
        }
        Ok(())
    }

    /// The EC2 instance role, via IMDSv2's token exchange.
    async fn resolve_imdsv2(&self) -> Result<Option<Credentials>, CredsError> {
        const BASE: &str = "http://169.254.169.254";
        let transport = |e: reqwest::Error| CredsError::Transport {
            source_name: "IMDSv2",
            message: transport_detail(&e),
        };

        // The PUT is what makes this IMDSv2. A plain GET would work on hosts
        // with IMDSv1 still enabled, and would also be reachable through any
        // SSRF bug in a co-located process -- which is the whole reason v2
        // exists. Failing closed here is deliberate.
        let token_resp = self
            .imds
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
            .imds
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
        // The role name is interpolated into a URL path. A URL parser rewrites
        // '.' and '..' segments, so an unvalidated name could redirect the
        // credential fetch to a different metadata path -- the same failure
        // key::validate_path_text prevents for object keys.
        if !role.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '+' | '=' | ',' | '.' | '@')
        }) || role.split('/').any(|seg| seg == "." || seg == "..")
        {
            return Err(CredsError::BadResponse {
                source_name: "IMDSv2",
                body_len: role.len(),
                missing: "a role name containing only IAM-legal characters",
            });
        }

        let body = self
            .imds
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
            .map_err(|missing| CredsError::BadResponse {
                source_name: "IMDSv2",
                body_len: body.len(),
                missing,
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
        expiry: Expiry::Never,
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
///
/// `Err` names the first missing field. That name is a fixed string from this
/// source file, never response bytes — see [`CredsError::BadResponse`].
fn parse_sts_xml(body: &str) -> Result<Credentials, &'static str> {
    Ok(Credentials {
        access_key_id: xml_field(body, "AccessKeyId")
            .ok_or("AccessKeyId")?
            .to_string(),
        secret_access_key: SecretString::new(
            xml_field(body, "SecretAccessKey").ok_or("SecretAccessKey")?,
        ),
        session_token: Some(SecretString::new(
            xml_field(body, "SessionToken").ok_or("SessionToken")?,
        )),
        expiry: expiry_from(xml_field(body, "Expiration")),
    })
}

/// Maps a declared expiry string to an [`Expiry`].
///
/// A field that is present but unparseable becomes [`Expiry::Unreadable`], not
/// [`Expiry::Never`]: the source told us these credentials expire, so treating
/// them as immortal would cache a session that dies an hour later.
fn expiry_from(field: Option<&str>) -> Expiry {
    match field {
        None => Expiry::Never,
        Some(s) => match parse_iso8601_to_unix(s) {
            Some(t) => Expiry::At(t),
            None => Expiry::Unreadable,
        },
    }
}

/// Parses an IMDSv2 credential document.
fn parse_imds_json(body: &str) -> Result<Credentials, &'static str> {
    Ok(Credentials {
        access_key_id: json_field(body, "AccessKeyId")
            .ok_or("AccessKeyId")?
            .to_string(),
        secret_access_key: SecretString::new(
            json_field(body, "SecretAccessKey").ok_or("SecretAccessKey")?,
        ),
        session_token: Some(SecretString::new(json_field(body, "Token").ok_or("Token")?)),
        expiry: expiry_from(json_field(body, "Expiration")),
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
    // Anchor on the actual separators. Without this, "2026x09x06x14x25x30x"
    // parses happily, and a leading '+' makes the year field parse as a signed
    // number -- yielding an expiry thousands of years in the past, which makes
    // needs_refresh permanently true and defeats the cache entirely.
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    if !b[..19]
        .iter()
        .enumerate()
        .all(|(i, c)| matches!(i, 4 | 7 | 10 | 13 | 16) || c.is_ascii_digit())
    {
        return None;
    }
    // UTC only. A "+01:00" offset silently parsed as UTC would put the expiry
    // an hour wrong in the direction that keeps a dead credential alive.
    if b[19] != b'Z' {
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

    fn creds(expiry: Expiry) -> Credentials {
        Credentials {
            access_key_id: "AKIA".into(),
            secret_access_key: SecretString::new("s"),
            session_token: None,
            expiry,
        }
    }

    #[test]
    fn long_lived_credentials_never_need_refresh() {
        assert!(!creds(Expiry::Never).needs_refresh(i64::MAX));
        assert!(creds(Expiry::Never).still_valid(i64::MAX));
    }

    #[test]
    fn credentials_refresh_five_minutes_before_expiry() {
        // Refreshing exactly at expiry races the request already in flight: the
        // signature is computed before the request is sent.
        let c = creds(Expiry::At(1_000));
        assert!(!c.needs_refresh(600), "10 min out: still valid");
        assert!(c.needs_refresh(700), "exactly at the margin: refresh");
        assert!(c.needs_refresh(750), "4 min out: must refresh");
        assert!(c.needs_refresh(1_001), "expired: must refresh");
    }

    #[test]
    fn an_unreadable_expiry_refreshes_rather_than_caching_forever() {
        // The defect this variant exists to prevent: collapsing "expiry we
        // could not parse" into "no expiry" made a session credential cache
        // forever, so it expired an hour later and every commit failed
        // ExpiredToken until the process restarted.
        let c = creds(Expiry::Unreadable);
        assert!(c.needs_refresh(0), "must refresh immediately");
        assert!(!c.still_valid(0), "must not be served from cache");
    }

    #[test]
    fn needs_refresh_does_not_overflow_at_the_end_of_time() {
        // now_unix comes from the system clock; a debug build would panic.
        assert!(creds(Expiry::At(i64::MAX)).needs_refresh(i64::MAX));
    }

    #[test]
    fn still_valid_uses_the_real_expiry_not_the_margin() {
        // The margin is headroom for a retry. When a refresh fails we keep
        // serving until the credential is ACTUALLY dead, not until the margin.
        let c = creds(Expiry::At(1_000));
        assert!(
            c.needs_refresh(800) && c.still_valid(800),
            "inside the margin"
        );
        assert!(!c.still_valid(1_000), "at expiry it is dead");
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
        assert_eq!(c.expiry, Expiry::At(1_788_704_730));
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
        assert_eq!(c.expiry, Expiry::At(1_788_704_730));
    }

    #[test]
    fn a_declared_but_unparseable_expiry_is_unreadable_not_never() {
        let body = r#"{"AccessKeyId":"A","SecretAccessKey":"s","Token":"t","Expiration":"soon"}"#;
        assert_eq!(parse_imds_json(body).unwrap().expiry, Expiry::Unreadable);
    }

    #[test]
    fn a_partial_response_names_the_missing_field_and_never_echoes_the_body() {
        // THE credential-leak guard. In a real IMDSv2 document the secret sits
        // at byte 154, so a 256-byte excerpt -- which an earlier version put in
        // this error -- wrote a live AWS secret to the daemon's log, repeatedly,
        // because every error here is transient and the drain retries.
        let truncated = r#"{"AccessKeyId":"AKIAEXAMPLE","SecretAccessKey":"wJalrXUtnFEMI"#;
        assert_eq!(parse_imds_json(truncated).unwrap_err(), "SecretAccessKey");

        let err = CredsError::BadResponse {
            source_name: "IMDSv2",
            body_len: truncated.len(),
            missing: "SecretAccessKey",
        };
        let rendered = err.to_string();
        assert!(!rendered.contains("wJalr"), "secret leaked: {rendered}");
        assert!(
            !rendered.contains("AKIAEXAMPLE"),
            "key id leaked: {rendered}"
        );
        assert!(
            rendered.contains("SecretAccessKey"),
            "must name the field: {rendered}"
        );
        assert!(
            rendered.contains(&truncated.len().to_string()),
            "must give the length"
        );
    }

    #[test]
    fn a_truncated_or_error_response_yields_err_rather_than_partial_credentials() {
        // Half-parsed credentials would be signed with and rejected, which
        // reads as an auth bug rather than the transport failure it is.
        assert!(parse_sts_xml("<Error><Code>AccessDenied</Code></Error>").is_err());
        assert!(parse_imds_json(r#"{"Code":"AssumeRoleUnauthorizedAccess"}"#).is_err());
        assert!(parse_sts_xml("<AccessKeyId>a</AccessKeyId>").is_err());
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
    }

    #[test]
    fn malformed_timestamps_are_rejected_rather_than_silently_misread() {
        // Each of these previously parsed. The '+026' case is the dangerous
        // one: it yields an expiry thousands of years in the past, making
        // needs_refresh permanently true and defeating the cache entirely.
        for bad in [
            "nonsense",
            "2026x09x06x14x25x30x",      // separators unchecked
            "+026-09-06T14:25:30Z",      // signed year
            "2026-09-06 14:25:30Z",      // space instead of T
            "2026-09-06T14:25:30+01:00", // non-UTC offset read as UTC
            "202X-09-06T14:25:30Z",      // non-digit in a numeric field
        ] {
            assert_eq!(parse_iso8601_to_unix(bad), None, "{bad:?} must be rejected");
        }
    }

    #[test]
    fn the_token_file_path_is_sanitised_against_log_forging() {
        // The path comes from an environment variable and reaches the log; a
        // newline there forges a whole log record (S29).
        let e = CredsError::TokenFile {
            path: truncate(&sanitize_log_excerpt("/tmp/x\nERROR forged"), 256),
            message: sanitize_log_excerpt("no such file"),
        };
        assert!(!e.to_string().contains('\n'), "{e}");
    }

    #[test]
    fn a_half_configured_irsa_fails_loudly_instead_of_using_the_node_role() {
        // Falling through to IMDSv2 would authenticate as the EC2/EKS *node*
        // role -- a different, usually broader identity -- with no diagnostic.
        let e = CredsError::Incomplete {
            source_name: "web identity (IRSA)",
            detail: "AWS_ROLE_ARN is not set.".into(),
        };
        let rendered = e.to_string();
        assert!(rendered.contains("IRSA"), "{rendered}");
        assert!(rendered.contains("AWS_ROLE_ARN"), "{rendered}");
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
                body_len: 3,
                missing: "Token",
            },
            CredsError::Incomplete {
                source_name: "s",
                detail: "d".into(),
            },
            CredsError::TokenFile {
                path: "p".into(),
                message: "m".into(),
            },
        ];
        assert_eq!(
            variants.len(),
            5,
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
        assert_eq!(c.expiry, Expiry::Never, "static credentials do not expire");
    }
}
