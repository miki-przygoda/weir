# Vendored AWS SigV4 test vectors

**Source:** <https://github.com/awslabs/aws-c-auth/tree/main/tests/aws-signing-test-suite/v4>
**Retrieved:** 2026-09-06
**Licence:** Apache-2.0 (same as this repository) — see the upstream `LICENSE`.

38 cases. Each directory holds the four files `tests/sigv4_vectors.rs` reads:

| File | What it is |
|---|---|
| `request.txt` | The raw HTTP request to sign |
| `context.json` | Credentials, region, service, timestamp, and the `normalize` / `sign_body` / `omit_session_token` flags |
| `header-canonical-request.txt` | Expected canonical request |
| `header-string-to-sign.txt` | Expected string-to-sign |
| `header-signature.txt` | Expected signature (hex only — **not** the full `Authorization` header) |

The upstream `query-*.txt` files are not vendored: they cover query-string
signing, which this sink never performs (it only PUTs and HEADs objects).

## Why seven cases are excluded

`tests/sigv4_vectors.rs` skips the seven `*-normalized` cases. Each requires
RFC 3986 path normalization, which an **S3** signer must not do: S3 object keys
may legitimately contain `.`, `..` and `//` segments, so collapsing them would
sign a different key than the one being written.

The exclusion is by rule, not by convenience — the suite ships an
`*-unnormalized` twin for every excluded case, and those twins **are** asserted.
`get-space-unnormalized` in particular is what proves `uri_encode_path` exists
and works: it expects `/example space/` to sign as `/example%20space/`.

31 of 38 cases therefore run, and all 31 must pass.

## Do not hand-edit

These files are an independent oracle. Their value is that nothing in this
repository computed them. If a vector fails, fix the signer — never the vector.
To refresh, re-download from the URL above and re-run the suite.
