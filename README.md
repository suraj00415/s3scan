# s3scan

S3 Bucket Existence & Listing Scanner — probes buckets for existence and directory listing (anonymous + authenticated).

Designed to handle millions of buckets with rate limiting and exponential backoff to respect AWS rate limits.

## Installation

```bash
npm install
```

**Docker:**
```bash
docker build -t surajshah/securescope:s3scan-v1.0 .
```

## Usage

```bash
# Basic scan from file
s3scan -i buckets.txt -o results.json

# Pipe from s3gen
s3gen -k tesla -l 2 | s3scan --stdin -o tesla-scan.json

# High throughput for large lists (10M+)
s3scan -i 10m-buckets.txt -c 80 -r 150 --retries 3 -v -o results.json

# Only check anonymous listing on known buckets
s3scan -i known-buckets.txt --skip-auth --listable-only -o public.json

# Full scan with verbose progress
s3scan -i targets.txt -v -o full-results.json

# JSONL output for streaming
s3scan -i buckets.txt --jsonl -o results.jsonl
```

## Scan Phases

### Phase 1 — Existence Check
HTTP GET to `https://<bucket>.s3.amazonaws.com` (or path-style for dotted bucket names).

| HTTP Code | Status |
|-----------|--------|
| 200 | Exists (public) |
| 403 | Exists (private) |
| 404 | Does not exist |
| 301 | Exists (redirect/wrong region) |

### Phase 2 — Anonymous Listing
Attempts `ListObjectsV2` via raw HTTP without credentials. If the bucket returns object keys, they are captured in the output.

### Phase 3 — Authenticated Listing
Uses AWS SDK with provided credentials to test if **any authenticated AWS user** can list the bucket contents. This catches overly permissive bucket policies that grant access to `AuthenticatedUsers`.

## Authentication

Credentials are resolved in this order:
1. CLI flags (`--access-key`, `--secret-key`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. AWS profile (`--profile` or `AWS_PROFILE`)
4. `~/.aws/credentials` default profile
5. IAM instance role / ECS task role

```bash
# Pass credentials directly
s3scan -i buckets.txt --access-key AKIAXXXXXXXX --secret-key wJalrXXXXXXXX -o results.json

# Use a specific AWS profile
s3scan -i buckets.txt --profile pentester -o results.json

# Use session token (temporary creds from STS)
s3scan -i buckets.txt --access-key ASIA... --secret-key ... --session-token ... -o results.json

# Skip authenticated checks entirely
s3scan -i buckets.txt --skip-auth -o results.json
```

## Rate Limiting & Performance

| Flag | Default | Description |
|------|---------|-------------|
| `-c, --concurrency` | 50 | Max concurrent requests |
| `-r, --rate-limit` | 100 | Max requests per second |
| `-t, --timeout` | 10000 | Per-request timeout (ms) |
| `--retries` | 2 | Retries on timeout/throttle |

**How it handles AWS rate limits:**
- Token bucket algorithm enforces the RPS ceiling
- Exponential backoff on HTTP 429 (Too Many Requests) and 503
- Automatic retry with increasing delay on timeouts
- Concurrency cap limits parallel connections

**Recommended settings for 10M+ buckets:**
```bash
s3scan -i massive-list.txt -c 80 -r 150 --retries 3 -v -o results.json
```

## Output Format

Only existing buckets appear in the output (404s and errors are excluded).

```json
{
  "scan_metadata": {
    "timestamp": "2026-08-17T12:00:00.000Z",
    "concurrency": 50,
    "rate_limit_rps": 100,
    "auth_enabled": true,
    "anon_enabled": true
  },
  "summary": {
    "total_scanned": 10000,
    "existing": 150,
    "not_found": 9840,
    "errors": 10,
    "public_listing": 5,
    "auth_listing": 12
  },
  "buckets": [
    {
      "name": "acme-backup",
      "status": "exists-private",
      "httpCode": 403,
      "anonymousListing": { "listable": false, "statusCode": 403 },
      "authenticatedListing": { "listable": true, "objects": ["db-dump.sql", "keys/"], "objectCount": 2 }
    },
    {
      "name": "acme-public-assets",
      "status": "exists-public",
      "httpCode": 200,
      "anonymousListing": { "listable": true, "objects": ["logo.png", "style.css"], "objectCount": 2, "statusCode": 200 },
      "authenticatedListing": { "listable": true, "objects": ["logo.png", "style.css"], "objectCount": 2 }
    }
  ]
}
```

## Dotted Bucket Names

Buckets containing dots (e.g., `company.prod.backup`) can't use virtual-hosted-style HTTPS because the wildcard cert `*.s3.amazonaws.com` only covers one subdomain level. s3scan automatically detects these and falls back to path-style URLs (`https://s3.amazonaws.com/bucket-name`).

## Pipeline with s3gen

```bash
# Generate permutations and scan in one shot
s3gen -k tesla -l 2 | s3scan --stdin -v -o tesla-results.json

# Multi-keyword scan
s3gen -k "acme,acmecorp" -l 2 --extra-service "graphql,kafka" | s3scan --stdin --profile myprofile -o results.json
```

## All Options

```
INPUT:
  -i, --input <file>      File containing bucket names (one per line)
  --stdin                  Read bucket names from stdin

OUTPUT:
  -o, --output <file>     Output JSON file (default: stdout)
  --jsonl                  Output as JSON Lines (one per line)
  --listable-only         Only include buckets with directory listing enabled

PERFORMANCE:
  -c, --concurrency <n>   Max concurrent requests [default: 50]
  -r, --rate-limit <n>    Max requests per second [default: 100]
  -t, --timeout <ms>      Per-request timeout in ms [default: 10000]
  --retries <n>           Retries on timeout/throttle [default: 2]

AUTHENTICATION:
  --access-key <key>      AWS Access Key ID
  --secret-key <key>      AWS Secret Access Key
  --session-token <tok>   AWS Session Token (temporary creds)
  --profile <name>        AWS profile from ~/.aws/credentials
  --region <region>       AWS region [default: us-east-1]
  --skip-auth             Skip authenticated listing check
  --skip-anon             Skip anonymous listing check

OTHER:
  --max-keys <n>          Max objects to retrieve in listing [default: 20]
  -v, --verbose           Print progress to stderr
  -h, --help              Show help
```
