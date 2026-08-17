#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const https = require("https");
const http = require("http");
const { S3Client, ListObjectsV2Command } = require("@aws-sdk/client-s3");

// ─── Configuration ──────────────────────────────────────────────────────────

const DEFAULT_CONCURRENCY = 50;
const DEFAULT_RATE_LIMIT = 100; // requests per second
const DEFAULT_TIMEOUT = 10000; // 10s per request
const DEFAULT_RETRIES = 2;
const RETRY_BACKOFF_BASE = 1000; // 1s base for exponential backoff
const RATE_LIMIT_BACKOFF = 5000; // 5s pause on 429/503 throttle

// ─── Rate Limiter (Sliding Window with Queue) ──────────────────────────────

class RateLimiter {
  constructor(rps) {
    this.rps = rps;
    this.interval = 1000 / rps;
    this.lastDispatch = 0;
    this.queue = [];
    this.processing = false;
  }

  acquire() {
    return new Promise((resolve) => {
      this.queue.push(resolve);
      this.process();
    });
  }

  process() {
    if (this.processing) return;
    this.processing = true;

    const tick = () => {
      if (this.queue.length === 0) {
        this.processing = false;
        return;
      }

      const now = Date.now();
      const elapsed = now - this.lastDispatch;

      if (elapsed >= this.interval) {
        this.lastDispatch = now;
        const resolve = this.queue.shift();
        resolve();
        setImmediate(tick);
      } else {
        const waitMs = this.interval - elapsed;
        setTimeout(tick, waitMs);
      }
    };

    tick();
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── HTTP Probe (Anonymous) ─────────────────────────────────────────────────

const TLS_OPTIONS = process.env.NODE_TLS_REJECT_UNAUTHORIZED === "0"
  ? { rejectUnauthorized: false }
  : {};

// Buckets with dots can't use virtual-hosted-style HTTPS because the wildcard
// cert *.s3.amazonaws.com only covers one subdomain level. Fall back to path-style.
function hasDots(name) {
  return name.includes(".");
}

function getBucketUrl(bucketName) {
  if (hasDots(bucketName)) {
    return `https://s3.amazonaws.com/${bucketName}`;
  }
  return `https://${bucketName}.s3.amazonaws.com`;
}

function probeBucketHttp(bucketName, timeout) {
  return new Promise((resolve) => {
    const url = getBucketUrl(bucketName);
    const req = https.get(url, { timeout, ...TLS_OPTIONS }, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        resolve({
          bucket: bucketName,
          statusCode: res.statusCode,
          headers: res.headers,
          body: body.slice(0, 2000),
        });
      });
    });

    req.on("timeout", () => {
      req.destroy();
      resolve({ bucket: bucketName, statusCode: 0, error: "timeout" });
    });

    req.on("error", (err) => {
      resolve({ bucket: bucketName, statusCode: 0, error: err.message });
    });
  });
}

// ─── Anonymous Listing Probe ────────────────────────────────────────────────

function probeListAnonymous(bucketName, timeout, redirectRegion) {
  return new Promise((resolve) => {
    let url;
    if (redirectRegion) {
      if (hasDots(bucketName)) {
        url = `https://s3.${redirectRegion}.amazonaws.com/${bucketName}?list-type=2&max-keys=20`;
      } else {
        url = `https://${bucketName}.s3.${redirectRegion}.amazonaws.com/?list-type=2&max-keys=20`;
      }
    } else if (hasDots(bucketName)) {
      url = `https://s3.amazonaws.com/${bucketName}?list-type=2&max-keys=20`;
    } else {
      url = `https://${bucketName}.s3.amazonaws.com/?list-type=2&max-keys=20`;
    }
    const req = https.get(url, { timeout, ...TLS_OPTIONS }, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        const listable = res.statusCode === 200 && body.includes("<ListBucketResult");
        const objects = [];
        if (listable) {
          const keyRegex = /<Key>([^<]+)<\/Key>/g;
          let match;
          while ((match = keyRegex.exec(body)) !== null) {
            objects.push(match[1]);
          }
        }
        resolve({
          bucket: bucketName,
          statusCode: res.statusCode,
          listable,
          objects,
        });
      });
    });

    req.on("timeout", () => {
      req.destroy();
      resolve({ bucket: bucketName, statusCode: 0, listable: false, objects: [], error: "timeout" });
    });

    req.on("error", (err) => {
      resolve({ bucket: bucketName, statusCode: 0, listable: false, objects: [], error: err.message });
    });
  });
}

// ─── Authenticated Listing Probe (AWS SDK) ──────────────────────────────────

async function probeListAuthenticated(s3Client, bucketName, redirectRegion) {
  if (!s3Client) return { bucket: bucketName, listable: false, objects: [], error: "no-credentials" };

  const clientToUse = redirectRegion
    ? new S3Client({ region: redirectRegion, credentials: s3Client.config.credentials })
    : s3Client;

  try {
    const cmd = new ListObjectsV2Command({
      Bucket: bucketName,
      MaxKeys: 20,
    });
    const response = await clientToUse.send(cmd);
    const objects = (response.Contents || []).map((obj) => obj.Key);
    return {
      bucket: bucketName,
      listable: true,
      objectCount: response.KeyCount || objects.length,
      objects,
    };
  } catch (err) {
    const code = err.name || err.Code || "";
    if (code === "PermanentRedirect" && !redirectRegion && err.$response) {
      const region = err.$response.headers && err.$response.headers["x-amz-bucket-region"];
      if (region) {
        return probeListAuthenticated(s3Client, bucketName, region);
      }
    }
    return {
      bucket: bucketName,
      listable: false,
      objects: [],
      error: code,
      message: err.message,
    };
  }
}

// ─── Classify Bucket Status ─────────────────────────────────────────────────

function classifyBucket(statusCode) {
  switch (statusCode) {
    case 200:
      return "exists-public";
    case 403:
      return "exists-private";
    case 404:
      return "not-found";
    case 301:
      return "exists-redirect";
    case 0:
      return "error";
    default:
      return `exists-${statusCode}`;
  }
}

// ─── Worker Pool ────────────────────────────────────────────────────────────

async function processWithPool(items, concurrency, rateLimiter, workerFn) {
  const results = [];
  let index = 0;

  async function worker() {
    while (index < items.length) {
      const i = index++;
      await rateLimiter.acquire();
      const result = await workerFn(items[i], i);
      results.push(result);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(concurrency, items.length); i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
}

// ─── Retry Wrapper ──────────────────────────────────────────────────────────

async function withRetry(fn, retries, rateLimiter) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const result = await fn();

    if (result.statusCode === 429 || result.statusCode === 503) {
      if (attempt < retries) {
        const backoff = RATE_LIMIT_BACKOFF * Math.pow(2, attempt);
        process.stderr.write(`[!] Rate limited on ${result.bucket}, backing off ${backoff}ms\n`);
        await sleep(backoff);
        continue;
      }
    }

    if (result.error === "timeout" && attempt < retries) {
      const backoff = RETRY_BACKOFF_BASE * Math.pow(2, attempt);
      await sleep(backoff);
      continue;
    }

    return result;
  }
}

// ─── CLI Argument Parsing ───────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    input: null,
    stdin: false,
    output: null,
    concurrency: DEFAULT_CONCURRENCY,
    rateLimit: DEFAULT_RATE_LIMIT,
    timeout: DEFAULT_TIMEOUT,
    retries: DEFAULT_RETRIES,
    authRegion: "us-east-1",
    accessKey: null,
    secretKey: null,
    sessionToken: null,
    profile: null,
    skipAuth: false,
    skipAnon: false,
    listableOnly: false,
    jsonl: false,
    verbose: false,
    maxKeys: 20,
    help: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "-i":
      case "--input":
        opts.input = args[++i];
        break;
      case "--stdin":
        opts.stdin = true;
        break;
      case "-o":
      case "--output":
        opts.output = args[++i];
        break;
      case "-c":
      case "--concurrency":
        opts.concurrency = parseInt(args[++i], 10);
        break;
      case "-r":
      case "--rate-limit":
        opts.rateLimit = parseInt(args[++i], 10);
        break;
      case "-t":
      case "--timeout":
        opts.timeout = parseInt(args[++i], 10);
        break;
      case "--retries":
        opts.retries = parseInt(args[++i], 10);
        break;
      case "--region":
        opts.authRegion = args[++i];
        break;
      case "--access-key":
        opts.accessKey = args[++i];
        break;
      case "--secret-key":
        opts.secretKey = args[++i];
        break;
      case "--session-token":
        opts.sessionToken = args[++i];
        break;
      case "--profile":
        opts.profile = args[++i];
        break;
      case "--skip-auth":
        opts.skipAuth = true;
        break;
      case "--skip-anon":
        opts.skipAnon = true;
        break;
      case "--listable-only":
        opts.listableOnly = true;
        break;
      case "--jsonl":
        opts.jsonl = true;
        break;
      case "--max-keys":
        opts.maxKeys = parseInt(args[++i], 10);
        break;
      case "-v":
      case "--verbose":
        opts.verbose = true;
        break;
      case "-h":
      case "--help":
        opts.help = true;
        break;
      default:
        if (!arg.startsWith("-") && !opts.input) {
          opts.input = arg;
        } else {
          process.stderr.write(`[!] Unknown option: ${arg}\n`);
          process.exit(1);
        }
    }
  }
  return opts;
}

function printHelp() {
  const help = `
s3scan - S3 Bucket Existence & Listing Scanner

Probes a list of S3 bucket names to determine:
  1. Which buckets exist (HTTP 200/403 = exists, 404 = not found)
  2. Which buckets allow anonymous directory listing
  3. Which buckets allow authenticated (any-AWS-user) directory listing

Designed to handle millions of buckets with rate limiting and backoff to
respect AWS rate limits.

USAGE:
  s3scan -i <buckets-file> [options]
  s3scan <buckets-file> [options]
  cat buckets.txt | s3scan --stdin [options]
  s3gen -k acme | s3scan --stdin -o results.json

INPUT:
  -i, --input <file>      File containing bucket names (one per line)
  --stdin                  Read bucket names from stdin

OUTPUT:
  -o, --output <file>     Output JSON file (default: stdout)
  --jsonl                  Output as JSON Lines (one JSON object per line)
  --listable-only         Only include buckets with directory listing enabled

PERFORMANCE & RATE LIMITING:
  -c, --concurrency <n>   Max concurrent requests [default: 50]
  -r, --rate-limit <n>    Max requests per second [default: 100]
  -t, --timeout <ms>      Per-request timeout in ms [default: 10000]
  --retries <n>           Number of retries on timeout/throttle [default: 2]

AUTHENTICATION:
  --access-key <key>      AWS Access Key ID (overrides env/profile)
  --secret-key <key>      AWS Secret Access Key (overrides env/profile)
  --session-token <tok>   AWS Session Token (optional, for temporary creds)
  --profile <name>        AWS profile from ~/.aws/credentials
  --region <region>       AWS region for authenticated checks [default: us-east-1]
  --skip-auth             Skip authenticated listing check
  --skip-anon             Skip anonymous listing check (only check existence)

SCAN MODES:
  --max-keys <n>          Max objects to retrieve in listing [default: 20]
  -v, --verbose           Print progress to stderr

PHASES:
  Phase 1 - Existence Check:
    HEAD request to https://<bucket>.s3.amazonaws.com
    200 = exists (public), 403 = exists (private), 404 = doesn't exist

  Phase 2 - Anonymous Listing:
    GET https://<bucket>.s3.amazonaws.com/?list-type=2
    Checks if bucket contents are publicly listable without credentials

  Phase 3 - Authenticated Listing:
    Uses AWS SDK with credentials from environment/profile
    Checks if any authenticated AWS user can list the bucket
    (Requires AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY or AWS profile)

OUTPUT FORMAT (JSON):
  {
    "summary": {
      "total_scanned": 1000,
      "existing": 150,
      "not_found": 850,
      "public_listing": 5,
      "auth_listing": 12
    },
    "buckets": [
      {
        "name": "acme-backup",
        "status": "exists-private",
        "httpCode": 403,
        "anonymousListing": { "listable": false },
        "authenticatedListing": { "listable": true, "objects": ["file1.txt", ...] }
      }
    ]
  }

EXAMPLES:
  # Basic scan from file
  s3scan -i buckets.txt -o results.json

  # Pipe from s3gen
  s3gen -k tesla -l 2 | s3scan --stdin -o tesla-scan.json

  # High throughput for large lists
  s3scan -i 10m-buckets.txt -c 100 -r 200 -o results.json

  # Quick existence scan (only existing buckets appear in output)
  s3scan -i buckets.txt --skip-auth --skip-anon -o exists.json

  # Only check anonymous listing on known buckets
  s3scan -i known-buckets.txt --skip-auth --listable-only -o public.json

  # Full scan with verbose progress
  s3scan -i targets.txt -v -o full-results.json

  # JSONL output for streaming processing
  s3scan -i buckets.txt --jsonl -o results.jsonl

  # Pass AWS credentials directly via CLI
  s3scan -i buckets.txt --access-key AKIAIOSFODNN7EXAMPLE --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY -o results.json

  # Use a specific AWS profile
  s3scan -i buckets.txt --profile pentester -o results.json

RATE LIMIT HANDLING:
  - Token bucket algorithm enforces requests-per-second ceiling
  - Exponential backoff on HTTP 429 (Too Many Requests) and 503
  - Automatic retry on timeouts with increasing delay
  - Configurable concurrency cap limits parallel connections
  - For 10M+ buckets, recommended: -c 80 -r 150 --retries 3

NOTES:
  - AWS credentials are resolved in this order:
    1. CLI flags (--access-key, --secret-key)
    2. Environment vars (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    3. AWS profile (--profile or AWS_PROFILE env var)
    4. ~/.aws/credentials default profile
    5. IAM instance role / ECS task role
  - If no credentials found, authenticated check is skipped gracefully
  - Bucket names are deduplicated and trimmed before scanning
`;
  process.stdout.write(help);
}

// ─── Progress Reporter ──────────────────────────────────────────────────────

class ProgressReporter {
  constructor(total, verbose) {
    this.total = total;
    this.processed = 0;
    this.existing = 0;
    this.publicList = 0;
    this.authList = 0;
    this.errors = 0;
    this.verbose = verbose;
    this.startTime = Date.now();
    this.lastReport = 0;
  }

  update(result) {
    this.processed++;
    if (result.status !== "not-found" && result.status !== "error") {
      this.existing++;
    }
    if (result.anonymousListing && result.anonymousListing.listable) {
      this.publicList++;
    }
    if (result.authenticatedListing && result.authenticatedListing.listable) {
      this.authList++;
    }
    if (result.status === "error") {
      this.errors++;
    }

    if (this.verbose) {
      const now = Date.now();
      if (now - this.lastReport >= 2000 || this.processed === this.total) {
        this.report();
        this.lastReport = now;
      }
    }
  }

  report() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const rate = Math.round(this.processed / elapsed);
    const pct = ((this.processed / this.total) * 100).toFixed(1);
    const eta = Math.round((this.total - this.processed) / Math.max(rate, 1));
    process.stderr.write(
      `\r[*] ${this.processed}/${this.total} (${pct}%) | ` +
        `${rate}/s | ETA: ${eta}s | ` +
        `exists: ${this.existing} | public-list: ${this.publicList} | ` +
        `auth-list: ${this.authList} | errors: ${this.errors}   `
    );
  }
}

// ─── Main Scanner ───────────────────────────────────────────────────────────

async function scan(buckets, opts) {
  const rateLimiter = new RateLimiter(opts.rateLimit);
  const progress = new ProgressReporter(buckets.length, opts.verbose);

  let s3Client = null;
  if (!opts.skipAuth) {
    const clientConfig = { region: opts.authRegion };

    if (opts.accessKey && opts.secretKey) {
      const creds = {
        accessKeyId: opts.accessKey,
        secretAccessKey: opts.secretKey,
      };
      if (opts.sessionToken) {
        creds.sessionToken = opts.sessionToken;
      }
      clientConfig.credentials = creds;
    } else if (opts.profile) {
      clientConfig.credentials = { profile: opts.profile };
      process.env.AWS_PROFILE = opts.profile;
    }

    try {
      s3Client = new S3Client(clientConfig);
    } catch (e) {
      process.stderr.write(`[!] Failed to initialize AWS client: ${e.message}\n`);
      process.stderr.write(`[!] Skipping authenticated checks\n`);
    }
  }

  const results = [];
  let index = 0;

  async function worker() {
    while (index < buckets.length) {
      const i = index++;
      const bucketName = buckets[i];

      await rateLimiter.acquire();

      // Phase 1: Existence check
      const existence = await withRetry(
        () => probeBucketHttp(bucketName, opts.timeout),
        opts.retries,
        rateLimiter
      );

      const status = classifyBucket(existence.statusCode);
      const result = {
        name: bucketName,
        status,
        httpCode: existence.statusCode,
      };

      // Skip non-existent buckets for further checks
      if (status === "not-found" || status === "error") {
        if (existence.error) result.error = existence.error;
        progress.update(result);
        results.push(result);
        continue;
      }

      // For 301 redirects, extract the correct region from response
      let redirectRegion = null;
      if (existence.statusCode === 301) {
        // Try x-amz-bucket-region header first
        if (existence.headers && existence.headers["x-amz-bucket-region"]) {
          redirectRegion = existence.headers["x-amz-bucket-region"];
        }
        // Try extracting region from Endpoint in body
        if (!redirectRegion && existence.body) {
          const endpointMatch = existence.body.match(/<Endpoint>([^<]+)<\/Endpoint>/);
          if (endpointMatch) {
            const regionMatch = endpointMatch[1].match(/s3[.-]([a-z0-9-]+)\.amazonaws\.com/);
            if (regionMatch) redirectRegion = regionMatch[1];
          }
        }
        // Try Location header
        if (!redirectRegion && existence.headers && existence.headers.location) {
          const locMatch = existence.headers.location.match(/s3[.-]([a-z0-9-]+)\.amazonaws\.com/);
          if (locMatch) redirectRegion = locMatch[1];
        }
      }

      // Phase 2: Anonymous listing
      if (!opts.skipAnon) {
        await rateLimiter.acquire();
        const anonResult = await withRetry(
          () => probeListAnonymous(bucketName, opts.timeout, redirectRegion),
          opts.retries,
          rateLimiter
        );
        result.anonymousListing = {
          listable: anonResult.listable,
          statusCode: anonResult.statusCode,
        };
        if (anonResult.listable) {
          result.anonymousListing.objects = anonResult.objects;
          result.anonymousListing.objectCount = anonResult.objects.length;
        }
      }

      // Phase 3: Authenticated listing
      if (!opts.skipAuth && s3Client) {
        await rateLimiter.acquire();
        const authResult = await probeListAuthenticated(s3Client, bucketName, redirectRegion);
        result.authenticatedListing = {
          listable: authResult.listable,
        };
        if (authResult.listable) {
          result.authenticatedListing.objects = authResult.objects;
          result.authenticatedListing.objectCount = authResult.objectCount;
        }
        if (authResult.error) {
          result.authenticatedListing.error = authResult.error;
        }
      }

      progress.update(result);
      results.push(result);
    }
  }

  const workers = [];
  const concurrency = Math.min(opts.concurrency, buckets.length);
  for (let i = 0; i < concurrency; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  if (opts.verbose) {
    process.stderr.write("\n");
  }

  return results;
}

// ─── Output Formatting ──────────────────────────────────────────────────────

function formatOutput(results, opts) {
  let filtered = results.filter(
    (r) => r.status !== "not-found" && r.status !== "error"
  );

  if (opts.listableOnly) {
    filtered = filtered.filter(
      (r) =>
        (r.anonymousListing && r.anonymousListing.listable) ||
        (r.authenticatedListing && r.authenticatedListing.listable)
    );
  }

  if (opts.jsonl) {
    return filtered.map((r) => JSON.stringify(r)).join("\n") + "\n";
  }

  const summary = {
    total_scanned: results.length,
    existing: results.filter(
      (r) => r.status !== "not-found" && r.status !== "error"
    ).length,
    not_found: results.filter((r) => r.status === "not-found").length,
    errors: results.filter((r) => r.status === "error").length,
    public_listing: results.filter(
      (r) => r.anonymousListing && r.anonymousListing.listable
    ).length,
    auth_listing: results.filter(
      (r) => r.authenticatedListing && r.authenticatedListing.listable
    ).length,
  };

  const output = {
    scan_metadata: {
      timestamp: new Date().toISOString(),
      concurrency: opts.concurrency,
      rate_limit_rps: opts.rateLimit,
      auth_enabled: !opts.skipAuth,
      anon_enabled: !opts.skipAnon,
    },
    summary,
    buckets: filtered,
  };

  return JSON.stringify(output, null, 2) + "\n";
}

// ─── Main ───────────────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs();

  if (opts.help) {
    printHelp();
    process.exit(0);
  }

  let bucketNames = [];

  if (opts.input) {
    try {
      const content = fs.readFileSync(opts.input, "utf-8");
      bucketNames = content
        .split("\n")
        .map((l) => l.trim().toLowerCase())
        .filter((l) => l && !l.startsWith("#"));
    } catch (err) {
      process.stderr.write(`[!] Error reading file: ${opts.input}: ${err.message}\n`);
      process.exit(1);
    }
  }

  if (opts.stdin) {
    const rl = readline.createInterface({ input: process.stdin });
    for await (const line of rl) {
      const trimmed = line.trim().toLowerCase();
      if (trimmed && !trimmed.startsWith("#")) {
        bucketNames.push(trimmed);
      }
    }
  }

  if (bucketNames.length === 0) {
    process.stderr.write("[!] No bucket names provided. Use -i <file> or --stdin\n\n");
    printHelp();
    process.exit(1);
  }

  // Deduplicate
  bucketNames = [...new Set(bucketNames)];

  process.stderr.write(`[*] s3scan - S3 Bucket Scanner\n`);
  process.stderr.write(`[*] Loaded ${bucketNames.length} unique bucket names\n`);
  process.stderr.write(`[*] Concurrency: ${opts.concurrency} | Rate limit: ${opts.rateLimit} req/s\n`);
  process.stderr.write(`[*] Auth check: ${opts.skipAuth ? "disabled" : "enabled"} | Anon listing: ${opts.skipAnon ? "disabled" : "enabled"}\n`);
  process.stderr.write(`[*] Starting scan...\n`);

  const results = await scan(bucketNames, opts);

  const output = formatOutput(results, opts);

  if (opts.output) {
    fs.writeFileSync(opts.output, output);
    process.stderr.write(`[*] Results written to ${opts.output}\n`);
  } else {
    process.stdout.write(output);
  }

  // Print summary to stderr
  const existing = results.filter(
    (r) => r.status !== "not-found" && r.status !== "error"
  ).length;
  const publicList = results.filter(
    (r) => r.anonymousListing && r.anonymousListing.listable
  ).length;
  const authList = results.filter(
    (r) => r.authenticatedListing && r.authenticatedListing.listable
  ).length;

  process.stderr.write(`\n[*] Scan complete!\n`);
  process.stderr.write(`[*] Summary: ${existing} existing / ${publicList} public-listable / ${authList} auth-listable / ${bucketNames.length} total\n`);
}

main().catch((err) => {
  process.stderr.write(`[!] Fatal: ${err.message}\n`);
  process.exit(1);
});
