# EC2 + S3: Automated File Processing — Practice Material

Build a private file-processing pipeline, then improve its reliability through experiments. This is a guided lab, not an assessment. There are no marks or submission requirements.

Suggested pace: two sessions of 3–4 hours, followed by optional extensions. Prerequisites: Linux commands, basic Python, IAM roles, EC2 security groups, and S3 objects.

## What you will build

1. A client uploads a CSV, TXT, or PDF to a private S3 bucket.
2. S3 sends an object-created event to SQS.
3. A Python worker on EC2 reads the message and downloads the specific object version.
4. The worker generates a JSON report and uploads it to S3.
5. Later, an API issues temporary upload/download access and returns job status.

Core services: EC2, S3, IAM, SQS. Add DynamoDB for persistent job state and conditional claims, and CloudWatch for monitoring.

Start with one worker. Add the API and a second worker only after the basic flow works.

## 1. Concepts to understand first

| Concept | Meaning in this project |
|---|---|
| S3 prefix | Part of an object key; it is not a real directory |
| IAM role | Temporary AWS credentials supplied to EC2 applications |
| Queue | Holds work while a worker is busy or unavailable |
| Visibility timeout | Temporarily hides a received message; it does not delete it |
| Receipt handle | Token for a particular message delivery; use the latest handle when deleting |
| Dead-letter queue | Holds messages that exceed the configured receive attempts |
| Idempotency | Repeating a job leaves one consistent externally visible result |
| Lease | A temporary claim that another worker can reclaim after expiry |
| Presigned access | Time-limited access authorized by the signing IAM principal |

Both event delivery and queue delivery can produce duplicates. Design for repeated execution, rather than assuming that a notification happens exactly once. A visibility timeout alone is not a distributed lock.

## 2. Prepare the resources

Use one AWS Region throughout; `ap-south-1` is an example. Choose your own globally unique bucket name. Resource names below are suggestions.

| Resource | Suggested configuration |
|---|---|
| S3 bucket | Private, versioning enabled, default SSE-S3 encryption |
| Prefixes | `incoming/`, `processed/`, `logs/` |
| Main SQS queue | Standard queue, long polling 20 seconds, visibility timeout 120 seconds, retention 4 days |
| Dead-letter queue | Standard queue, retention 14 days |
| Redrive policy | Main queue sends to DLQ after `maxReceiveCount=5` |
| Worker EC2 | Ubuntu 24.04 LTS; choose instance capacity for the workload |
| EC2 metadata | Require IMDSv2 |
| Job table, later | DynamoDB on-demand table with string partition key `job_id` |

For this lab, use SSE-S3 and SQS-managed encryption to avoid introducing customer-managed KMS key policies. KMS encryption is a useful later exercise.

### Network setup

For the simplest lab, place EC2 in a public subnet with an internet gateway, a public IPv4 address, DNS resolution, and outbound HTTPS access. Permit SSH only from your IP, or use a properly configured Session Manager connection. The worker needs no application inbound port. A public IPv4 address and other services may incur charges.

A private-subnet variation needs a NAT path or appropriate endpoints for every AWS service used. An S3 gateway endpoint does not provide access to SQS, DynamoDB API endpoints outside its configured path, CloudWatch Logs, or package repositories by itself.

### Connect S3 to SQS

First add the following resource policy to the main queue. Replace all uppercase placeholders with your real values; this example assumes the standard AWS commercial partition.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowOnlyLabBucketEvents",
    "Effect": "Allow",
    "Principal": {"Service": "s3.amazonaws.com"},
    "Action": "sqs:SendMessage",
    "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:file-processing-queue",
    "Condition": {
      "ArnEquals": {"aws:SourceArn": "arn:aws:s3:::BUCKET_NAME"},
      "StringEquals": {"aws:SourceAccount": "ACCOUNT_ID"}
    }
  }]
}
```

Then configure an S3 event notification:

- Event: all object-created events (`s3:ObjectCreated:*`).
- Prefix filter: `incoming/`.
- Destination: the main SQS queue.

The bucket and queue must be in the same Region. Direct S3 notifications use a standard SQS queue, not FIFO. Filtering on `incoming/` prevents generated reports from creating a processing loop.

S3 may send an `s3:TestEvent` when the notification is configured. Handle it separately from normal `Records` messages.

## 3. Give EC2 only the access it needs

Create an IAM role trusted by `ec2.amazonaws.com`, then attach it to the worker through an instance profile. Do not run `aws configure` with access keys on EC2.

Worker policy for the core lab:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::BUCKET_NAME/incoming/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": [
        "arn:aws:s3:::BUCKET_NAME/processed/*",
        "arn:aws:s3:::BUCKET_NAME/logs/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage", "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT_ID:file-processing-queue"
    }
  ]
}
```

This worker does not need bucket listing because the event supplies the key. `GetObjectVersion` is needed when reading a particular version. The queue policy authorizes S3 delivery; the worker role authorizes message consumption. These solve different problems.

When adding job tracking, grant `dynamodb:GetItem`, `dynamodb:PutItem`, and `dynamodb:UpdateItem` on the job table ARN. Grant CloudWatch Logs permissions separately when configuring the agent. Keep provisioning and DLQ-redrive permissions on an operator identity rather than the worker.

## 4. Prepare Python on the worker

Run on Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y python3-venv python3-pip
sudo install -d -o ubuntu -g ubuntu /opt/file-worker
cd /opt/file-worker
python3 -m venv .venv
.venv/bin/pip install boto3 pypdf
```

After verifying the dependencies, record the installed versions with `.venv/bin/pip freeze > requirements-lock.txt` for a repeatable lab.

Set these values in the shell where you run the worker:

```bash
export AWS_DEFAULT_REGION=ap-south-1
export BUCKET_NAME=replace-with-your-bucket
export QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/ACCOUNT_ID/file-processing-queue
```

Verify the instance identity without displaying credentials:

```bash
.venv/bin/python -c 'import boto3; print(boto3.client("sts").get_caller_identity()["Arn"])'
```

The ARN should identify the attached role. Check for environment credentials or old credential files if it identifies another principal.

## 5. Build the processor locally first

Save the following as `processor.py`. It is a runnable starter for small trusted lab files. It is not a malware scanner or a production document sandbox.

```python
import csv
import io
import re
from collections import Counter
from pathlib import Path

from pypdf import PdfReader

MAX_BYTES = 20 * 1024 * 1024


def process_file(filename, data):
    if not data or len(data) > MAX_BYTES:
        raise ValueError("File must be between 1 byte and 20 MiB")

    extension = Path(filename).suffix.lower()
    if extension == ".pdf":
        if not data.startswith(b"%PDF-"):
            raise ValueError("File does not have a PDF header")
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted:
            raise ValueError("Encrypted PDFs are not supported in this lab")
        return {
            "type": "pdf",
            "page_count": len(reader.pages),
            "text": "\n".join(page.extract_text() or "" for page in reader.pages),
        }

    if extension not in {".csv", ".txt"}:
        raise ValueError("Only PDF, CSV, and TXT are supported")

    text = data.decode("utf-8-sig")
    if "\x00" in text:
        raise ValueError("Binary content is not supported")

    if extension == ".txt":
        words = re.findall(r"\b\w+\b", text.lower())
        return {
            "type": "txt",
            "line_count": len(text.splitlines()),
            "word_count": len(words),
            "top_words": Counter(words).most_common(10),
        }

    reader = csv.reader(io.StringIO(text), strict=True)
    headers = next(reader, None)
    if not headers or any(not name.strip() for name in headers):
        raise ValueError("CSV requires nonempty column names")
    if len(set(headers)) != len(headers):
        raise ValueError("CSV column names must be unique")
    missing = dict.fromkeys(headers, 0)
    row_count = 0
    for row in reader:
        if len(row) != len(headers):
            raise ValueError("CSV row has a different number of columns")
        row_count += 1
        for name, value in zip(headers, row):
            if not value.strip():
                missing[name] += 1
    return {
        "type": "csv", "row_count": row_count,
        "columns": headers, "missing_values": missing,
    }
```

Create `students.csv`:

```csv
name,course,score
Asha,AWS,85
Ravi,Linux,
Meera,AWS,92
```

Try the processor:

```bash
.venv/bin/python - <<'PY'
import json
from pathlib import Path
from processor import process_file
print(json.dumps(process_file("students.csv", Path("students.csv").read_bytes()), indent=2))
PY
```

Expect three rows and one missing value under `score`. CSV and TXT are both text formats: there is no universal magic header that reliably distinguishes them. This lab defines CSV as UTF-8 text with a valid header and consistent column counts. A scanned PDF may return no text; OCR is a separate extension. Add process-level time and memory limits before handling untrusted PDFs.

## 6. Add the queue worker

Save this as `worker.py` beside `processor.py`. This initial version demonstrates the full storage/queue flow. It deliberately leaves distributed job claims and heartbeats for the next stage; do not mistake it for the completed reliable design.

```python
import hashlib
import json
import logging
import os
import time
from urllib.parse import unquote_plus

import boto3
from processor import MAX_BYTES, process_file

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
queue_url = os.environ["QUEUE_URL"]
expected_bucket = os.environ["BUCKET_NAME"]
logging.basicConfig(level=logging.INFO, format="%(message)s")


def handle_record(record):
    bucket = record["s3"]["bucket"]["name"]
    obj = record["s3"]["object"]
    key = unquote_plus(obj["key"])
    version = obj.get("versionId")
    if bucket != expected_bucket or not key.startswith("incoming/"):
        raise ValueError("Unexpected event source")
    if not version or version == "null":
        raise ValueError("Enable bucket versioning before uploading lab files")
    identity = json.dumps([bucket, key, version], separators=(",", ":"))
    job_id = hashlib.sha256(identity.encode()).hexdigest()
    started = time.monotonic()
    response = s3.get_object(Bucket=bucket, Key=key, VersionId=version)
    body = response["Body"]
    try:
        if response["ContentLength"] > MAX_BYTES:
            raise ValueError("File exceeds 20 MiB")
        data = body.read(MAX_BYTES + 1)
    finally:
        body.close()
    report = process_file(key, data)
    report.update({
        "job_id": job_id, "source_key": key, "source_version": version,
        "status": "COMPLETED",
        "processing_seconds": round(time.monotonic() - started, 3),
    })
    result_key = f"processed/{job_id}/report.json"
    s3.put_object(
        Bucket=bucket, Key=result_key,
        Body=json.dumps(report).encode(), ContentType="application/json",
    )
    logging.info(json.dumps({"job_id": job_id, "status": "COMPLETED",
                             "result_key": result_key}))


def handle_message(message):
    event = json.loads(message["Body"])
    if event.get("Event") == "s3:TestEvent":
        return
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("Unsupported event envelope")
    for record in records:
        handle_record(record)


while True:
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1,
            WaitTimeSeconds=20, VisibilityTimeout=120,
        )
        for message in response.get("Messages", []):
            try:
                handle_message(message)
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                )
            except Exception:
                # Keep the message for retry and eventual DLQ routing.
                logging.exception("Message processing failed")
    except Exception:
        logging.exception("Queue polling failed")
        time.sleep(5)
```

Run `.venv/bin/python worker.py`. Upload `students.csv` to `incoming/customer-a/demo/students.csv` using the S3 console. Find its JSON report under `processed/`.

An S3 object key in a notification is URL encoded; `unquote_plus` correctly handles spaces encoded as `+`. Read the version from the event, rather than fetching whichever version is latest when the worker starts.

Starter limitations: repeated deliveries reprocess files and write additional report versions; a job exceeding 120 seconds can become visible while still running; failed jobs do not yet have persistent status; error logs are not yet structured. These are the next practice steps.

## 7. Add persistent state and safe retries

Create a DynamoDB table with partition key `job_id` (String). Use the hash of bucket, key, and version as the processing identity. A replacement at the same key is a new job because it has a different version.

Suggested fields:

```text
job_id, status, owner_token, lease_until, attempts,
source_bucket, source_key, source_version,
result_key, result_version, updated_at, last_error
```

Work through this algorithm before running two workers:

1. Generate a unique owner token for this processing attempt.
2. Atomically claim a new job or an expired unfinished job using a DynamoDB conditional write. Do not use a read followed by an unconditional write.
3. If the job is already completed, acknowledge the duplicate without reprocessing.
4. If another owner has a valid lease, leave the message unacknowledged and delay its visibility. Do not delete it merely because someone else appears to be processing it.
5. While processing, renew the DynamoDB lease and extend SQS visibility, for example every 30 seconds to a 120-second horizon.
6. If ownership is lost, stop committing results. A heartbeat failure must be handled, not just logged and ignored.
7. Write the result to an attempt-specific key, such as `processed/<job-id>/<owner-token>.json`.
8. Mark the job completed with a conditional update requiring the same owner token and an unexpired lease. Store the winning result key and version ID.
9. Only then delete the queue message.

A conditional claim expression can follow this shape, with aliases and values supplied through Boto3:

```text
attribute_not_exists(job_id)
OR (#status <> :completed AND lease_until < :now)
```

Ensure every unfinished item has a lease field, use synchronized UTC clocks, and increment attempts atomically. Do not use DynamoDB TTL as the lock timer: background TTL deletion is not immediate.

Why attempt-specific result keys? A worker that resumes after losing its lease might still finish an S3 write. Ownership checks cannot make an S3 write and a DynamoDB update one transaction. Separate keys plus a conditional pointer commit keep that stale write from replacing the published result. A retry can leave an orphan result; lifecycle cleanup can remove it later.

For transient failures, record a retryable state, release the lease conditionally, and leave the message for retry. For the DLQ experiment, allow even corrupted input to retry until redrive occurs. In a more mature design, classify permanent validation errors, durably record `FAILED`, and acknowledge those messages without repeatedly parsing them.

Keep retryable `FAILED` jobs claimable when redriving them. A terminal status that always causes messages to be skipped would make redrive ineffective.

## 8. Add temporary uploads and downloads

Use a second EC2 instance for a small Flask or FastAPI service. Give it a separate IAM role: upload access to `incoming/`, download access to `processed/`, and only the job-table operations required by your implementation. It should not consume the worker queue.

Suggested routes:

| Route | Purpose |
|---|---|
| `POST /uploads` | Generate a request ID and presigned upload form |
| `GET /uploads/<request-id>` | Resolve uploaded versions and their processing jobs |
| `GET /jobs/<job-id>` | Read persistent status |
| `GET /jobs/<job-id>/download` | Return a five-minute download URL for the committed result |

Use a presigned POST when you want S3 to enforce a size range:

```python
form = s3.generate_presigned_post(
    Bucket=bucket,
    Key=f"incoming/{customer_id}/{request_id}/{safe_filename}",
    Conditions=[["content-length-range", 1, 20 * 1024 * 1024]],
    ExpiresIn=300,
)
```

The client submits the returned `fields` and the file as multipart form data to `form['url']`. This is a POST form, not a URL for `PUT`. Keep the generated key server-controlled and validate the extension before signing. Content validation still happens on the worker.

For downloads:

```python
url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": committed_result_key,
            "VersionId": committed_result_version},
    ExpiresIn=300,
)
```

If including `VersionId`, grant the API role `s3:GetObjectVersion` on results. Treat signed URLs and upload fields as temporary secrets; do not log them.

The upload request ID exists before S3 creates a version, whereas the processing ID includes the version. Persist the mapping when handling the event. Make this mapping update idempotent, and return `PENDING` until an upload is observed. Do not silently mix these two identifiers.

For a private lab, a per-customer API key can supply the authenticated customer identity. Keep keys outside source code and use TLS or an SSH tunnel for API access. Compare the authenticated customer with the job owner on every status and download request. An unpredictable job ID does not replace authorization.

If uploading directly from browser JavaScript to S3, configure bucket CORS for the specific frontend origin and required method. A Python client does not need browser CORS configuration.

## 9. Run the worker as a service

Create `/etc/file-worker.env` with the three environment values from section 4, without `export`. Do not put AWS access keys there. Keep this file root-owned.

Save `/etc/systemd/system/file-worker.service`:

```ini
[Unit]
Description=S3 file processing worker
Wants=network-online.target
After=network-online.target

[Service]
User=ubuntu
WorkingDirectory=/opt/file-worker
EnvironmentFile=/etc/file-worker.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/file-worker/.venv/bin/python /opt/file-worker/worker.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now file-worker
sudo systemctl status file-worker
sudo journalctl -u file-worker -f
```

For graceful shutdown, handle SIGTERM by stopping new receives, continuing heartbeats while the active job finishes, then exiting. Choose a systemd shutdown timeout consistent with the maximum job duration. Abrupt shutdown must still be recoverable through lease expiry and redelivery.

## 10. Observe the system

Install the CloudWatch agent using the official instructions linked below. Configure your Python application to write structured JSON to a rotating log file, then configure the agent to collect that file. The service above writes to journald by default; a file-collection configuration will not automatically collect it.

Log job ID, attempt token, source version, duration, status, and error type. Never log file contents, credentials, or presigned access.

Useful alarms and observations:

- Main queue `ApproximateAgeOfOldestMessage`: catches work waiting too long.
- Main queue `ApproximateNumberOfMessagesVisible`: shows backlog.
- DLQ `ApproximateNumberOfMessagesVisible >= 1`: signals jobs needing attention.
- Application failure count: emit a custom metric or create a log metric filter.
- EC2 CPU and memory: the CloudWatch agent is needed for guest memory metrics.

For daily archives, first write application logs to a rotating local file. Archive closed rotations to a timestamped file and upload to `logs/` using the instance role. Schedule through a systemd timer or cron, with absolute paths and an explicit Region. Keep archived logs separate from `incoming/`.

Example lab retention: expire incoming objects after 7 days and results/logs after 30 days; also expire noncurrent versions after an appropriate lab interval and abort incomplete multipart uploads. Versioned-object expiration can leave delete markers and old versions, so configure those deliberately rather than assuming one rule removes everything.

## 11. Learn through failure experiments

| Try this | Observe and investigate |
|---|---|
| Stop the worker, then upload three files | Messages remain queued; processing resumes after restart |
| Kill the worker after download | Message returns after visibility expiry; the job lease eventually becomes reclaimable |
| Kill after S3 result write but before status commit | Retry recovers; an uncommitted result may remain |
| Kill after completion commit but before queue deletion | Duplicate sees completed state and is acknowledged |
| Start two workers and duplicate the same event | Conditional claims prevent conflicting published results |
| Make processing take longer than 120 seconds | Heartbeats preserve visibility and ownership |
| Upload a corrupted PDF | Error appears; worker remains available for other files |
| Temporarily deny result uploads | Retry behavior is visible; repeated failure reaches DLQ |
| Restore permission and redrive DLQ messages | Failed work can be reclaimed and completed |
| Upload two versions at the same key | Each event reads its own version and has a separate job ID |
| Use spaces and plus signs in filenames | Event-key decoding retrieves the correct object |
| Request customer A's job with customer B's identity | API denies access |

For deterministic duplicate practice, capture one real S3 event and send its body twice to the main queue using an operator identity. Do not grant `SendMessage` to the worker merely for this experiment. Receiving messages in the console also increments receive counts and can influence DLQ behavior.

## 12. Troubleshooting reference

| Symptom | First things to inspect |
|---|---|
| Upload creates no queue message | Region, queue resource policy, notification prefix, event configuration |
| Destination validation fails | Queue ARN, queue policy, Region, encryption settings |
| Worker has no AWS credentials | Attached instance profile, role trust, IMDS access, SDK configuration |
| S3 access denied | Exact prefix ARN, `GetObjectVersion`, bucket explicit denies, encryption permissions |
| File not found | URL decoding, wrong bucket/Region, key case, source version |
| Same file processed repeatedly | Exceptions before deletion, heartbeat timeout, duplicate claim logic |
| Jobs stay processing forever | Lease expiry, reclaim condition, worker ownership renewal |
| DLQ fills with valid files | Missing permission, resource exhaustion, visibility too short, excessive console receives |
| Upload works with Python but browser fails | CORS, signed form fields, method, frontend origin |
| Download URL fails immediately | Signing role's permission, key/version, session expiry, URL alteration |
| PDF returns empty text | Scanned pages require OCR; extracted text is not guaranteed |
| Service works manually but not at boot | EnvironmentFile, executable path, ownership, working directory, network |

## 13. Optional extensions

1. Add a second worker, then build an Auto Scaling group based on backlog per worker.
2. Add termination lifecycle handling so workers stop receiving and finish active work before scale-in.
3. Move workers into private subnets and configure the required service endpoints.
4. Use customer-managed KMS keys and work through the additional key-policy requirements.
5. Write Terraform or CloudFormation for a repeatable deployment.
6. Add OCR for scanned PDFs using a service or isolated processing tool of your choice.

## 14. Cleanup

Stop the services and disable their schedules. Remove S3 notifications before deleting destination queues. Terminate the lab EC2 instances and remove unused volumes, snapshots, and addresses. Empty all S3 object versions and delete markers, then delete the bucket. Delete both SQS queues, the job table, lab log groups and alarms, and assignment-specific IAM policies/roles/instance profiles. Remove optional endpoints or NAT resources you created. Review your billing dashboard afterward; do not assume the lab is free.

## Official reading material

Read these alongside the corresponding stages:

- EC2 instance roles: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html
- S3 notification destinations: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html
- S3 event structure and object-key encoding: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
- SQS visibility and heartbeats: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- DynamoDB conditional writes: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
- Boto3 presigned POST: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/generate_presigned_post.html
- S3 presigned access: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- CloudWatch agent: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Install-CloudWatch-Agent.html

The Python starter is intended for guided practice. The supplied code has been checked locally for syntax and basic CSV/TXT behavior; deployment, IAM, S3 notifications, retries, and multi-worker behavior must be exercised in your AWS account. The advanced sections are implementation exercises, not features already supplied by the starter.
