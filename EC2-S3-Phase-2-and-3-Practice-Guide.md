# EC2 + S3 File Processing — Phases 2 and 3

A hands-on companion to **EC2-S3-Practice-Guide.md**. Continue your existing project rather than creating another bucket and queue. This guide is for practice: no marks, submission rules, or evaluation.

## Roadmap

| Phase | Build | Suggested pace |
|---|---|---|
| 1 — Existing foundation | Private S3 uploads, SQS events, one EC2 processor | Already introduced in the original guide |
| 2 — Reliable processing | Persistent job state, leases, duplicate handling, heartbeats, retries and crash recovery | Two practice sessions |
| 3 — Application access | API on a second EC2 instance, customer isolation, temporary upload/download access and a client | Two practice sessions |

### Before you continue

Confirm that uploading `students.csv` under `incoming/customer-a/demo/` creates a JSON report in `processed/`. Keep the original `processor.py`: both later phases reuse its `process_file` function and `MAX_BYTES` constant.

The bucket must have versioning enabled before uploading new practice files. The main SQS queue and bucket must share a Region; configure object-created notifications only for `incoming/`. Use the existing standard queue with a 120-second visibility timeout, 20-second long polling and a DLQ after five receives. Use the retention settings from Phase 1.

Examples use Ubuntu, `/opt/file-worker`, and `ap-south-1`. Substitute your real Region, bucket name, queue URL and account ID. All AWS operations below are run by you in your own account; the guide does not provision resources automatically. Use sample files. Charges apply.

## Phase 2: Make processing recoverable

### 2.1 Understand what changes

The original worker can process a message more than once. It can also crash after writing a result but before deleting the message. We will persist enough information to recover safely.

| Situation | Intended behavior |
|---|---|
| New source version | Claim the job and process it |
| Already completed job | Acknowledge the duplicate without processing again |
| Another worker holds a valid claim | Leave the message for later |
| Worker disappears | Reclaim its job after the lease expires |
| Processing takes longer than visibility | Renew both the job lease and message visibility |
| Old worker resumes after losing ownership | It cannot replace the published result pointer |

This provides a consistent published result; it does not guarantee that computation or S3 writes happen exactly once. Duplicate delivery remains possible. Briefly overlapping computation can happen when a lease expires; conditional ownership checks control which attempt can publish its result.

### 2.2 Create the job table

Use the DynamoDB console or run this from CloudShell/an administrative terminal:

```bash
aws dynamodb create-table \
  --table-name file-processing-jobs \
  --attribute-definitions AttributeName=job_id,AttributeType=S \
  --key-schema AttributeName=job_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1
```

If you already created this table during the earlier job-registration exercise, reuse it. Wait for `ACTIVE` before continuing. No sort key is required.

The earlier `job_demo.py` created demonstration `PENDING` items without leases. Those are not worker-ready records. Delete only those identified demonstration items through an administrative identity, or use new file versions for this lab. Never remove active worker records.

Add this statement to the worker's IAM policy, retaining its existing S3 and SQS permissions:

```json
{
  "Effect": "Allow",
  "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"],
  "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/file-processing-jobs"
}
```

The worker still needs `s3:GetObjectVersion` for inputs, `s3:PutObject` for reports, and SQS receive/delete/change-visibility permissions. Attach policies to the instance role; do not put access keys in code.

Set this on EC2:

```bash
cd /opt/file-worker
export AWS_DEFAULT_REGION=ap-south-1
export BUCKET_NAME=your-existing-bucket
export QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/ACCOUNT_ID/file-processing-queue
export JOB_TABLE=file-processing-jobs
```

### 2.3 Define job identity and ownership

We derive `job_id` from the exact JSON encoding of `[bucket, key, version_id]`, then SHA-256 hash it. This matches the original worker. The same event produces the same ID; a new object version produces a different ID.

| Field | Purpose |
|---|---|
| `job_id` | Stable identity of the source version |
| `status` | `PROCESSING`, retryable `FAILED`, or `COMPLETED` |
| `owner_token` | Random identity for one processing attempt |
| `lease_until` | UTC epoch seconds when ownership can be reclaimed |
| `attempts` | Number of successful worker claims |
| `customer_id` | Customer segment from the server-controlled source key |
| `source_bucket`, `source_key`, `source_version` | Exact input |
| `result_key`, `result_version` | Published result pointer |
| `updated_at`, `last_error` | Operational information |

The worker creates `PROCESSING` directly when claiming the job. Phase 3 represents pre-processing waiting states at the API layer; it does not insert incomplete `PENDING` job rows.

Keep EC2 clocks synchronized. A lease is an application-level deadline, not DynamoDB TTL; TTL deletion is asynchronous and unsuitable for timely lock release.

### 2.4 Add the job-state module

Save as `/opt/file-worker/job_state.py`:

```python
import hashlib
import json
import os
import time
import uuid

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

TABLE = os.getenv('JOB_TABLE', 'file-processing-jobs')
LEASE_SECONDS = 120


def table():
    # Each calling thread gets its own resource instance.
    return boto3.resource('dynamodb', config=Config(
        connect_timeout=5, read_timeout=10,
        retries={'mode': 'standard', 'total_max_attempts': 3},
    )).Table(TABLE)


def job_id_for(bucket, key, version):
    identity = json.dumps([bucket, key, version], separators=(',', ':'))
    return hashlib.sha256(identity.encode()).hexdigest()


def get_job(job_id):
    return table().get_item(Key={'job_id': job_id}, ConsistentRead=True).get('Item')


def claim(bucket, key, version):
    job_id = job_id_for(bucket, key, version)
    owner = str(uuid.uuid4())
    now = int(time.time())
    customer = key.split('/')[1]
    try:
        table().update_item(
            Key={'job_id': job_id},
            UpdateExpression=(
                'SET #s=:running, owner_token=:owner, lease_until=:expiry, '
                'source_bucket=:bucket, source_key=:key, source_version=:version, '
                'customer_id=:customer, updated_at=:now ADD attempts :one'
            ),
            ConditionExpression=(
                'attribute_not_exists(job_id) OR '
                '(#s <> :done AND lease_until <= :now)'
            ),
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':running': 'PROCESSING', ':done': 'COMPLETED', ':owner': owner,
                ':expiry': now + LEASE_SECONDS, ':now': now, ':bucket': bucket,
                ':key': key, ':version': version, ':customer': customer, ':one': 1,
            },
        )
        return job_id, owner
    except ClientError as error:
        if error.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        item = get_job(job_id)
        if item and item['status'] == 'COMPLETED':
            return job_id, None
        raise RuntimeError('Job is owned by another worker or lacks an expired lease')


def owned_update(job_id, owner, update, values):
    now = int(time.time())
    return table().update_item(
        Key={'job_id': job_id}, UpdateExpression=update,
        ConditionExpression='owner_token=:owner AND #s=:running AND lease_until > :now',
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={
            ':owner': owner, ':running': 'PROCESSING', ':now': now, **values,
        },
    )


def renew(job_id, owner):
    owned_update(job_id, owner, 'SET lease_until=:expiry, updated_at=:now',
                 {':expiry': int(time.time()) + LEASE_SECONDS})


def complete(job_id, owner, result_key, result_version):
    owned_update(
        job_id, owner,
        'SET #s=:done, result_key=:key, result_version=:version, updated_at=:now',
        {':done': 'COMPLETED', ':key': result_key, ':version': result_version},
    )


def failed(job_id, owner, error):
    owned_update(
        job_id, owner,
        'SET #s=:failed, lease_until=:zero, last_error=:error, updated_at=:now',
        {':failed': 'FAILED', ':zero': 0, ':error': type(error).__name__},
    )
```

Read the claim condition carefully: a missing job can be created, or an unfinished job whose lease has expired can be reclaimed. `COMPLETED` jobs are never reclaimed. Renewal, completion and failure updates require the current owner and a valid lease.

Conditional failures during a duplicate claim are normal. Network, IAM and service errors must remain visible rather than being mistaken for duplicates.

### 2.5 Replace the starter worker

Save as `/opt/file-worker/worker_v2.py`. Keep the original worker for comparison. Stop it before running the replacement so your old worker does not bypass the new coordination logic.

The heartbeat extends visibility for the entire message and renews the active job. Multiple records in one event are handled sequentially. If a later record fails, an earlier completed record is skipped when the message is retried.

```python
import json
import logging
import os
import signal
import threading
import time
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config

import job_state
from processor import MAX_BYTES, process_file

BUCKET = os.environ['BUCKET_NAME']
QUEUE = os.environ['QUEUE_URL']
CONFIG = Config(connect_timeout=5, read_timeout=30,
                retries={'mode': 'standard', 'total_max_attempts': 3})
s3 = boto3.client('s3', config=CONFIG)
sqs = boto3.client('sqs', config=CONFIG)
stopping = threading.Event()
logging.basicConfig(level=logging.INFO, format='%(message)s')


def log(**fields):
    logging.info(json.dumps(fields))


class Heartbeat:
    def __init__(self, receipt):
        self.receipt = receipt
        self.stop = threading.Event()
        self.lost = threading.Event()
        self.lock = threading.Lock()
        self.active = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        # Clients/resources used by this thread are constructed here.
        queue = boto3.client('sqs', config=CONFIG)
        while not self.stop.wait(30):
            try:
                with self.lock:
                    if self.active:
                        job_state.renew(*self.active)
                    queue.change_message_visibility(
                        QueueUrl=QUEUE, ReceiptHandle=self.receipt,
                        VisibilityTimeout=120,
                    )
            except Exception as error:
                self.lost.set()
                log(event='heartbeat_failed', error_type=type(error).__name__)
                return

    def check(self):
        if self.lost.is_set():
            raise RuntimeError('Heartbeat lost; refusing to commit')


def process_record(record, heartbeat):
    bucket = record['s3']['bucket']['name']
    obj = record['s3']['object']
    key = unquote_plus(obj['key'])
    version = obj.get('versionId')
    parts = key.split('/')
    if (bucket != BUCKET or len(parts) < 4 or parts[0] != 'incoming'
            or not parts[1] or not version or version == 'null'):
        raise ValueError('Unexpected source, key layout, or missing version')

    with heartbeat.lock:
        heartbeat.check()
        job_id, owner = job_state.claim(bucket, key, version)
        if owner is None:
            log(job_id=job_id, event='duplicate_completed')
            return
        heartbeat.active = (job_id, owner)

    started = time.monotonic()
    log(job_id=job_id, owner_token=owner, event='processing')
    try:
        response = s3.get_object(Bucket=bucket, Key=key, VersionId=version)
        try:
            if response['ContentLength'] > MAX_BYTES:
                raise ValueError('File exceeds size limit')
            data = response['Body'].read(MAX_BYTES + 1)
        finally:
            response['Body'].close()
        # Lab only: simulate a long job without blocking heartbeat execution.
        time.sleep(int(os.getenv('LAB_DELAY_SECONDS', '0')))
        report = process_file(key, data)
        report.update(job_id=job_id, source_key=key, source_version=version,
                      processing_seconds=round(time.monotonic() - started, 3))
        heartbeat.check()
        # Each attempt writes its own object. Only the committed pointer is publicized.
        result_key = f'processed/{job_id}/{owner}.json'
        result = s3.put_object(
            Bucket=bucket, Key=result_key, Body=json.dumps(report).encode(),
            ContentType='application/json',
        )
        with heartbeat.lock:
            heartbeat.check()
            job_state.complete(job_id, owner, result_key, result['VersionId'])
            heartbeat.active = None
        log(job_id=job_id, event='completed', result_key=result_key)
    except Exception as error:
        with heartbeat.lock:
            heartbeat.active = None
            try:
                job_state.failed(job_id, owner, error)
            except Exception:
                # Ownership may have expired, or completion may already have succeeded.
                pass
        log(job_id=job_id, event='attempt_failed', error_type=type(error).__name__)
        raise


def handle(message):
    event = json.loads(message['Body'])
    if event.get('Event') == 's3:TestEvent':
        return
    records = event.get('Records')
    if not isinstance(records, list) or not records:
        raise ValueError('Unsupported event envelope')
    heartbeat = Heartbeat(message['ReceiptHandle'])
    heartbeat.thread.start()
    try:
        for record in records:
            process_record(record, heartbeat)
        heartbeat.check()
    finally:
        heartbeat.stop.set()
        heartbeat.thread.join()


def main():
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    while not stopping.is_set():
        try:
            response = sqs.receive_message(
                QueueUrl=QUEUE, MaxNumberOfMessages=1,
                WaitTimeSeconds=20, VisibilityTimeout=120,
            )
            for message in response.get('Messages', []):
                if stopping.is_set():
                    sqs.change_message_visibility(
                        QueueUrl=QUEUE, ReceiptHandle=message['ReceiptHandle'],
                        VisibilityTimeout=0,
                    )
                    break
                try:
                    handle(message)
                    sqs.delete_message(QueueUrl=QUEUE,
                                       ReceiptHandle=message['ReceiptHandle'])
                except Exception as error:
                    log(event='message_retry', error_type=type(error).__name__)
        except Exception as error:
            log(event='poll_error', error_type=type(error).__name__)
            stopping.wait(5)


if __name__ == '__main__':
    main()
```

Why write each attempt to its own S3 key? An expired worker could finish a delayed S3 write. With separate attempt keys, it cannot overwrite another attempt's object. A conditional completion update decides which result pointer the API will expose. Orphan attempt objects can remain; this is a deliberate tradeoff.

The worker checks for heartbeat loss before publishing. If the network stalls or processing monopolizes execution beyond the lease, ownership may expire; the completion condition prevents a stale owner from committing after takeover. This teaching implementation does not forcibly terminate a CPU-hung PDF parser. Use isolated subprocesses with time/memory limits before processing arbitrary untrusted documents.

Keep jobs short in this lab, ideally under ten minutes. SQS visibility extensions cannot exceed the overall 12-hour limit measured from receipt. The worker has no special long-duration workflow support.

### 2.6 Start the worker

For foreground practice:

```bash
.venv/bin/python worker_v2.py
```

If Phase 1 uses systemd, first stop it:

```bash
sudo systemctl stop file-worker
```

Update `/etc/file-worker.env` to include `JOB_TABLE=file-processing-jobs` and your existing Region, bucket and queue values. Update the service `ExecStart` to:

```ini
ExecStart=/opt/file-worker/.venv/bin/python /opt/file-worker/worker_v2.py
TimeoutStopSec=660
```

Use a shutdown timeout appropriate to the lab's maximum duration; 660 seconds is for jobs deliberately kept under ten minutes. SIGTERM stops new polling while allowing the current message to finish. A forced kill is recovered through lease expiry and redelivery.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now file-worker
sudo journalctl -u file-worker -f
```

### 2.7 Practice: follow a job end to end

1. Upload `students.csv` to `incoming/customer-a/demo/students.csv` again so it gets a new version.
2. Watch for a `processing` log entry and copy its `job_id`.
3. Explore the corresponding DynamoDB item; small jobs may already be `COMPLETED`.
4. Inspect its source version, attempts, result key and result version.
5. Download the report using an administrative identity and compare it with the original CSV.

For a slower run, stop systemd and run the foreground worker with:

```bash
LAB_DELAY_SECONDS=150 .venv/bin/python worker_v2.py
```

This delay allows you to see `PROCESSING` and heartbeat renewals. The artificial delay is only for practice. Remove it afterward.

### 2.8 Practice: duplicates and two workers

Capture a real S3 notification body from the main queue using an operator identity while the worker is stopped, and save it as `event.json`. Receiving it increases its receive count; delete that captured delivery using its current receipt handle to avoid leaving an extra in-flight copy. Do this with fresh practice data, not a production queue.

Send the saved event twice from an operator terminal:

```bash
aws sqs send-message --queue-url "$QUEUE_URL" \
  --message-body file://event.json --region ap-south-1

aws sqs send-message --queue-url "$QUEUE_URL" \
  --message-body file://event.json --region ap-south-1
```

Do not send an SNS-wrapped message; this worker expects the direct S3 `Records` envelope. Do not use an `s3:TestEvent` for duplicate-processing practice.

Start the updated worker on two EC2 instances with the same configuration, or use two foreground terminals for the first experiment. Never run the old worker alongside them. Observe one valid claim, then either a busy-job retry or a completed-duplicate log. After completion, replaying the event should not increment attempts.

### 2.9 Practice: crash, retry and DLQ recovery

| Experiment | What to look for |
|---|---|
| Stop worker, then upload | Queue holds messages until a worker returns |
| Kill a delayed foreground worker with `kill -9 PID` | A later worker claims the expired lease and completes the job |
| Stop after report upload but before completion | Unpublished attempt result may remain; retry publishes its own pointer |
| Stop after completion but before queue deletion | Redelivery sees `COMPLETED` and is acknowledged |
| Temporarily remove result-write permission | Retryable `FAILED`, then eventual DLQ routing |
| Upload two versions at the same key | Two job IDs, each processing its own input version |
| Use a corrupted PDF | Worker remains available; repeated failure eventually reaches DLQ |

Use an editor breakpoint or temporary lab pause at the indicated boundary to make the two commit-window experiments deterministic. Remove those pauses afterward. If using systemd, `Restart=on-failure` may restart a killed process immediately; that also exercises recovery, or use foreground workers for more control.

Restore permissions before redriving the DLQ through the SQS console using an operator identity. `FAILED` is intentionally retryable in this lab. A permanently corrupt file will fail again even after redrive; upload a corrected file as a new version instead.

The `attempts` counter counts claims, not every SQS receive. A message can reach DLQ without a claim—for example, when another worker is busy. DLQ arrival does not automatically update DynamoDB; the record shows its last processing state. A reconciliation component is a useful extension.

## Phase 3: Add an API and a client

### 3.1 Application flow

The API creates an upload request and returns a temporary S3 POST form. The client uploads directly to S3. The existing event pipeline processes the new version. The API resolves the latest version for that request to a job and returns its state. Once completed, it issues a temporary download URL for the committed report version.

| Identifier | When it exists | Meaning |
|---|---|---|
| `request_id` | Before upload | One server-generated upload location |
| S3 version ID | After upload | Exact stored version of the file |
| `job_id` | Derivable after upload | Hash of bucket, key and version |

A presigned POST can be reused before expiry. Repeated upload to the same request key creates versions. In this lab `GET /uploads/<request-id>` returns the **latest uploaded version**; previously returned job IDs remain usable at `/jobs/<job-id>`. Create a new request for each ordinary upload. This is not a one-use upload-token implementation.

### 3.2 Prepare the API instance and upload table

Launch a second Ubuntu EC2 instance. Give it a separate instance role and require IMDSv2. It needs outbound AWS connectivity. Keep the API bound to loopback; use an SSH tunnel for the exercises so API keys are not sent over public HTTP. Do not open port 8000 publicly for this setup.

Create another on-demand DynamoDB table:

```bash
aws dynamodb create-table \
  --table-name file-upload-requests \
  --attribute-definitions AttributeName=request_id,AttributeType=S \
  --key-schema AttributeName=request_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1
```

Wait for `ACTIVE`. Each item records `request_id`, authenticated `customer_id` and the exact `source_key`. This small mapping removes the need to scan the jobs table or guess job IDs before S3 creates a version.

### 3.3 API IAM permissions

Attach this policy to the API instance role after replacing placeholders. Keep worker and API roles separate.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::BUCKET_NAME/incoming/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::BUCKET_NAME"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObjectVersion",
      "Resource": "arn:aws:s3:::BUCKET_NAME/processed/*"
    },
    {
      "Effect": "Allow",
      "Action": "dynamodb:GetItem",
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/file-processing-jobs"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem", "dynamodb:GetItem"],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/file-upload-requests"
    }
  ]
}
```

`GetObject` permits the API's `HeadObject` check on inputs. Bucket-level `ListBucket` allows S3 to distinguish a nonexistent object with a 404 response; without it an absent object may produce 403, which must not be silently treated as pending. The API does not expose listing. This grants the trusted API role bucket-wide list capability as a deliberate lab simplification; a more restricted design can track uploaded versions through an event-maintained request mapping and avoid HEAD checks.

The API role can sign version-specific result downloads but cannot write reports or consume queue messages. Customer isolation is enforced by the API for requests made with its shared role; it is not separate AWS credentials per customer. Other identities allowed to upload directly to the bucket are trusted operators in this lab.

### 3.4 Install the API dependencies

On the API EC2 instance:

```bash
sudo apt-get update
sudo apt-get install -y python3-venv
sudo install -d -o ubuntu -g ubuntu /opt/file-api
cd /opt/file-api
python3 -m venv .venv
.venv/bin/pip install boto3 flask gunicorn
```

Copy `job_state.py` from Phase 2 into this directory; the API uses its ID function. Configure the same Region and bucket, plus both table names:

```bash
export AWS_DEFAULT_REGION=ap-south-1
export BUCKET_NAME=your-existing-bucket
export JOB_TABLE=file-processing-jobs
export UPLOAD_TABLE=file-upload-requests
```

Generate two random tokens using `.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))'`. Store one for customer A and one for customer B. Configure the API with the real values:

```bash
export API_KEYS_JSON='{"REPLACE_WITH_TOKEN_A":"customer-a","REPLACE_WITH_TOKEN_B":"customer-b"}'
```

Do not use those literal placeholder strings as actual credentials. Treat API keys as secrets. For this private exercise environment variables are sufficient; add secret storage and rotation as a later improvement. Do not commit them, paste them into shared screenshots, or enable verbose request-header logging.

### 3.5 Create the API

Save as `/opt/file-api/api.py`:

```python
import hmac
import json
import os
import re
import uuid
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, abort, g, jsonify, request

from job_state import job_id_for

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024  # JSON requests, not file bodies
BUCKET = os.environ['BUCKET_NAME']
KEYS = json.loads(os.environ['API_KEYS_JSON'])  # token -> customer identity
if not KEYS or any(not token or not re.fullmatch(r'[a-zA-Z0-9_-]+', customer)
                   for token, customer in KEYS.items()):
    raise ValueError('Configure nonempty API tokens and safe customer IDs')

ddb = boto3.resource('dynamodb')
jobs = ddb.Table(os.getenv('JOB_TABLE', 'file-processing-jobs'))
uploads = ddb.Table(os.getenv('UPLOAD_TABLE', 'file-upload-requests'))
s3 = boto3.client('s3', config=Config(signature_version='s3v4'))


@app.before_request
def authenticate():
    token = request.headers.get('X-API-Key', '')
    for expected, customer in KEYS.items():
        if hmac.compare_digest(token, expected):
            g.customer = customer
            return
    abort(401)


def clean(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def owned_item(table, key):
    item = table.get_item(Key=key, ConsistentRead=True).get('Item')
    if not item or item.get('customer_id') != g.customer:
        abort(404)  # Same response for nonexistent and another customer's data.
    return item


@app.errorhandler(ClientError)
@app.errorhandler(BotoCoreError)
def aws_failure(error):
    app.logger.error('AWS operation failed: %s', type(error).__name__)
    return jsonify(error='AWS operation unavailable; retry later'), 503


@app.post('/uploads')
def new_upload():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400, 'Provide a JSON object')
    filename = body.get('filename', '')
    if not isinstance(filename, str) or not re.fullmatch(
            r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,119}', filename):
        abort(400, 'Use a simple filename of at most 120 characters')
    if filename.rsplit('.', 1)[-1].lower() not in {'pdf', 'csv', 'txt'}:
        abort(400, 'Only PDF, CSV, and TXT are supported')
    request_id = str(uuid.uuid4())
    key = f'incoming/{g.customer}/{request_id}/{filename}'
    uploads.put_item(
        Item={'request_id': request_id, 'customer_id': g.customer,
              'source_key': key},
        ConditionExpression='attribute_not_exists(request_id)',
    )
    form = s3.generate_presigned_post(
        Bucket=BUCKET, Key=key,
        Conditions=[['content-length-range', 1, 20 * 1024 * 1024]],
        ExpiresIn=300,
    )
    return jsonify(request_id=request_id, upload=form), 201


@app.get('/uploads/<request_id>')
def upload_status(request_id):
    item = owned_item(uploads, {'request_id': request_id})
    try:
        source = s3.head_object(Bucket=BUCKET, Key=item['source_key'])
    except ClientError as error:
        if error.response['Error']['Code'] in {'404', 'NoSuchKey', 'NotFound'}:
            return jsonify(request_id=request_id, status='PENDING_UPLOAD')
        raise
    version = source.get('VersionId')
    if not version or version == 'null':
        abort(409, 'Bucket versioning must be enabled')
    job_id = job_id_for(BUCKET, item['source_key'], version)
    job = jobs.get_item(Key={'job_id': job_id}, ConsistentRead=True).get('Item')
    return jsonify(request_id=request_id, job_id=job_id,
                   source_version=version,
                   status=job['status'] if job else 'QUEUED')


@app.get('/jobs/<job_id>')
def job_status(job_id):
    item = owned_item(jobs, {'job_id': job_id})
    # Return only public status fields, not internal lease/owner information.
    fields = ['job_id', 'status', 'attempts', 'source_key', 'source_version',
              'updated_at', 'last_error']
    return jsonify(clean({key: item[key] for key in fields if key in item}))


@app.get('/jobs/<job_id>/download')
def download(job_id):
    item = owned_item(jobs, {'job_id': job_id})
    if item['status'] != 'COMPLETED':
        abort(409, 'Report is not ready')
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': BUCKET, 'Key': item['result_key'],
                'VersionId': item['result_version']},
        ExpiresIn=300,
    )
    return jsonify(url=url, expires_in=300)
```

The API derives customer identity from the key, not a customer ID supplied in JSON. Every status/download read checks ownership. Unknown jobs and another customer's jobs both return 404. `job_id` unpredictability alone would not protect data.

Presigned POST fields are submitted as multipart form data with the file. They are not a signed PUT URL. The size range is checked by S3; file content is checked by the worker. CSV/TXT format limitations and PDF parsing precautions from Phase 1 still apply.

### 3.6 Start it through a local SSH tunnel

On the API server:

```bash
cd /opt/file-api
.venv/bin/gunicorn --bind 127.0.0.1:8000 --workers 1 --threads 1 api:app
```

One process and one thread keeps this example's shared Boto3 resource usage simple. Scale using separate Gunicorn worker processes later; do not turn on multithreading without reviewing SDK resource sharing.

On your laptop, open a separate terminal:

```bash
ssh -i /path/to/key.pem \
  -L 8000:127.0.0.1:8000 ubuntu@API_EC2_PUBLIC_IP
```

Keep the tunnel open. Your client connects to `http://127.0.0.1:8000`; SSH encrypts traffic across the network. The subsequent S3 upload/download uses HTTPS directly.

### 3.7 Upload a file and download its report

On your laptop, create a Python virtual environment and install `requests`. Save the script below as `client.py` and put `students.csv` in the same working directory.

Sample `students.csv`:

```csv
name,course,score
Asha,AWS,85
Ravi,Linux,
Meera,AWS,92
```

```python
import os
import time
from pathlib import Path

import requests

base = os.getenv('API_BASE', 'http://127.0.0.1:8000')
headers = {'X-API-Key': os.environ['API_KEY']}
path = Path('students.csv')

response = requests.post(f'{base}/uploads', headers=headers,
                         json={'filename': path.name}, timeout=30)
response.raise_for_status()
created = response.json()
form = created['upload']
with path.open('rb') as stream:
    response = requests.post(form['url'], data=form['fields'],
                             files={'file': (path.name, stream)}, timeout=60)
response.raise_for_status()
print('Uploaded. Request:', created['request_id'])

deadline = time.monotonic() + 600
while time.monotonic() < deadline:
    response = requests.get(f"{base}/uploads/{created['request_id']}",
                            headers=headers, timeout=30)
    response.raise_for_status()
    status = response.json()
    print('Status:', status['status'])
    if status['status'] == 'COMPLETED':
        response = requests.get(f"{base}/jobs/{status['job_id']}/download",
                                headers=headers, timeout=30)
        response.raise_for_status()
        report = requests.get(response.json()['url'], timeout=30)
        report.raise_for_status()
        Path('report.json').write_bytes(report.content)
        print('Saved report.json. Job:', status['job_id'])
        break
    # FAILED is retryable in this lab; DLQ routing is checked by the operator.
    time.sleep(3)
else:
    raise SystemExit('Still unfinished after 10 minutes; inspect worker and DLQ')
```

Set `API_KEY` locally using customer A's real token, then run the script:

```bash
export API_BASE=http://127.0.0.1:8000
export API_KEY=REPLACE_WITH_TOKEN_A
python client.py
```

Use your virtual environment's Python executable if it is not activated. Expected behavior: upload succeeds, status progresses from `QUEUED` or `PROCESSING` to `COMPLETED`, and `report.json` is saved. Fast jobs may complete before you observe intermediate states. For the sample CSV, expect three rows and one missing score.

Possible states:

| State | Meaning |
|---|---|
| `PENDING_UPLOAD` | Request exists, but there is no object at its key yet |
| `QUEUED` | Latest source version exists, but no job record has been claimed |
| `PROCESSING` | Worker has claimed the job; inspect lease expiry when diagnosing stale state |
| `FAILED` | Last attempt failed; retry or DLQ action may still be pending |
| `COMPLETED` | A result pointer has been committed |

The client times out after ten minutes rather than polling forever. It does not automatically redrive DLQs. Pending requests with abandoned uploads remain in the request table until cleanup; automated expiry is a useful extension.

### 3.8 Explore the API yourself

| Practice | What to observe |
|---|---|
| Call `/uploads` without a key | 401 |
| Request an upload for `photo.exe` | 400 |
| Request `../../students.csv` | 400; filenames cannot choose an S3 path |
| Upload a file larger than 20 MiB through the signed form | S3 rejects it |
| Use customer B's key to read customer A's job | 404 |
| Request a result while processing | 409 |
| Request an upload but never send a file | `PENDING_UPLOAD` |
| Start a new download after its five-minute URL expires | Access denied |
| Open an unsigned private S3 object URL | Access denied |
| Stop the worker, upload through API, then restart worker | Queue retains work; API eventually reports completion |
| Upload again with the same still-valid form | New source version and job; request status resolves the latest one |

An already started transfer need not stop at URL expiry. Make a fresh request after expiry for the expiration experiment. Presigned access can also end earlier if its underlying credentials cease to be valid.

Python clients do not need browser CORS. If you later add a frontend that uploads directly to S3, configure CORS for the exact frontend origin and required method; do not change bucket public-access settings to solve CORS problems.

### 3.9 Make the API survive reboot

Create a root-owned `/etc/file-api.env` containing the five API environment variables above, without `export`. Restrict it with `sudo chmod 600 /etc/file-api.env`. systemd reads it before starting the unprivileged process.

Create `/etc/systemd/system/file-api.service`:

```ini
[Unit]
Description=Private file-processing API
Wants=network-online.target
After=network-online.target

[Service]
User=ubuntu
WorkingDirectory=/opt/file-api
EnvironmentFile=/etc/file-api.env
ExecStart=/opt/file-api/.venv/bin/gunicorn --bind 127.0.0.1:8000 --workers 1 --threads 1 api:app
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Stop your foreground Gunicorn process before enabling the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now file-api
sudo journalctl -u file-api -f
```

Reconnect the SSH tunnel after reboot. For a publicly accessible application, replace the tunnel with a properly configured HTTPS entry point and review authentication, rate limiting and request handling before exposing it.

## Troubleshooting both phases

| Symptom | Inspect |
|---|---|
| DynamoDB access denied | Correct instance role, Region and table ARN |
| Claim always fails for a demo row | Earlier `PENDING` demo item lacks a lease; identify it and use fresh practice data |
| Job shows `PROCESSING` after a crash | Lease expiry, queue visibility and surviving worker availability |
| Extra S3 reports exist | Uncommitted attempts can leave orphans; use DynamoDB's result pointer |
| Completed job ran again | Same source version? Same job-ID encoding? Old worker still running? |
| DLQ contains a valid file | Processing permission, memory/CPU pressure, heartbeat, repeated busy deliveries |
| API returns 503 while pending | HEAD permission, bucket-list capability, Region and table access |
| API returns 404 for a job | Job not yet registered, wrong ID, or wrong authenticated customer |
| Signed upload fails | Use all returned POST fields, correct endpoint, unexpired form, size limit |
| Download fails | API role `GetObjectVersion`, exact committed version, expired link |
| Port 8000 already in use | Stop foreground Gunicorn before systemd, or resolve a local tunnel conflict |
| API unreachable from laptop | Tunnel, SSH security group, public address, local port and Gunicorn status |

## Monitoring practice

Continue the original CloudWatch exercises: main queue oldest-message age, visible backlog and DLQ visible count. Add job ID and event type to log filters. The worker uses structured JSON on stdout, so systemd stores it in journald. If you use the CloudWatch agent's file collector, configure a rotating application file or an explicit logging route; file collection does not automatically ingest journald.

Record dependency versions after a successful run with `.venv/bin/pip freeze > requirements-lock.txt` on each instance. Keep original code before experiments so you can compare behavior.

## Cleanup and retention

Disable the worker/API services and lab schedules when finished. Remove temporary tokens from local environment files, and delete local sample reports if no longer needed. Delete lab instances and unused storage/addresses when you are done using them. Remove notification configuration before deleting destination queues. Empty all S3 versions and delete markers before deleting the bucket. Delete both DynamoDB tables only if no longer needed for the project. Remove lab-specific IAM roles/policies and monitoring resources.

Choose retention that does not delete source versions before queued work can finish. Expire old attempt objects only after their results are no longer needed; a simple lifecycle rule cannot know which pointer DynamoDB currently publishes. Blanket expiration of all `processed/` objects eventually breaks old download records unless the application also expires or reconciles those records.

## Official references

- [Create a DynamoDB table](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/create-table.html)
- [DynamoDB conditional operations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_dynamodb_Scenario_ConditionalOperations_section.html)
- [DynamoDB UpdateItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html)
- [SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [S3 notification message structure](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html)
- [Boto3 presigned POST forms](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/generate_presigned_post.html)
- [S3 presigned access and expiration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)
- [S3 HeadObject and permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

## Validation scope

This companion provides runnable teaching code and guided failure experiments. Local checks cover syntax and selected mocked behavior; no live AWS deployment or multi-instance fault test was performed while preparing it. Complete the queue, IAM, lease, retry and customer-isolation exercises in your account before relying on the implementation. It is a lab foundation, not a production document-processing service.
