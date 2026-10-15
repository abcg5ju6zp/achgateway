---
layout: page
title: Submitting Files
hide_hero: true
show_sidebar: false
menubar: docs-menu
---

# File Submission

ACH is a fixed-width file format used to debit or credit bank accounts. Often businesses will use this payment method to collect or distribute funds to their customers/users. The main record within an ACH file to accomplish this [EntryDetail records are created](https://moov-io.github.io/ach/file-structure/#entry-detail-record) to specify the amount, account, and description. For more details refer to Gusto's developer-focused series: [How ACH Works: A Developer Perspective, Part 1](https://engineering.gusto.com/how-ach-works-a-developer-perspective-part-1-339d3e7bea1), [Part 2](https://engineering.gusto.com/how-ach-works-a-developer-perspective-part-2-7a890638c4dd), and [Part 3](https://engineering.gusto.com/how-ach-works-a-developer-perspective-part-3-cd98728cf31f).

ACHGateway does not cover the business rules or Nacha requirements of creating ACH files. ACHGateway does allow files to be submitted and queued for their eventual delivery to the Federal Reserve. There are "cutoff windows" throughout a banking day to flush files from ODFI's to RDFI's (receiving financial institutions).

<a href="../../images/OSS_File_Submission.png"><img src="../../images/OSS_File_Submission.png" /></a>

## Implementation

There are two main methods for submitting files to ACHGateway: HTTP or stream. Files can also be canceled. Each file needs to have a `shardKey` and `fileID`. Refer to [our guide on sharding](../shards/) for more context.

- `shardKey`: This is a many-to-one identifier used for assigning the shard.
- `fileID`: A unique identifier for this file.

### HTTP

ACHGateway has an endpoint for submitting a file to be queued. Refer to the [endpoint docs](https://moov-io.github.io/achgateway/api/#post-/shards/-shardKey-/files/-fileID-) for more details.

```
POST /shards/{shardKey}/files/{fileID}
```

- Content-Type: `text/plain` (default)
   - Body: Nacha format
- Content-Type: `application/json`
   - Body: moov-io/ach JSON format

The request body may be a [Nacha formatted](https://github.com/moov-io/ach/blob/master/test/testdata/ppd-debit.ach) file or the [moov-io/ach JSON representation](https://github.com/moov-io/ach/blob/master/test/testdata/ppd-valid.json). The incoming file must pass Nacha validation rules enforced by the moov-io/ach library.

A successful HTTP response is JSON and includes the predicted next automated cutoff when it can be computed:

```
{
  "id": "uuid",
  "shardKey": "uuid",
  "error": "",
  "nextCutoff": "2025-01-07T16:15:00-05:00",
  "acceptedAt": "2025-01-07T18:37:00Z"
}
```

`nextCutoff` is optional. Existing clients that only read `id`, `shardKey`, and `error` can ignore the new fields. The timestamp is the next configured window for that shard after the file is accepted, using `Cutoffs.On` (`banking-days` by default, or `all-days`). Weekends never have an automated cutoff. A later [manual trigger](../../ops/cutoffs/#manual-triggers) can still upload the file earlier.

### Stream

ACHGateway can accept files over a "stream" implementation supported by `gocloud.dev/pubsub`. The most common implementation is Kafka and the event format is JSON described by the [`models` package provided with ACHGateway](https://pkg.go.dev/github.com/moov-io/achgateway/pkg/models).

Submit `QueueACHFile` events:

```
{
  "id": "uuid",
  "shardKey": "uuid",
  "file": {
    ...
  }
}
```

#### Notes

Kafka topics need to be created outside of ACHGateway. Consider your needs around partitions, retention, and checkpointing when creating topics.

Make sure to understand the implications of enabling/disabling consumer groups with your kafka subscription and multiple instances of ACHGateway.

## Encryption

Both submission implementations can accepted encoded and encrypted files. This is often required to meet compliance rules. Refer to the [`compliance` package provided with ACHGateway](https://pkg.go.dev/github.com/moov-io/achgateway/pkg/compliance) for protecting files prior to submission.

**Example**: Specify the [`Transform` config section](../../config/#inbound)

## Upload Receipt

After pending files are uploaded to the remote server a `FileUploaded` event is emitted.

```
{
    "fileID": "uuid",
    "shardKey": "uuid",
    "filename": "BANK_ACH_UPLOAD_20220601_123051.ach",
    "uploadedAt": "timestamp"
}
```

## Idempotent submissions and retries

Every submission carries a **stable commit identity**. Choose one explicitly with an `Idempotency-Key` HTTP request header — Kafka producers can instead set the record key, which is exposed as message metadata. When no key is provided the identity is derived deterministically from the shard key and file ID (`sha256(shardKey, fileID)`, ignoring an optional `.ach` suffix).

Internally a submission moves through durable, recoverable stages before it is eligible for a cutoff:

1. **received** — the reception fact is recorded in the ledger (`file_receptions`).
2. **staged** — the NACHA bytes (and ValidateOpts sidecar) are fsync'd into a staging directory; a write failure cleans up and the message is redelivered.
3. **indexed** — an unpublished file index row exists.
4. **published** — the staged file is atomically renamed into the shard's `mergable/` directory and the result (`acceptedAt`, `nextCutoff`, any error) is frozen in the ledger.

The upstream message is acknowledged **only after stage 4**, so a crash in stages 1-3 simply redelivers the message: the same commit resumes where it stopped without writing the file twice. A retry that arrives after publication — an HTTP timeout or a Kafka redelivery — returns the original frozen result (identical `acceptedAt` and `nextCutoff`) and never re-merges or double-counts the file. Concurrent requests carrying the same key share one in-flight result.

On startup ACHGateway reconciles unfinished commits against staging and `mergable/` disk state: staged files are finished, files moved before the final marker are finalized, and content missing everywhere is marked failed with a frozen error result. Pre-upgrade `mergable/` files without a ledger row are backfilled to published.

A background **notification outbox** delivers the frozen `QueueACHFileResponse` event for every terminal commit (published or failed). If the event sink is down the result stays pending and the send retries; this never touches the stored file. HTTP callers that missed their synchronous response should simply retry the same commit.

Cutoff merges **only files marked published** in the index. A file still in staging (or whose publication cannot be verified) is held back and merged at a later cutoff; if the index is unavailable the cutoff fails closed rather than moving unverified files.

# Canceling Files

### HTTP

Make an HTTP request to cancel the file. This request can be made before the file is submitted to ACHGateway. Refer to the [endpoint docs](https://moov-io.github.io/achgateway/api/#delete-/shards/-shardKey-/files/-fileID-) for more details.

```
DELETE /shards/{shardKey}/files/{fileID}
```

### Stream

Publish a [`CancelACHFile`](https://pkg.go.dev/github.com/moov-io/achgateway/pkg/models#CancelACHFile) event to cancel a submitted file. The canceling can arrive before the `QueueACHFile` event and will still cancel the file.

```
{
  "id": "uuid",
  "shardKey": "uuid"
}
```

A cancel that arrives **before** the file is published is durable: it is recorded against the same commit identity and applied atomically at publish time. Cancellation only suppresses files that have not been uploaded yet — once a cutoff has uploaded a file, cancel cannot recall it.

# Additional Notes

- Refer to the [merging operations](../../ops/merging/) page for more details on pending file storage.
- Every accepted submission produces a `QueueACHFileResponse` event (HTTP responses and the notification outbox both carry it). See the [events documentation](./events.md) for the payload and delivery semantics.

