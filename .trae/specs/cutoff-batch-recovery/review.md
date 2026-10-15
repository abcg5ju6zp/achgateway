# Independent Review - Resumable Cutoff Batches

- **Reviewer**: fresh-context independent agent (read-only)
- **Result**: **fail** → remediation required
- **Commands run**: `go build ./...`, `go vet ./...`, `gofmt -l`, `go test -skip <docker tests> ./...`, `-race` on pipeline/upload/models, `-count=6` stability.

## Checkpoints (per AC)

| CP | Covers | Result | Evidence |
|---|---|---|---|
| CP-R1 | rule | pass | `go build`/`go vet`/`gofmt` clean; go.mod/go.sum unchanged |
| CP-R2 | AC-1/2/3 | pass | deterministic IDs, find-or-create, fixed snapshot and filenames; TestBatch_Identities / TestRecovery_SameWindowReuseAndSnapshotFixed |
| CP-R3 | AC-4/5/6 | pass | ambiguous→digest verify, missing→pending, mismatch→frozen+0 writes; TestRecovery_Ambiguous/Missing/Conflict |
| CP-R4 | AC-7/8 | pass | audit/event backfill only; TestRecovery_AuditFailure/EventFailure |
| CP-R5 | AC-9/10 | pass | per-output recovery; TestRecovery_PartialSuccess; crash-equivalent scenarios covered (prepared/audited/event gaps) |
| CP-R6 | AC-11 | pass (weak evidence) | code walk only; no holiday zero-op test |
| CP-R7 | AC-12 | **fail** | Open error falls back to legacy delete rule (F3) |
| CP-R8 | AC-13 | pass (minor diffs) | legacy suites pass substantively; F6/F7 behavior deltas |
| CP-R9 | AC-14 | pass | TestFileUploaded_BatchIDRoundTrip |
| CP-R10 | AC-15 (rubric) | 3/5 **fail** | F1 crash window, F2 cross-goroutine manifest rollback, F4 race |
| CP-R11 | AC-16 | **fail** | flaky panic + deterministic -race failure in pre-existing test after startup-recovery change (F4) |

## Findings

- **F1 (major)** crash between upload ACK (in-memory MarkUploaded) and manifest Persist re-uploads unconditionally on recovery; no pre-flight remote verification for prepared outputs.
- **F2 (major)** admin event-backfill endpoint loads→emits→full-state Persist concurrently with runBatch; can roll back Uploaded/Audited flags and cause re-upload. Fixed shared temp name.
- **F3 (major)** cleanup: manifest Open error (permissions/decrypt failure) falls through to "uploaded/ non-empty → delete" and can destroy active/conflicting batches.
- **F4 (major, test gate)** deterministic data race: Start's new startup recovery reads merger storage while TestFileReceiver_AcceptFileErr swaps in a nil-chest MockStorage; also caused one observed nil panic. Test fix required.
- **F5 (minor)** cleanup regex admits `-N` suffix dirs but timestamp parse doesn't strip it → collision dirs never cleaned + check errors.
- **F6 (minor)** merged-cache save failure permanently kills the output even though the parsed ACH file is in memory this run.
- **F7 (minor)** positive inputs but zero merged outputs + nil error leaves batch active forever.
- **F8 (minor)** one unreadable manifest aborts recovery of all other batches (contradicts independent-failure contract).
- **F9 (advisory)** conflict double-alerted within a run and re-alerted every window.
- **F10 (advisory)** manual event backfill omits batchID/UploadedAt.
- **F11 (advisory)** test gaps: holiday zero-op, events_api manifest marking, encrypted chest, UploadedAt equality.

## Verified safe

ReadFile error classification is conservative (EOF/network → unconfirmed); conflict path performs 0 writes/deletes; atomic manifest commit; fixed remote/audit paths; event concurrency cap + single persist/round; empty/holiday/cancel/merge-failure input preservation; cleanup cannot race a completed batch within retention.

## Remediation plan (tracked as issues in tasks.md)

F1 persist-after-ACK + pre-flight verify on resumed outputs; F2 serialize manifest writes via aggregator batchOpMu + unique temp + endpoint rewrite; F3 not-exist vs open-error split; F4 test MockStorage with embedded chest; F5 strip suffix in parse; F6 use in-memory parsed file; F7 complete zero-output batches; F8 skip corrupt manifests with metric; F9 single alert; F10 batchID/UploadedAt on backfill; F11 add tests.
