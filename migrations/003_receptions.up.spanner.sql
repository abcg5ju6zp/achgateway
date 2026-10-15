ALTER TABLE files ADD COLUMN published_at TIMESTAMP;

CREATE TABLE file_receptions (
  commit_id STRING(128) NOT NULL,
  shard_name STRING(100) NOT NULL,
  shard_key STRING(50) NOT NULL,
  file_id STRING(128) NOT NULL,
  source STRING(20) NOT NULL,
  hostname STRING(100) NOT NULL,
  stage STRING(20) NOT NULL,
  accepted_at TIMESTAMP NOT NULL,
  staged_at TIMESTAMP,
  published_at TIMESTAMP,
  notified_at TIMESTAMP,
  cancel_requested_at TIMESTAMP,
  content_sha256 STRING(64),
  result_json STRING(MAX),
  last_error STRING(500),
  updated_at TIMESTAMP NOT NULL,
) PRIMARY KEY (commit_id);

CREATE UNIQUE INDEX uniq_reception_shard_file ON file_receptions(shard_key, file_id);
