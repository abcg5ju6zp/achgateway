ALTER TABLE files ADD COLUMN published_at TIMESTAMP NULL;

CREATE TABLE file_receptions(
       commit_id VARCHAR(128) PRIMARY KEY,
       shard_name VARCHAR(100) NOT NULL,
       shard_key VARCHAR(50) NOT NULL,
       file_id VARCHAR(128) NOT NULL,
       source VARCHAR(20) NOT NULL,
       hostname VARCHAR(100) NOT NULL,
       stage VARCHAR(20) NOT NULL,
       accepted_at TIMESTAMP NOT NULL,
       staged_at TIMESTAMP NULL,
       published_at TIMESTAMP NULL,
       notified_at TIMESTAMP NULL,
       cancel_requested_at TIMESTAMP NULL,
       content_sha256 CHAR(64) NULL,
       result_json MEDIUMTEXT NULL,
       last_error VARCHAR(500) NULL,
       updated_at TIMESTAMP NOT NULL,
       UNIQUE KEY uniq_reception_shard_file (shard_key, file_id)
);
