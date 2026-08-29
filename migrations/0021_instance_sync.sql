-- Durable state for direct, peer-to-peer synchronization. Authorized credentials use the existing
-- encrypted credentials table and peer-specific mappings; vault master keys, runtime work, PTYs,
-- leases, and operation queues intentionally remain local.

CREATE TABLE instance_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    instance_id TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE instance_sync_peers (
    id TEXT PRIMARY KEY,
    peer_instance_id TEXT,
    display_name TEXT NOT NULL UNIQUE,
    endpoint TEXT NOT NULL,
    outbound_credential_id TEXT NOT NULL REFERENCES credentials(id),
    inbound_token_sha256 TEXT NOT NULL,
    allowed_collections_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    last_pushed_at TEXT,
    last_pulled_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE instance_sync_receipts (
    origin_instance_id TEXT NOT NULL,
    collection_json TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (origin_instance_id, collection_json, entity_type, entity_key, payload_sha256)
);

CREATE TABLE instance_sync_entity_mappings (
    origin_instance_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    remote_entity_key TEXT NOT NULL,
    local_entity_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (origin_instance_id, entity_type, remote_entity_key)
);

CREATE TABLE instance_sync_conflicts (
    id TEXT PRIMARY KEY,
    origin_instance_id TEXT NOT NULL,
    collection_json TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    local_updated_at TEXT NOT NULL,
    remote_updated_at TEXT NOT NULL,
    local_payload_sha256 TEXT NOT NULL,
    remote_payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX instance_sync_receipts_origin_lookup
ON instance_sync_receipts(origin_instance_id, collection_json, entity_type, entity_key);
