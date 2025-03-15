ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
SELECT * FROM pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
CREATE PUBLICATION debezium_publication FOR ALL TABLES;
