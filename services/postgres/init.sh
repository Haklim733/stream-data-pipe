#!/bin/bash

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE replication_role WITH REPLICATION LOGIN PASSWORD '$REPLICATION_PASSWORD';
    GRANT USAGE ON SCHEMA public TO replication_role;
    SELECT * FROM pg_create_logical_replication_slot('$REPLICATION_SLOT_NAME', 'pgoutput',  false, true);
    CREATE PUBLICATION $PUBLICATION_NAME FOR ALL TABLES;
EOSQL