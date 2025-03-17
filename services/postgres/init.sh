#!/bin/bash

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    
    CREATE TABLE IF NOT EXISTS $TABLE (
        id serial primary key,
        generator_id varchar(50),
        messages text,
        created_at timestamp,
        inserted_at timestamp default now(),
        updated_at timestamp default now()
        );
    ALTER TABLE $TABLE REPLICA IDENTITY FULL;
    CREATE OR REPLACE function public.update_timestamp()
        returns trigger
        language plpgsql
        as \$\$
        begin
        if new.updated_at = old.updated_at then
            new.updated_at = NOW();
        end if;
        return NEW;
        end;
        \$\$;
    CREATE TRIGGER update_timestamp 
        BEFORE update on $TABLE for each row
        EXECUTE procedure update_timestamp();

    CREATE USER sequin_user;
    GRANT CONNECT on database $POSTGRES_DB TO sequin_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO sequin_user;

    ALTER USER sequin_user WITH REPLICATION;
    CREATE PUBLICATION $PUBLICATION_NAME FOR ALL TABLES;
    SELECT * FROM pg_create_logical_replication_slot('$REPLICATION_SLOT_NAME', 'pgoutput',  false, true);
EOSQL
    # CREATE ROLE replication_role WITH REPLICATION LOGIN PASSWORD '$REPLICATION_PASSWORD';
    # GRANT USAGE ON SCHEMA public TO replication_role;