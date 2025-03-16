# manual verification that changes are being recorded

psql -h localhost -p 5432 -U admin -d stream
SHOW wal_level;
SELECT * FROM pg_publication WHERE pubname = 'cdc';

#check if publication is for all tables
SELECT pubname, puballtables FROM pg_publication WHERE pubname = 'cdc';

SELECT * FROM pg_logical_slot_peek_binary_changes(
  'cdc_slot', 
  NULL, 
  NULL, 
  'proto_version', '1',
  'publication_names', 'cdc'
);