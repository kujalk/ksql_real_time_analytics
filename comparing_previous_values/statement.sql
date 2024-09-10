
CREATE STREAM synthetic_transactions_stream (
    server VARCHAR,
    transactiontime BIGINT,
    testsource VARCHAR
) WITH (
    KAFKA_TOPIC = 'synthetic_transactions',
    partitions=1,
    VALUE_FORMAT = 'JSON'
);

---
CREATE TABLE synthetic_transactions_last_two
WITH (kafka_topic='synthetic_transactions_last_two', partitions=1, value_format='JSON',key_format='json') AS
SELECT
    server,
    testsource,
    LATEST_BY_OFFSET(transactiontime) latest,
    LATEST_BY_OFFSET(transactiontime,2)[1] previous
FROM synthetic_transactions_stream
GROUP BY server,testsource;

---
CREATE TABLE synthetic_transactions_diff
WITH (kafka_topic='synthetic_transactions_diff', partitions=1, value_format='JSON') AS
select *
from synthetic_transactions_last_two
where latest-previous > 200;

---
SELECT * 
FROM synthetic_transactions_diff
EMIT CHANGES;

---
drop table if exists synthetic_transactions_diff delete TOPIC;
drop table synthetic_transactions_last_two delete TOPIC;
drop stream synthetic_transactions_stream delete TOPIC;