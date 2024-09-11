CREATE TABLE synthetic_transactions_anomalies 
WITH (kafka_topic='synthetic_transactions_anomalies', partitions=1, value_format='JSON',key_format='json') AS
SELECT
    server,
    testsource,
    STDDEV_SAMPLE(transactiontime) AS stddev,
    AVG(transactiontime) AS avg_transactiontime,
    AVG(transactiontime) - STDDEV_SAMPLE(transactiontime) AS lower_bound,
    AVG(transactiontime) + STDDEV_SAMPLE(transactiontime) AS upper_bound,
    COUNT(*) AS count
FROM synthetic_transactions_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY server, testsource;

-----------
CREATE TABLE synthetic_transactions_anomalies 
WITH (kafka_topic='synthetic_transactions_anomalies', partitions=1, value_format='JSON',key_format='json') AS
SELECT
    server,
    testsource,
    LATEST_BY_OFFSET(transactiontime) AS transactiontime,
    STDDEV_SAMPLE(transactiontime) AS stddev,
    AVG(transactiontime) AS avg_transactiontime,
    AVG(transactiontime) - STDDEV_SAMPLE(transactiontime) AS lower_bound,
    AVG(transactiontime) + STDDEV_SAMPLE(transactiontime) AS upper_bound,
    COUNT(*) AS count
FROM synthetic_transactions_stream
WINDOW HOPPING (SIZE 1 MINUTE, ADVANCE BY 30 SECONDS)
GROUP BY server, testsource

---
CREATE TABLE synthetic_transactions_anomalies 
WITH (kafka_topic='synthetic_transactions_anomalies', partitions=1, value_format='JSON', key_format='json') AS
SELECT
    server,
    testsource,
    LATEST_BY_OFFSET(transactiontime) AS transactiontime,
    CASE
        WHEN LATEST_BY_OFFSET(transactiontime) < (AVG(transactiontime) - STDDEV_SAMPLE(transactiontime))
          OR LATEST_BY_OFFSET(transactiontime) > (AVG(transactiontime) + STDDEV_SAMPLE(transactiontime)) 
        THEN TRUE
        ELSE FALSE
    END AS abnormal
FROM synthetic_transactions_stream
WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 3 SECONDS)
GROUP BY server, testsource;
---

drop table synthetic_transactions_anomalies delete topic;

SELECT * 
FROM synthetic_transactions_anomalies
EMIT CHANGES;