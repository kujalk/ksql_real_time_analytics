
CREATE STREAM synthetic_transactions_stream (
    server VARCHAR,
    transactiontime BIGINT,
    testsource VARCHAR
) WITH (
    KAFKA_TOPIC = 'synthetic_transactions',
    partitions=1,
    VALUE_FORMAT = 'JSON'
);


CREATE TABLE synthetic_transactions_anomalies 
WITH (kafka_topic='synthetic_transactions_anomalies', partitions=1, value_format='JSON',key_format='json') AS
SELECT
    server as server_key,
    testsource as testsource_key,
    AS_VALUE(server) AS server,
    AS_VALUE(testsource) AS testsource,
    LATEST_BY_OFFSET(transactiontime) AS transactiontime,
    STDDEV_SAMPLE(transactiontime) AS stddev,
    AVG(transactiontime) AS avg_transactiontime,
    AVG(transactiontime) + STDDEV_SAMPLE(transactiontime) AS upper_bound,
    (LATEST_BY_OFFSET(transactiontime) / AVG(transactiontime)) / STDDEV_SAMPLE(transactiontime) AS z_score,
    COUNT(*) AS count,
    CASE
        WHEN LATEST_BY_OFFSET(transactiontime) > AVG(transactiontime) + STDDEV_SAMPLE(transactiontime)
        THEN TRUE
        ELSE FALSE
    END AS anomaly1,
    CASE
        WHEN (LATEST_BY_OFFSET(transactiontime) / AVG(transactiontime)) / STDDEV_SAMPLE(transactiontime) NOT BETWEEN -0.01 AND 0.01 
        AND (LATEST_BY_OFFSET(transactiontime) > AVG(transactiontime))
        THEN TRUE
        ELSE FALSE
    END AS anomaly2
FROM synthetic_transactions_stream
WINDOW SESSION (15 MINUTES)
GROUP BY server, testsource;

SELECT * 
FROM synthetic_transactions_anomalies
EMIT CHANGES;