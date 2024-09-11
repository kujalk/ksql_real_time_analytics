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
    COUNT(*) AS count,
    CASE
        WHEN LATEST_BY_OFFSET(transactiontime) < (AVG(transactiontime) - STDDEV_SAMPLE(transactiontime))
          OR LATEST_BY_OFFSET(transactiontime) > (AVG(transactiontime) + STDDEV_SAMPLE(transactiontime)) 
        THEN TRUE
        ELSE FALSE
    END AS abnormal
FROM synthetic_transactions_stream
WINDOW SESSION (15 MINUTES)
GROUP BY server, testsource;