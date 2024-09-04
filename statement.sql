CREATE TABLE suspicious_names (CREATED_TS VARCHAR,
                               COMPANY_NAME VARCHAR PRIMARY KEY,
                               COMPANY_ID INT)
    WITH (kafka_topic='suspicious_names',
          partitions=1,
          value_format='JSON',
          timestamp='CREATED_TS',
          timestamp_format = 'yyyy-MM-dd HH:mm:ss');
---

INSERT INTO suspicious_names (CREATED_TS, COMPANY_NAME, COMPANY_ID) VALUES (FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() - (5 * 24 * 60 * 60 * 1000)),'yyyy-MM-dd HH:mm:ss'), 'Verizon', 1);
INSERT INTO suspicious_names (CREATED_TS, COMPANY_NAME, COMPANY_ID) VALUES (FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() - (4 * 24 * 60 * 60 * 1000)),'yyyy-MM-dd HH:mm:ss'), 'Spirit Halloween', 2);
INSERT INTO suspicious_names (CREATED_TS, COMPANY_NAME, COMPANY_ID) VALUES (FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() - (3 * 24 * 60 * 60 * 1000)),'yyyy-MM-dd HH:mm:ss'), 'Best Buy', 3);
---

CREATE STREAM transactions (TXN_ID BIGINT, USERNAME VARCHAR, RECIPIENT VARCHAR, AMOUNT DOUBLE, TS VARCHAR)
    WITH (kafka_topic='transactions',
          partitions=1,
          value_format='JSON',
          timestamp='TS',
          timestamp_format = 'yyyy-MM-dd HH:mm:ss');
---

CREATE STREAM suspicious_transactions
    WITH (kafka_topic='suspicious_transactions', partitions=1, value_format='JSON') AS
    SELECT T.TXN_ID, T.USERNAME, T.RECIPIENT, T.AMOUNT, T.TS
    FROM transactions T
    INNER JOIN
    suspicious_names S
    ON T.RECIPIENT = S.COMPANY_NAME;
---

SELECT TXN_ID, USERNAME, RECIPIENT, AMOUNT
FROM suspicious_transactions
EMIT CHANGES;
