-- インテグレーションの作成
CREATE OR REPLACE NOTIFICATION INTEGRATION streaming_transaction_integration
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/fivetran-handson-389804/subscriptions/streaming_transaction';

-- インテグレーションの詳細の確認
DESC INTEGRATION streaming_transaction_integration;

-- パイプの作成
CREATE OR REPLACE PIPE transaction_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = streaming_transaction_integration
AS
  COPY INTO HANDM.PUBLIC.TRANSACTION (T_DAT, CUSTOMER_ID, ARTICLE_ID,PRICE,SALES_CHANNEL_ID)
  FROM (
    SELECT
      $1:t_dat::STRING AS T_DAT,
      $1:customer_id::STRING AS CUSTOMER_ID,
      $1:article_id::int AS article_id,
      $1:price::float AS PRICE ,
      $1:sales_channel_id::int AS SALES_CHANNEL_ID
      
    FROM @STOREAMING_TRANSACTION
  )
  FILE_FORMAT = (TYPE = 'JSON');

--debug
-- SELECT T_DAT , customer_id FROM HANDM.PUBLIC.TRANSACTION
-- WHERE T_DAT = '2023-06-17';