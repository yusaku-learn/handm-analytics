--統合用のサービスアカウントを作成
CREATE OR REPLACE STORAGE INTEGRATION gcs_handm
TYPE = external_stage
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://handmdataset/')

DESC STORAGE INTEGRATION gcs_handm;

--ステージング処理
CREATE OR REPLACE stage handm_stage
url = 'gcs://handmdataset/'
storage_integration = gcs_handm;

show stages;

--ステージングのデータを確認
list @handm_stage;


--ストリーミングデータ用のバケットのステージング
CREATE OR REPLACE STORAGE INTEGRATION storeaming_transaction
TYPE = external_stage
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://streaming_transaction/')

DESC STORAGE INTEGRATION storeaming_transaction;

--ステージング処理
CREATE OR REPLACE stage storeaming_transaction
url = 'gcs://streaming_transaction'
storage_integration = storeaming_transaction;

show stages;

--ステージングのデータを確認
list @storeaming_transaction;


--ストリーミング用のarticleステージング作成
CREATE OR REPLACE STORAGE INTEGRATION streaming_articles
TYPE = external_stage
STORAGE_PROVIDER= GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://streaming_articles/');

DESC STORAGE INTEGRATION streaming_articles;

--ステージング処理
CREATE OR REPLACE stage storeaming_articles
url = 'gcs://streaming_articles'
storage_integration = streaming_articles;


--ストリーミング用のcustomerステージング作成
CREATE OR REPLACE STORAGE INTEGRATION streaming_customer
TYPE = external_stage
STORAGE_PROVIDER= GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://streaming_customer');

DESC STORAGE INTEGRATION streaming_customer;

--ステージング処理
CREATE OR REPLACE stage streaming_customer
url = 'gcs://streaming_customer'
storage_integration = streaming_customer;
