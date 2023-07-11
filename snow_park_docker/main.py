from snowflake.snowpark.session import Session
import json
import datetime

def check_quarter(quarter):
    if quarter == 1:
        return 4
    else:
        return quarter - 1
    
def check_year(quarter,year):
    if quarter == 1:
        return year - 1
    
    else:
        return year

def main():
    # credention情報を入力
    snowflake_connection_cfg = json.loads(open('./snowflake_connection.json').read())
    session = Session.builder.configs(snowflake_connection_cfg).create()
    
    # HANDMデータベースを使用することを指定
    query_use_database = '''
    use database HANDM
    '''
    session.sql(query_use_database).collect()
    
    now = datetime.datetime.now()
    quarter = (now.month - 1) // 3 + 1
    year = now.year
    month = now.month
    
    # テーブルの作成
    query_create_table = f'''
    create or replace task HANDM.PUBLIC.CREATE_transaction_quarter_TABLE
    warehouse=COMPUTE_WH
    schedule='USING CRON 00 2 1 1,4,7,10 * Asia/Tokyo'
    as
        create or replace TABLE HANDM.PUBLIC.TRANSACTION_{year}_{quarter} (
        T_DAT string,
        CUSTOMER_ID string,
        article_id int,
        PRICE float ,
        SALES_CHANNEL_ID int
    );
    '''
    session.sql(query_create_table).collect()
    
    # transactionデータを挿入する場所の変更
    # 四半期当日の午前２時に実施
    query_change_into = f'''
    create or replace task HANDM.PUBLIC.CHANGE_INTO_TABLE
    warehouse=COMPUTE_WH
    schedule='USING CRON 00 2 1 1,4,7,10 * Asia/Tokyo'
    as
    create or replace pipe HANDM.PUBLIC.TRANSACTION_PIPE auto_ingest=true integration='STREAMING_TRANSACTION_INTEGRATION' as COPY INTO
    HANDM.PUBLIC.TRANSACTION_{year}_{quarter} (T_DAT, CUSTOMER_ID, ARTICLE_ID,PRICE,SALES_CHANNEL_ID)
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
    '''
    session.sql(query_change_into).collect()
    
    # アーカイブ用のバケットを作成(ステージングも行う)
    # 四半期の最後に実施(schedule)

    query_export_csv = f'''
    create or replace task HANDM.PUBLIC.export_table_transaction
    warehouse=COMPUTE_WH
    schedule='USING CRON 00 2 1 1,4,7,10 * Asia/Tokyo'
    as
    COPY INTO @ARCHIVE_TRANSACTION/{check_year(quarter,year)}/{check_year(quarter,year)}_{check_quarter(quarter)}
    FROM TRANSACTION_{check_year(quarter,year)}_{check_quarter(quarter)}
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',');
    '''
    session.sql(query_export_csv).collect()
    
if __name__ == "__main__": 
    main()

    
    
    