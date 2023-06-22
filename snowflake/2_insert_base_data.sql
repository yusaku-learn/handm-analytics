--どのデータベースを使うかを指定
USE DATABASE HANDM;

--各種テーブルの作成
COPY INTO TRANSACTION
    FROM @handm_stage
    FILE_FORMAT=(TYPE = CSV FIELD_DELIMITER=',',SKIP_HEADER=1)
    PATTERN=".*transaction.*";

COPY INTO customer
    FROM @handm_stage
    FILE_FORMAT=(TYPE = CSV FIELD_DELIMITER=',',SKIP_HEADER=1)
    PATTERN=".*customer.*";

--ON_ERROR:追加しない場合、区切り文字がおかしいとのエラーが出てくるが、CONTINUEを指定することでエラーを無視して行を追加することが可能
--特に影響はないのでOK
COPY INTO articles
    FROM @handm_stage
    FILE_FORMAT=(TYPE = CSV FIELD_DELIMITER=',',SKIP_HEADER=1)
    PATTERN=".*articles_hm.*"
    ON_ERROR = 'CONTINUE';


COPY INTO articles
FROM @handm_stage
FILE_FORMAT=(TYPE = CSV FIELD_DELIMITER=',' RECORD_DELIMITER='\n' SKIP_HEADER=1)
PATTERN=".articles_hm.";

--debug
-- select * from articles
-- LIMIT 10;