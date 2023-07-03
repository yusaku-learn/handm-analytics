MERGE INTO customer AS target
USING TEMP_CUSTOMER_TABLE AS source
ON (target.customer_id = source.customer_id)
WHEN MATCHED THEN
  UPDATE SET
    target.customer_id = source.customer_id,
    target.FN = source.FN,
    target.ACTIVE = source.ACTIVE,
    target.FASHION_NEWS_FREQUENCY = source.FASHION_NEWS_FREQUENCY,
    target.CLUB_MEMBER_STATUS = source.CLUB_MEBMER_STATUS,
    target.POSTAL_CODE = source.POSTAL_CODE
WHEN NOT MATCHED THEN
  INSERT (
    CUSTOMER_ID,
    FN,
    ACTIVE,
    CLUB_MEMBER_STATUS,
    FASHION_NEWS_FREQUENCY,
    AGE,
    POSTAL_CODE
  )
  VALUES (
    source.CUSTOMER_ID,
    source.FN,
    source.ACTIVE,
    source.CLUB_MEBMER_STATUS,
    source.FASHION_NEWS_FREQUENCY,
    source.AGE,
    source.POSTAL_CODE
  );
