-- 使うデータベースの指定
USE DATABASE HANDM;

--以下テーブル構築
CREATE OR REPLACE TABLE transaction (
  t_dat DATE,
  customer_id STRING,
  article_id int,
  price FLOAT,
  sales_channel_id int
);

CREATE OR REPLACE TABLE customer (
  customer_id STRING,
  FN FLOAT,
  Active FLOAT,
  club_member_status string,
  fashion_news_frequency string,
  age int,
  postal_code string
);

CREATE OR REPLACE TABLE articles (
  article_id int,
  product_code int,
  prod_name string,
  product_type_no int,
  product_type_name string,
  product_group_name string,
  graphical_appearance_no string,
  graphical_appearance_name	STRING,
  colour_group_code	INTEGER,			
  colour_group_name	STRING	,		
  perceived_colour_value_id	INTEGER	,
  perceived_colour_value_name	STRING,		
  perceived_colour_master_id	INTEGER,			
  perceived_colour_master_name	STRING	,
  department_no	INTEGER,		
  department_name	STRING,			
  index_code	STRING,
  index_name	STRING,					
  index_group_no	INTEGER,					
  index_group_name	STRING,			
  section_no	INTEGER,		
  section_name	STRING,			
  garment_group_no	INTEGER,					
  garment_group_name	STRING,				
  detail_desc	varchar(16777216))