import pandas as pd
import fsspec
import gcsfs
import random
import datetime
from mlxtend.frequent_patterns import association_rules
from mlxtend.frequent_patterns import apriori

age_list = [20,30,40,50,60,70,80]
random_dict = {}


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    base_object = "gs://"
    bucket_and_object_name = file["id"].split("/")
    object_url = base_object +"/"+bucket_and_object_name[0]+ "/"+bucket_and_object_name[1]

    df_customer = pd.read_csv(object_url)
    df_transaction = pd.read_csv("gs://handmdataset/transactions_train.csv")

    df_customer , df_transaction

def sampling_customer_sum(age):
  # 年代ごとの割合を計算
  if age == 20:
    teenagers_df = customer_df[(customer_df['age'] < age)]
    teenagers_percentage = len(teenagers_df) / len(customer_df) * 100
    sampling_customer = int(20000 * (teenagers_percentage/100))
  elif age == 80:
    over_df = customer_df[(customer_df['age'] > age)]
    over_percentage = len(over_df) / len(customer_df) * 100
    sampling_customer = int(20000 * (over_percentage/100))
  else:
    middle_df = customer_df[(customer_df['age'] >= (age-10)) & (customer_df['age'] < age)]
    over_percentage = len(middle_df) / len(customer_df) * 100
    sampling_customer = int(20000 * (over_percentage/100))

  return sampling_customer


def sampling_customer():
  for i,age in enumerate(age_list):
    if age == 20:
      age_sum = sampling_customer_sum(age)
      # データフレームから条件に合致する行を抽出
      filtered_df = customer_df[customer_df["age"] < age]
      # ランダムな行のインデックスを抽出
      random_indices = random.sample(list(filtered_df.index), age_sum)
      random_dict[i] = random_indices
    elif age < 80:
      age_sum = sampling_customer_sum(age)
      # データフレームから条件に合致する行を抽出
      filtered_df = customer_df[customer_df["age"] < age]

      # ランダムな行のインデックスを抽出
      random_indices = random.sample(list(filtered_df.index), age_sum)

      random_dict[i] = random_indices
    else :
      age_sum = sampling_customer_sum(age)
      # データフレームから条件に合致する行を抽出
      filtered_df = customer_df[(age - 10 <= customer_df["age"]) & (customer_df["age"] < age)]
      # ランダムな行のインデックスを抽出
      random_indices = random.sample(list(filtered_df.index), age_sum)
      random_dict[i] = random_indices

  merged_list = [item for sublist in random_dict.values() for item in sublist]

  # df1からdfのcustomer_idに該当する記録を抽出
  filtered_records = customer_df[customer_df.index.isin(merged_list)]
  # dfのcustomer_idをリストとして取得
  XXX = filtered_records['customer_id'].tolist()
  # df1からdfのcustomer_idに該当する記録を抽出
  filtered_df = transaction_df[transaction_df['customer_id'].isin(XXX)]

def arociation_model():
  # 最新の日付を取得
  latest_date_str = filtered_df["t_dat"].max()
  # 年月日形式の文字列を日付オブジェクトに変換
  latest_date = datetime.datetime.strptime(latest_date_str, "%Y-%m-%d")
  # 1年前の日付を計算
  one_year_ago = latest_date - datetime.timedelta(days=30)
  # 結果を年月日形式の文字列として取得
  one_year_ago_str = one_year_ago.strftime("%Y-%m-%d")
  one_year_latest = filtered_df[filtered_df["t_dat"] >=one_year_ago_str]
  # クロス集計の作成
  cross_table = pd.crosstab(filtered_df['customer_id'], filtered_df['article_id'])
  # 1つでも買っていれば1そうでなければ0
  cross_table = cross_table.applymap(lambda x: 1 if x != 0 else 0)
  cross_table = cross_table.astype('object')
  freq_article = apriori(cross_table, min_support=0.001, use_colnames=True)
  freq_article.sort_values("support", ascending=False)
  rules = association_rules(freq_article , metric="lift",min_threshold=0.001)
  
  rules.sort_values("lift",ascending=False)[["antecedents",	"consequents","lift"]].to_csv("gs://cloud_function_lift/rule.csv")
