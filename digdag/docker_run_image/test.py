import pandas as pd
def main_function():
  df = pd.read_csv("0525campaign_master.csv")
  return df

if __name__ == "__main__":
  main_function()
