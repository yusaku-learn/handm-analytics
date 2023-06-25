# ベースイメージを指定します
FROM python:3.9-slim-buster

# 作業ディレクトリを設定します
WORKDIR /app

# 必要なパッケージをインストールします
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y libgl1-mesa-dev

# ホストのrequirements.txtをコンテナの作業ディレクトリにコピーします
COPY requirements.txt .

# 必要なPythonパッケージをインストールします
RUN pip install --no-cache-dir -r requirements.txt

# ホストのコードをコンテナの作業ディレクトリにコピーします
COPY . .

# ポート番号を指定します（必要に応じて変更してください）
EXPOSE 8080

# コンテナ内での実行コマンドを指定します
CMD ["python", "app.py"]

