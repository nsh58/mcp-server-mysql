# 設定手順

以下の手順で開発環境をセットアップできます。

① 仮想環境を作成

```
uv venv
```
このコマンドを実行すると、カレントディレクトリに .venv/ フォルダが作成されます。

② 仮想環境をアクティブにする
```
source .venv/bin/activate
```

③ 依存パッケージをインストールする

```
uv pip install -r requirements.txt
```
このコマンドで、プロジェクトに必要なPythonライブラリが一括でインストールされます。

④ LLMアプリの設定ファイルに下記を追記します。

```
{
  "mcpServers": {
    "mysql": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/mcp-server-mysql",
        "run",
        "main.py"
      ],
      "env": {
        "MYSQL_HOST": "${db_host}",
        "MYSQL_PORT": "${db_port}",
        "MYSQL_USER": "${db_user}",
        "MYSQL_PASSWORD": "${db_password}",
        "MYSQL_DATABASE": "${db_name}"
      }
    }
}
```

⑤ LLMアプリを再起動