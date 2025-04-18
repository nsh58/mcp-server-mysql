import mysql.connector
import os
from dotenv import load_dotenv, find_dotenv
import json
from mcp.server.fastmcp import FastMCP
import mcp.types as types

# デフォルト設定
DEFAULT_MAX_ROWS = 100

def get_db_connection():
    """
    MySQL データベース接続を確立し、接続オブジェクトとカーソルを返す関数
    """
    # 環境変数を読み込む
    load_dotenv(find_dotenv())
    
    # 必要な環境変数のリスト
    required_env_vars = [
        "MYSQL_USER",
        "MYSQL_PASSWORD",
        "MYSQL_HOST",
        "MYSQL_DATABASE"
    ]
    
    # 環境変数の存在確認
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"以下の環境変数が設定されていません: {', '.join(missing_vars)}")
    
    # 環境変数から設定を読み込む
    USERNAME = os.getenv("MYSQL_USER")
    PASSWORD = os.getenv("MYSQL_PASSWORD")
    HOST = os.getenv("MYSQL_HOST")
    DATABASE = os.getenv("MYSQL_DATABASE")
    PORT = os.getenv("MYSQL_PORT", 13306)  # Docker用に13306をデフォルトに
    
    try:
        # データベース接続
        db_connection = mysql.connector.connect(
            user=USERNAME,
            password=PASSWORD,
            host=HOST,
            database=DATABASE,
            port=int(PORT)
        )
        cursor = db_connection.cursor(dictionary=True)  # 辞書形式で結果を取得
        
        return db_connection, cursor
    
    except mysql.connector.Error as e:
        raise ValueError(f"データベース接続エラー: {str(e)}")

mcp = FastMCP("MYSQL")

@mcp.tool(
    name = "execute_mysql",
    description = """
    MySQL Databaseに対してSELECTクエリを実行し、結果を返す。
        Args:
            query: 実行するSQLクエリ（必須）
            max_rows: 取得する最大行数（integer型、デフォルト: 100）
    """)
def execute_mysql(query: str, max_rows: int = DEFAULT_MAX_ROWS) -> str:
    try:
        # データベース接続
        db_connection, cursor = get_db_connection()
        
        try:
            # クエリを実行
            cursor.execute(query)
            
            # 指定された行数を取得
            results = cursor.fetchmany(max_rows)
            
            # 結果をJSON形式に変換
            result_json = json.dumps(results, ensure_ascii=False, default=str, indent=2)
            
            # 追加データがあるかを確認
            if cursor.fetchone() is not None:
                result_with_note = json.loads(result_json)
                result_with_note.append({"message": "（行数制限により以降は省略）"})
                result_json = json.dumps(result_with_note, ensure_ascii=False, indent=2)
            
            return [types.TextContent(type="text", text=result_json)]
            
        finally:
            # リソースの解放
            cursor.close()
            db_connection.close()
            
    except Exception as e:
        return [types.TextContent(type="text", text=f"エラーが発生しました: {str(e)}")]

if __name__ == "__main__":
    # stdioで通信
    mcp.run(transport="stdio")
