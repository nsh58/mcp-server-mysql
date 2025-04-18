import os
import sys
import json
import argparse
from typing import Dict, List, Any, Tuple, Optional

import mysql.connector
from dotenv import load_dotenv, find_dotenv
from loguru import logger
from mcp.server.fastmcp import FastMCP
import mcp.types as types

DEFAULT_MAX_ROWS = 100
DEFAULT_PORT = 8888
DEFAULT_MYSQL_PORT = 13306
DEFAULT_MAX_LENGTH = 10000  # 出力テキストの最大文字数

logger.remove()
logger.add(sys.stderr, level=os.getenv("FASTMCP_LOG_LEVEL", "WARNING"))
logger = logger.bind(module="mysql_mcp")

# 環境変数を読み込む関数
def load_env_vars(keys: List[str]) -> Dict[str, str]:
    """必要な環境変数を読み込む

    Args:
        keys: 必要な環境変数のリスト

    Returns:
        環境変数の辞書

    Raises:
        ValueError: 必要な環境変数が設定されていない場合
    """
    load_dotenv(find_dotenv())
    values = {key: os.getenv(key) for key in keys}
    missing = [k for k, v in values.items() if not v]
    
    if missing:
        raise ValueError(f"以下の環境変数が不足しています: {', '.join(missing)}")
    
    return values

# データベース接続とカーソルを取得する関数
def get_connection() -> Tuple[mysql.connector.connection.MySQLConnection, mysql.connector.cursor.MySQLCursor]:
    """データベース接続とカーソルを取得する

    Returns:
        接続オブジェクトとカーソルのタプル
    """
    env = load_env_vars([
        "MYSQL_USER", 
        "MYSQL_PASSWORD", 
        "MYSQL_HOST", 
        "MYSQL_DATABASE"
    ])
    
    port = int(os.getenv("MYSQL_PORT", DEFAULT_MYSQL_PORT))
    
    conn = mysql.connector.connect(
        user=env["MYSQL_USER"],
        password=env["MYSQL_PASSWORD"],
        host=env["MYSQL_HOST"],
        database=env["MYSQL_DATABASE"],
        port=port
    )
    
    return conn, conn.cursor(dictionary=True)

# SQLクエリを実行して結果を取得する関数
def execute_query(query: str, max_rows: int = DEFAULT_MAX_ROWS) -> Tuple[List[Dict[str, Any]], bool]:
    """SQLクエリを実行して結果を取得する

    Args:
        query: 実行するSQLクエリ
        max_rows: 最大取得行数

    Returns:
        クエリ結果の辞書のリストと追加データの有無を示すブール値のタプル

    Raises:
        Exception: DB接続またはクエリ実行エラー
    """
    conn, cursor = None, None
    
    try:
        conn, cursor = get_connection()
        cursor.execute(query)

        # INSERT、UPDATE、DELETE、CREATEなどのデータ変更クエリの場合はコミットする
        if any(query.strip().upper().startswith(cmd) for cmd in ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]):
            conn.commit()

        results = cursor.fetchmany(size=max_rows)
        
        # 最大行数を超えた場合の追加データが存在するかをチェック
        more_rows_exist = cursor.fetchone() is not None
            
        return results, more_rows_exist
        
    finally:
        # 接続のクリーンアップ
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# クエリ結果を読みやすい形式にフォーマットする関数
def format_query_result(query: str, results: List[Dict[str, Any]], max_length: int = DEFAULT_MAX_LENGTH) -> str:
    """クエリ結果を読みやすい形式にフォーマットする

    Args:
        query: 実行されたSQLクエリ
        results: クエリ結果
        max_length: 出力テキスト全体の最大文字数

    Returns:
        フォーマットされた結果文字列
    """
    output_prefix = f"実行されたクエリ: {query}\n\n"
    
    # 特殊なメッセージが含まれているか確認
    truncation_message = None
    data_results = results.copy()
    more_rows_exist = False
    
    for i, row in enumerate(results):
        if "message" in row and len(row) == 1:
            truncation_message = row["message"]
            data_results.pop(i)
            more_rows_exist = True
            break
    
    # 結果がない場合
    if not data_results:
        return output_prefix + "結果: 該当するデータはありませんでした。"
    
    # JSON形式で結果を構築
    json_results = []
    for row in data_results:
        record = {}
        for header, value in row.items():
            try:
                # バイナリデータの場合はデコード
                if isinstance(value, bytes):
                    record[header] = value.decode('utf-8')
                else:
                    record[header] = value
            except Exception as e:
                record[header] = f"<表示エラー: {str(e)}>"
        json_results.append(record)
    
    # 行数制限メッセージを追加
    if more_rows_exist:
        json_results.append({
            "message": "(行数制限により以降は省略されています。max_rows を増やして再実行してください)"
        })
    
    # 結果を文字列に変換
    result_str = json.dumps(json_results, ensure_ascii=False, indent=2)
    
    # 合計件数情報
    count_info = f"\n\n合計 {len(data_results)} 件のデータが見つかりました。"
    
    complete_output = output_prefix + result_str + count_info
    
    # 文字数制限をチェック
    if len(complete_output) > max_length:
        # 結果部分を制限内に切り詰める
        available_space = max_length - len(output_prefix) - len(count_info) - 100  # 省略メッセージ用に余裕を持たせる
        truncated_result = result_str[:available_space]
        
        # 最後の完全なJSONオブジェクトを探し、それ以降を削除
        last_bracket = truncated_result.rfind('}')
        if last_bracket != -1:
            truncated_result = truncated_result[:last_bracket + 1]
            
        # 省略メッセージをJSON構造で追加
        truncated_result = truncated_result + ',\n  {\n    "message": "(文字数制限により以降は省略。より多くの結果を表示するには別のクエリを使用してください。)"\n  }\n]'
        
        return output_prefix + truncated_result + count_info
    
    return complete_output

# MCP サーバーの設定と実行
def init_mcp_server():
    """MySQLへのクエリ実行をサポートするMCPサーバーを初期化"""
    mcp = FastMCP("MYSQL_MCP_SERVER")
    
    @mcp.tool(
        name="execute_mysql",
        description="""
        MySQL に対して SQL クエリを実行し、結果を読みやすい形式で返します。

        Args:
            query: 実行する SQL クエリ
            max_rows: 最大取得行数（省略時は100）
        
        注意:
            クエリが大量の行を返す場合、末尾に省略メッセージが付与されます。
        """
    )
    def execute_mysql(query: str, max_rows: int = DEFAULT_MAX_ROWS) -> str:
        """MySQLクエリを実行するMCPツール

        Args:
            query: 実行するSQLクエリ
            max_rows: 最大取得行数

        Returns:
            フォーマット済みのクエリ結果
        """
        logger.debug("SQLクエリを実行しています")
        
        try:
            results, more_rows_exist = execute_query(query, max_rows)
            
            # 結果をJSONフォーマットで返す
            json_results = []
            for row in results:
                json_results.append(row)
                
            # 行数制限メッセージを追加
            if more_rows_exist:
                json_results.append({
                    "message": "(行数制限により以降は省略されています。max_rows を増やして再実行してください)"
                })
                
            formatted_result = format_query_result(query, json_results)
            return [types.TextContent(type="text", text=formatted_result)]
            
        except Exception as e:
            error_message = f"エラー: {str(e)}"
            logger.error(error_message)
            return [types.TextContent(type="text", text=error_message)]
    
    return mcp

def run_server(mcp, transport: str = "stdio", port: int = DEFAULT_PORT):
    """MCPサーバーを起動する

    Args:
        mcp: 設定済みのMCPオブジェクト
        transport: 使用するトランスポート ("stdio" または "sse")
        port: SSEトランスポート使用時のポート番号
    """
    if transport == "sse":
        logger.info(f"SSEトランスポートを使用: ポート {port}")
        mcp.settings.port = port
        mcp.run(transport="sse")
    else:
        logger.info("stdioトランスポートを使用")
        mcp.run()

def main():
    """メインエントリポイント"""
    parser = argparse.ArgumentParser(description="MySQL MCP Server")
    parser.add_argument("--sse", action="store_true", help="SSEトランスポートで起動")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="SSEポート番号")
    args = parser.parse_args()
    
    logger.info("MySQL MCPサーバーを起動中")
    
    mcp = init_mcp_server()
    transport = "sse" if args.sse else "stdio"
    run_server(mcp, transport=transport, port=args.port)

if __name__ == "__main__":
    main()