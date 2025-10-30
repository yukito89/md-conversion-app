import azure.functions as func
import logging
import pandas as pd
from openpyxl import load_workbook
import os
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from openai import AzureOpenAI
import json
import time
import io
from urllib.parse import quote
import re

load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

llm_service = os.getenv("LLM_SERVICE", "AZURE")
azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION")
azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
aws_region = os.getenv("AWS_REGION")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_bedrock_model_id = os.getenv("AWS_BEDROCK_MODEL_ID")

azure_client = None
bedrock_client = None

def validate_env():
    if llm_service == "AZURE":
        if not all([azure_api_key, azure_endpoint, azure_api_version, azure_deployment]):
            raise ValueError("Azure OpenAI の必須環境変数が設定されていません。")
    elif llm_service == "AWS":
        if not all([aws_region, aws_access_key_id, aws_secret_access_key, aws_bedrock_model_id]):
            raise ValueError("AWS Bedrock の必須環境変数が設定されていません。")
    else:
        raise ValueError(f"無効なLLMサービスが指定されました: {llm_service}")

def initialize_client():
    global azure_client, bedrock_client
    validate_env()
    if llm_service == "AZURE":
        azure_client = AzureOpenAI(
            api_version=azure_api_version,
            azure_endpoint=azure_endpoint,
            api_key=azure_api_key,
        )
    elif llm_service == "AWS":
        config = Config(read_timeout=600, connect_timeout=60)
        bedrock_client = boto3.client(
            "bedrock-runtime",
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config,
        )

# LLMサービスを呼び出す共通関数
def call_llm(system_prompt: str, user_prompt: str, max_retries: int = 5) -> str:
    """
    指定されたLLMサービス（AzureまたはAWS）を使ってプロンプトを送信し、応答を取得する。
    system_prompt: システムプロンプト（モデルの振る舞いを定義）
    user_prompt: ユーザーからの入力
    max_retries: 最大リトライ回数
    戻り値: モデルからの応答テキスト
    """
    global azure_client, bedrock_client
    
    # クライアントが未初期化の場合は初期化する
    if llm_service == "AZURE" and azure_client is None:
        initialize_client()
    elif llm_service == "AWS" and bedrock_client is None:
        initialize_client()
    
    for attempt in range(max_retries):
        try:
            if llm_service == "AZURE":
                # Azure OpenAIにチャット形式でリクエストを送信
                response = azure_client.chat.completions.create(
                    model=azure_deployment,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    max_completion_tokens=32768,
                )
                return response.choices[0].message.content

            elif llm_service == "AWS":
                # AWS BedrockにConverse APIでリクエストを送信
                response = bedrock_client.converse(
                    modelId=aws_bedrock_model_id,
                    messages=[{"role": "user", "content": [{"text": user_prompt}]}],
                    system=[{"text": system_prompt}],
                    inferenceConfig={"maxTokens": 64000},
                )
                # レスポンスの構造を確認してから取得
                if 'output' in response and 'message' in response['output']:
                    return response['output']['message']['content'][0]['text']
                else:
                    logging.error(f"予期しないレスポンス構造: {json.dumps(response, ensure_ascii=False)}")
                    raise RuntimeError("AWS Bedrockからの応答形式が不正です。")

        except Exception as e:
            error_message = str(e)
            # ThrottlingExceptionの場合はリトライ
            if "ThrottlingException" in error_message or "Too many requests" in error_message:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + (attempt * 2)  # エクスポネンシャルバックオフ
                    logging.warning(f"{llm_service} API レート制限エラー。{wait_time}秒後にリトライします（{attempt + 1}/{max_retries}）")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"{llm_service} API呼び出しが最大リトライ回数に達しました")
                    raise RuntimeError(f"{llm_service} APIのレート制限エラー。しばらく待ってから再試行してください。")
            else:
                # その他のエラーは即座に失敗
                logging.error(f"{llm_service} API呼び出し中にエラーが発生しました: {error_message}")
                raise RuntimeError(f"{llm_service} API呼び出しに失敗しました: {error_message}")
    
    raise RuntimeError(f"{llm_service} API呼び出しに失敗しました")

def structuring(prompt: str) -> str:
    system_prompt = '''
        あなたはExcelファイルを読みやすいMarkdownドキュメントに変換する専門家です。

        【タスク】
        提供されたExcelシートの内容を、構造化されたMarkdownに変換してください。

        【出力要件】
        - シート名を見出し（## シート名）として使用
        - 表形式のデータはMarkdown表として出力
        - 空行や意味のないデータは除外
        - 見出しや項目名は適切に認識して構造化
        - 数値、日付、テキストは元の形式を保持

        【記述ルール】
        - 出力形式はMarkdownのみ
        - 説明文や補足は不要
        - データをそのまま忠実に変換
        '''
    return call_llm(system_prompt, prompt)

@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    try:
        file = req.files.get("file")
        if not file:
            return func.HttpResponse("ファイルがアップロードされていません", status_code=400)
        
        file_bytes = file.read()
        filename = file.filename
        
        if not filename.endswith('.xlsx'):
            return func.HttpResponse("Excelファイル(.xlsx)のみ対応しています", status_code=400)
            
    except Exception as e:
        logging.error(f"ファイル取得エラー: {e}")
        return func.HttpResponse("ファイルの取得に失敗しました", status_code=400)

    logging.info(f"{filename} を受信しました。単体テスト生成を開始します。")

    try:
        # アップロードされたExcelファイル（バイナリ）をメモリ上で読み込み、全シートを辞書形式で取得
        # すべてのシートが {シート名: DataFrame} の形式で格納される
        # header=Noneで全行をデータとして読み込み、_clean_sheet()でヘッダーを設定
        excel_data = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, header=None)

        # Markdown構造化のためのリスト初期化
        toc_list = [] # 目次(Table of Contents)用のリスト
        md_sheets = [] # 各シートのmd文字列を格納するリスト

        # --- 各シートの処理 ---
        for sheet_name, df in excel_data.items():
            # 目次用のアンカーを生成 (GitHub-flavored)
            anchor = re.sub(r'[^a-z0-9-]', '', sheet_name.strip().lower().replace(' ', '-'))
            toc_list.append(f'- [{sheet_name}](#{anchor})')
            
            sheet_content = f"## {sheet_name}\n\n"

            # --- すべてのシートをAIで構造化 ---
            logging.info(f"「{sheet_name}」シートをAIで構造化します。")
            try:
                # DataFrameを行ごとにテキスト化（セル区切りを明示）
                raw_text = '\n'.join(df.apply(lambda row: ' | '.join(row.astype(str).fillna('')), axis=1))
                
                structuring_prompt = f'''
                    --- Excelシート「{sheet_name}」 ---
                    {raw_text}
                '''
                structured_content = structuring(structuring_prompt)
                sheet_content += structured_content
                
            except Exception as e:
                logging.error(f"AIによるシート構造化中にエラー: {e}")
                sheet_content += "（AIによる構造化に失敗しました）"
            
            md_sheets.append(sheet_content)

        # --- 1. 全体を結合して最終的なMarkdown設計書を生成 ---
        logging.info("全シートの処理が完了。最終的な設計書を組み立てます。")
        md_output_first = f"# {filename}\n\n"
        md_output_first += "## 目次\n\n"
        md_output_first += "\n".join(toc_list)
        md_output_first += "\n\n---\n\n"
        md_output_first += "\n\n---\n\n".join(md_sheets)
        logging.info("Markdown設計書をメモリ上に生成しました。")
        
        md_filename = filename.replace(".xlsx", ".md")
        encoded_filename = quote(md_filename)
        
        return func.HttpResponse(
            md_output_first,
            mimetype="text/markdown",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
            }
        )
    
    except Exception as e:
        logging.error(f"エラー: {str(e)}")
        return func.HttpResponse(f"エラーが発生しました: {str(e)}", status_code=500)