# 設計書からのテスト仕様書自動生成アプリケーション

このアプリケーションは、Excel形式の設計書をアップロードすると、LLM（大規模言語モデル）を利用して構造化された設計書、テスト観点、そして単体テスト仕様書を自動生成するAzure Functionsアプリケーションです。

バックエンドのLLMは、設定によって **AWS Bedrock** と **Azure OpenAI Service** を切り替えて使用することができます。

## 主な機能

- Excel形式の設計書（`.xlsx`）をHTTP POSTで受け付けます。
- アップロードされたExcelを解析し、内容をMarkdown形式で構造化します。
- 構造化された情報をもとに、LLMがテスト観点を抽出します。
- 抽出されたテスト観点から、LLMが単体テスト仕様書（Markdown形式）を生成します。
- 生成されたMarkdownを`単体テスト仕様書.xlsx`テンプレートに書き込みます。
- 最終的な成果物（構造化設計書.md, テスト観点.md, テスト仕様書.md, テスト仕様書.xlsx）をZIPファイルにまとめて返却します。

## 処理フロー

アプリケーション全体の処理フローは以下のシーケンス図を参照してください：

![out/シーケンス図/シーケンス図.png](out/シーケンス図.png)

---

## 目次

- [前提条件](#前提条件)
- [環境構築](#環境構築)
- [設定](#設定)
  - [.envファイル](#envファイル)
  - [LLMサービスの選択](#llmサービスの選択)
  - [AWS Bedrockの設定](#aws-bedrockの設定)
  - [Azure OpenAIの設定](#azure-openaiの設定)
- [ローカルでの実行](#ローカルでの実行)
- [主要ファイル構成](#主要ファイル構成)
- [使用技術一覧](#使用技術一覧)

---

## 前提条件

- [Python 3.9 以降](https://www.python.org/)
- [Azure Functions Core Tools](https://docs.microsoft.com/ja-jp/azure/azure-functions/functions-run-local)
- [AWS アカウント](https://aws.amazon.com/jp/) （Bedrock利用時）
- [Azure アカウント](https://azure.microsoft.com/ja-jp/) （Azure OpenAI利用時、またはAzureへのデプロイ時）

---

## 環境構築

1.  **リポジトリのクローン**
    ```bash
    git clone <リポジトリのURL>
    cd <プロジェクトディレクトリ>
    ```

2.  **仮想環境の作成と有効化**
    ```bash
    # 仮想環境を作成
    python -m venv .venv

    # Windowsの場合
    .venv\Scripts\activate

    # macOS/Linuxの場合
    source .venv/bin/activate
    ```

3.  **必要なライブラリのインストール**
    ```bash
    pip install -r requirements.txt
    ```

4.  **CORS設定（ローカル開発用）**
    フロントエンドからAPIを呼び出すために、`local.settings.json`にCORS設定が必要です。
    
    プロジェクトルートに`local.settings.json`ファイルを作成し、以下の内容を記述します：
    
    ```json
    {
        "IsEncrypted": false,
        "Values": {
            "AzureWebJobsStorage": "",
            "FUNCTIONS_WORKER_RUNTIME": "python"
        },
        "Host": {
            "CORS": "*"
        }
    }
    ```
    
    **注意:** `"CORS": "*"` はすべてのオリジンからのアクセスを許可します。本番環境では、特定のドメインのみを許可するように設定することを推奨します。

---

## 設定

### .envファイル

プロジェクトのルートにある`.env.example`をコピーして`.env`ファイルを作成します。このファイルに各種サービスの接続情報を記述します。

```bash
copy .env.example .env
```

**注意:** `.env`ファイルには認証情報などの機密情報が含まれるため、絶対にGitでコミットしないでください。`.gitignore`に`.env`が記載されていることを確認してください。

### LLMサービスの選択

`.env`ファイル内の`LLM_SERVICE`で使用するサービスを指定します。

- **AWS Bedrockを利用する場合:**
  ```
  LLM_SERVICE="AWS"
  ```
- **Azure OpenAI Serviceを利用する場合:**
  ```
  LLM_SERVICE="AZURE"
  ```

### AWS Bedrockの設定

`LLM_SERVICE="AWS"`を選択した場合、以下の設定を行います。

#### 1. モデルアクセスの有効化

AWS Bedrockでモデルを利用する前に、使用したいリージョンで対象モデルへのアクセスを有効化する必要があります。

1.  AWSマネジメントコンソールで **Bedrock** サービスに移動します。
2.  左下のメニューから **「モデルアクセス」** をクリックします。
3.  右上の **「モデルアクセスを管理」** ボタンをクリックします。
4.  **Anthropic** の **「Claude Sonnet 4.5」** にチェックを入れ、右下の「変更を保存」をクリックします。
    - アクセスステータスが「アクセス権が付与されました」に変わるまで数分待ちます。

#### 2. IAMユーザーの作成

このアプリケーション専用のIAMユーザーを作成し、プログラムによるアクセスキー（アクセスキーIDとシークレットアクセスキー）を発行します。

1.  AWSマネジメントコンソールでIAMサービスに移動します。
2.  「ユーザー」→「ユーザーを作成」をクリックします。
3.  ユーザー名を指定し、「次へ」をクリックします。
4.  「ポリシーを直接アタッチする」を選択します。（ポリシーは次のステップで作成します）
5.  ユーザーを作成後、「セキュリティ認証情報」タブに移動し、「アクセスキーを作成」をクリックします。
6.  「コマンドラインインターフェイス (CLI)」を選択し、アクセスキーを作成します。
7.  表示された「アクセスキーID」と「シークレットアクセスキー」をコピーし、`.env`ファイルに設定します。**この画面を閉じるとシークレットアクセスキーは二度と表示できないので注意してください。**

#### 3. IAMポリシーの作成とアタッチ

Bedrockのモデルを呼び出すための権限をまとめたIAMポリシーを作成し、上記で作成したIAMユーザーにアタッチします。

1.  IAMサービスの「ポリシー」→「ポリシーを作成」をクリックします。
2.  「JSON」タブを選択し、以下のポリシーを貼り付けます。

    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "bedrock:InvokeModel",
                "Resource": "*"
            }
        ]
    }
    ```
    *本番環境では、`Resource`をより厳密に特定のモデルARNやプロファイルARN (`arn:aws:bedrock:ap-northeast-1:799642483126:inference-profile/jp.anthropic.claude-sonnet-4-5-20250929-v1:0` など) に限定することが推奨されます。*

3.  ポリシーに名前（例: `BedrockAppInvokePolicy`）を付けて保存します。
4.  作成したIAMユーザーの「許可を追加」から、今作成したポリシーをアタッチします。

推論プロファイルは共有のものを利用するため、自身で作成する必要はありません。

#### 4. .envファイルの設定

日本リージョン用の共有推論プロファイルが用意されているため、それを利用します。

```.env
# --- AWS Bedrock Connection Information ---
# LLMサービスの選択 (AWSまたはAZURE)
LLM_SERVICE=AWS

# AWSリージョン (ap-northeast-1 を指定)
AWS_REGION=ap-northeast-1

# AWSアクセスキーID
AWS_ACCESS_KEY_ID=...

# AWSシークレットアクセスキー
AWS_SECRET_ACCESS_KEY=...

# AWS Bedrock 推論プロファイルID
AWS_BEDROCK_MODEL_ID=jp.anthropic.claude-sonnet-4-5-20250929-v1:0
```
**注意:** 上記の推論プロファイルは `ap-northeast-1` (東京リージョン) 専用です。他のリージョンで実行する場合は、別途推論プロファイルを作成し、そのARNを指定する必要があります。

---

### Azure OpenAIの設定

`LLM_SERVICE="AZURE"`を選択した場合、以下の環境変数を設定します。

```.env
# --- Azure OpenAI Service Connection Information ---
AZURE_OPENAI_API_KEY=...
AZURE_OPENAI_ENDPOINT=...
AZURE_OPENAI_API_VERSION=...
AZURE_OPENAI_DEPLOYMENT=...
```

これらの値は、AzureポータルでAzure OpenAI Serviceのリソースを作成し、「キーとエンドポイント」から取得できます。`AZURE_OPENAI_DEPLOYMENT`には、作成したモデルのデプロイ名を指定します。

---

## ローカルでの実行

1.  ターミナルのルートディレクトリで、Azure Functionsホストを起動します。
    ```bash
    func start
    ```

2.  起動後、`http://localhost:7071/api/upload` というエンドポイントが利用可能になります。

3.  `curl`やPostmanなどのツールを使って、ExcelファイルをPOSTします。（動作検証例）

    **curlの例:**
    ```bash
    curl -X POST -F "documentFile=@/path/to/your/設計書.xlsx" http://localhost:7071/api/upload -o output.zip
    ```
    - `@`の後にテストしたいExcelファイルの絶対パスまたは相対パスを指定します。
    - `-o output.zip`で、返却されたZIPファイルを保存します。

---

## 主要ファイル構成

- **`function_app.py`**: メインの処理が記述されたAzure FunctionsのHTTPトリガー関数。
- **`requirements.txt`**: Pythonの依存パッケージリスト。
- **`.env.example`**: 環境変数のテンプレートファイル。
- **`host.json`**: Azure Functionsホストのグローバル設定ファイル。ログ設定、拡張機能バンドルのバージョンなど、アプリケーション全体の動作を制御します。
- **`local.settings.json`**: ローカル開発環境専用の設定ファイル。ランタイム設定、CORS設定、ローカル環境変数などを管理します（`.gitignore`で除外済み）。
- **`単体テスト仕様書.xlsx`**: テスト仕様書を生成する際の書き込み先テンプレートExcelファイル。
- **`index.html`, `script.js`, `style.css`**: 簡単な動作確認用のフロントエンドファイル。

---

## 使用技術一覧

本プロジェクトで利用している主要なライブラリ、開発ツール、およびクラウドサービスは以下の通りです。

### 1. Pythonライブラリ (`requirements.txt`)

-   `azure-functions`: Azure Functionsのトリガーやバインディングなど、Pythonでの関数開発を可能にするためのコアライブラリ。
-   `boto3`: AWS SDK for Python。AWS Bedrockなど、AWSサービスを呼び出すためのクライアントライブラリ。
-   `openai`: Azure OpenAI ServiceおよびOpenAI APIを呼び出すためのクライアントライブラリ。
-   `python-dotenv`: `.env`ファイルから環境変数を読み込むために使用。ローカル開発で接続情報を管理します。
-   `pandas`, `openpyxl`, `tabulate`: 主にExcelファイルの読み書きやデータ整形のために使用されるライブラリ群。

### 2. 開発・デプロイツール

-   **Azure Functions Core Tools**:
    ローカル環境でAzure Functionsを開発、実行、デバッグするためのコマンドラインツール (`func`コマンド)。`func start`でのローカルテストに必須です。

-   **Visual Studio Code 拡張機能**:
    -   **Azure Tools**: Azure Functions, App Service, Storageなど、AzureリソースをVS CodeのGUI上から直接操作・管理・デプロイできる統合拡張機能パック。
    -   **Python**: VS CodeでPythonのコード補完、デバッグ、IntelliSenseなどを有効にするための必須拡張機能。

### 3. 利用しているクラウドサービス

-   **Azure Functions**:
    サーバーレスでコードを実行するためのコンピューティングサービス。本プロジェクトのバックエンド処理はこの上で動作します。

-   **AWS Bedrock**:
    Amazon Web Services上で提供される、Claude Sonnetなどの大規模言語モデルを利用するためのマネージドサービス。

-   **Azure OpenAI Service**:
    Microsoft Azure上で提供される、GPTなどの大規模言語モデルを利用するためのサービス。
