# ETLPipelineBuilder プロジェクト状態サマリー

作成日: 2026-03-05  
最終更新日: 2026-07-27
目的: セッションがクリアされたときに、新しいセッションのClaudeがプロジェクトの最新状態を正確に把握できるようにする。

---

## 0. フレームワークの対象範囲

この文書で「フレームワーク」と呼ぶ範囲は `backend/scripts/` 配下。ただし、
`backend/scripts/plugins/` はフレームワーク利用者が実装するサンプルプラグインのため、
プラグイン固有の処理はフレームワークのバグ修正対象外とする。

`backend/` 直下の `run_pipeline_*.py` や `call_*_service.py` は、ライブラリやAPIの
利用例となるmain関数相当のサンプルであり、フレームワーク本体には含めない。
ただし、公開インターフェースの互換性や利用方法を確認する際の参考実装として扱う。

フレームワークが維持する主要な実行インターフェースは次の3種類。

1. ライブラリ呼び出し: 辞書型コンフィグを `StepExecutor` に渡して実行
2. コンフィグファイル実行: JSONファイルからパイプライン定義を読み込んで実行
3. API実行: REST APIのリクエストBodyまたはクエリでコンフィグを受け取って実行

プラグインIF（`BasePlugin`）と上記3種類の実行IFは、互換性を維持する。

---

## 1. プロジェクト概要・設計意図

### 何を実現しているか

HTTPリクエストで受け取ったデータを、プラグインの組み合わせによってETL処理し、別のエンドポイントや保存先に転送するフレームワーク。

処理の流れは「データ受信 → 変換・検証 → 送信」であり、各ステップをプラグインとして独立実装することで、任意のパイプラインを構成できる。

### 設計意図

**プラグイン設計**
新しいデータソース・変換・送信先を追加するとき、既存コードを変更せずプラグインを1ファイル追加するだけで対応できる。起動時に `plugins/` 以下を自動スキャンして登録する。

実際の実行時登録は、`FrameworkManager` が `BasePlugin` のサブクラスを走査し、
プラグイン名とクラスの対応を独自キャッシュに保存する方式である。pluggyの
hookspecは定義されているが、現在の実行ディスパッチには使用していない。

**ストレージ抽象化**
プラグインはパスを指定するだけで、ローカルFS・S3・メモリのどこに読み書きするかを意識しない。`storage_adapter.read_df("memory://run1/step1.csv")` のように全スキームで同一IFを使う。

**実行方式の多様性**
ライブラリ呼び出し・コンフィグファイル実行・API実行の3つの公開IFをサポートする。
GUIはAPI実行IFを利用するクライアントであり、独立したフレームワークIFには数えない。
どの実行方法でも同じプラグインIFとコアを使用する。

**責務の明確な分離**
- データ処理 → プラグイン
- パイプライン制御 → サービス層
- インフラ（ストレージ・シークレット）→ コアインフラ層
- パスやパラメータの指定 → 呼び出し側の責務（フレームワーク側での自動注入は行わない）

---

## 2. アーキテクチャ全体像

```
┌─────────────────────────────────────────────┐
│  フロントエンド (Electron + React + Vite)    │
│                                              │
│  PluginSidebar → FlowCanvas → ParamsSidebar  │
│  （プラグイン選択） （DAG編集） （パラメータ設定）   │
│                    ↓ apiClient.js            │
└────────────────────┼────────────────────────┘
                     │ HTTP REST API
┌────────────────────▼────────────────────────┐
│  バックエンド (FastAPI + Prefect)             │
│                                              │
│  /api/v1/pipelines/run  → pipeline_service   │ ← GUIからの実行
│  /api/v1/proxy/configured → configured_svc  │ ← JSON設定ファイル実行
│  /api/v1/proxy/controlled → controlled_svc  │ ← 動的ステップ実行
│                    ↓                         │
│  ┌─────────────────────────────────────┐    │
│  │  コア層                              │    │
│  │  StepExecutor → FrameworkManager    │    │
│  │                     ↓               │    │
│  │  BasePlugin → (各プラグイン).run()   │    │
│  │                     ↓               │    │
│  │  StorageAdapter（統一ストレージIF）   │    │
│  │  ├── LocalStorageBackend            │    │
│  │  ├── S3StorageBackend               │    │
│  │  └── MemoryStorageBackend           │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘

直接実行（APIサーバー不要）:
  run_pipeline_directly*.py → StepExecutor → プラグイン
```

---

## 3. フォルダ構造

### バックエンド

```
ETLPipelineBuilder/
├── backend/
│   ├── scripts/                            # PYTHONPATH=scripts で実行
│   │   ├── api/
│   │   │   ├── routers/
│   │   │   │   ├── pipelines.py            # POST /api/v1/pipelines/run
│   │   │   │   ├── plugins.py              # GET  /api/v1/plugins/
│   │   │   │   ├── proxy_configured_service.py   # ルーター
│   │   │   │   └── proxy_controlled_service.py   # ルーター
│   │   │   ├── schemas/
│   │   │   │   ├── pipeline.py             # PipelineNode / PipelineEdge / PipelineDefinition
│   │   │   │   └── plugin.py
│   │   │   └── services/
│   │   │       ├── pipeline_service.py          # Prefect DAG実行
│   │   │       ├── proxy_configured_service.py  # JSON設定ファイルベース実行
│   │   │       └── proxy_controlled_service.py  # 動的ステップ実行
│   │   ├── core/
│   │   │   ├── __init__.py                 # is_memory_path 等をexport
│   │   │   ├── data_container/
│   │   │   │   ├── container.py            # DataContainer / DataContainerStatus
│   │   │   │   └── formats.py              # SupportedFormats
│   │   │   ├── infrastructure/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── storage_adapter.py      # ルーティングのみ担当
│   │   │   │   ├── storage_path_utils.py   # パス正規化ユーティリティ
│   │   │   │   ├── storage_backends/
│   │   │   │   │   ├── base.py             # BaseStorageBackend（抽象クラス）
│   │   │   │   │   ├── local_backend.py
│   │   │   │   │   ├── s3_backend.py
│   │   │   │   │   └── memory_backend.py
│   │   │   │   ├── env_detector.py
│   │   │   │   ├── secret.py
│   │   │   │   ├── secret_resolver.py
│   │   │   │   └── spark_session_factory.py
│   │   │   ├── pipeline/
│   │   │   │   └── step_executor.py
│   │   │   └── plugin_manager/
│   │   │       ├── base_plugin.py
│   │   │       ├── hooks.py
│   │   │       └── manager.py
│   │   ├── plugins/
│   │   │   ├── __init__.py
│   │   │   ├── extractors/    # from_http, from_ftp, from_scp,
│   │   │   │                  # from_http_with_basic_auth, receive_http
│   │   │   ├── cleansing/     # duplicate_remover, encoding_converter,
│   │   │   │                  # format_detector, null_handler, archive_extractor
│   │   │   ├── transformers/  # with_duckdb, with_jinja2, with_spark
│   │   │   ├── validators/    # data_quality, json_schema, ngsi_validator, business_rules
│   │   │   └── loaders/       # to_http, to_ftp, to_scp, s3_delete
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   └── logger.py      # AppLogger / setup_logger
│   │   └── main.py            # FastAPI エントリポイント
│   │
│   ├── run_pipeline_directly1.py     # ローカルファイルを使う直接実行サンプル
│   ├── run_pipeline_directly2.py
│   ├── run_pipeline_directly3.py
│   ├── run_pipeline_directly4.py
│   ├── run_pipeline_directly5.py     # memory:// を使う直接実行サンプル
│   ├── run_pipeline_with_parameter_file1.py
│   ├── run_pipeline_with_parameter_file2.py
│   ├── call_configured_service.py    # configured_pipeline.json 経由のサンプル
│   ├── call_controlled_service.py    # 動的ステップのサンプル
│   └── configured_pipeline.json      # パイプライン定義JSONサンプル
│
└── test/
    └── data/
        ├── Step1/    # CSVデータ（body.csv 等）
        ├── Step2/    # SQLファイル（step2_with_duckdb.sql）
        ├── Step3/    # Parquet出力先
        ├── Step4/    # Jinja2テンプレート（step4.j2）
        └── Step5/    # JSON出力先
```

**重要:** `test/` は `backend/` の外（`ETLPipelineBuilder/test/`）にある。  
フロントエンドやAPIからのパス指定は `../../test/data/...` が正しい。

### フロントエンド

```
frontend/
├── electron/
│   ├── main.cjs       # Electronメインプロセス（ウィンドウ生成・IPC）
│   └── preload.cjs    # RendererプロセスへのAPI公開
├── src/
│   ├── App.jsx        # ルートコンポーネント。全体レイアウトを定義
│   ├── main.jsx       # Reactエントリポイント
│   ├── index.css
│   ├── api/
│   │   └── apiClient.js       # バックエンドAPIとのHTTP通信をまとめる
│   ├── components/
│   │   ├── FlowCanvas.jsx     # React Flow でDAGを描画・編集するキャンバス
│   │   ├── PluginSidebar.jsx  # 利用可能なプラグイン一覧サイドバー
│   │   ├── ParamsSidebar.jsx  # 選択中ノードのパラメータ編集サイドバー
│   │   ├── PipelineTabs.jsx   # 複数パイプラインのタブ管理
│   │   └── PluginNode.jsx     # DAG上の各ノードのUI
│   └── store/
│       └── useFlowStore.js    # Zustand によるグローバル状態管理
├── index.html
├── vite.config.js
├── package.json
├── build-linux-AppImage.sh    # Linux向けビルド
└── build-win-exe.ps1          # Windows向けビルド
```

---

## 4. モジュール分割方針と各モジュールの役割

### 4.1 フロントエンド

| モジュール | 役割 |
|---|---|
| `FlowCanvas.jsx` | React Flow でDAGをビジュアル編集。ノードの追加・接続・削除を担当 |
| `PluginSidebar.jsx` | `/api/v1/plugins/` からプラグイン一覧を取得・表示。ドラッグでキャンバスに追加 |
| `ParamsSidebar.jsx` | 選択中ノードの `params` をフォームで編集。プラグインのJSONスキーマを元にフォームを動的生成 |
| `PipelineTabs.jsx` | 複数パイプライン定義をタブで管理 |
| `PluginNode.jsx` | DAG上の各ノードの見た目と状態を管理 |
| `useFlowStore.js` | Zustand でノード・エッジ・選択状態等のグローバル状態を管理 |
| `apiClient.js` | バックエンドへのHTTPリクエストをまとめる（パイプライン実行・プラグイン一覧取得等） |

**フロントエンドでできること:**
- プラグインをDAGとして視覚的に接続してパイプラインを構築
- 各ノードのパラメータ（ファイルパス・URL等）をGUIで設定
- 構築したパイプラインをバックエンドに送信して実行
- 複数のパイプライン定義をタブで管理
- ElectronによりデスクトップアプリとしてもWebとしても動作

---

### 4.2 バックエンド APIスキーマ（api/schemas/pipeline.py）

```python
class PipelineNode(BaseModel):
    id: str
    plugin: str
    params: Dict[str, Any] = {}

class PipelineEdge(BaseModel):
    source_node_id: str
    target_node_id: str

class PipelineDefinition(BaseModel):
    name: str
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
```

エッジはノード間の接続のみを表す。`params` によるデータ受け渡しパスの指定は呼び出し側が行う。

---

### 4.3 サービス層の比較

| | pipeline_service | proxy_configured_service | proxy_controlled_service |
|---|---|---|---|
| 実行エンジン | Prefect（非同期・並列） | 同期（再帰） | 同期（ループ） |
| パイプライン定義 | HTTPリクエストボディ | `configured_pipeline.json` | HTTPリクエストの `payload.steps` |
| 主な用途 | GUIからの実行 | 固定パイプラインの運用 | プログラムからの動的制御 |
| StepExecutorの生成 | タスク内（毎回） | ループ外（1回） | ループ外（1回） |

**pipeline_service.py の重要な実装:**

```python
# シンクノード判定（正しい実装）
source_node_ids = {edge.source_node_id for edge in pipeline_def.edges}
sink_node_ids = [nid for nid in nodes_map if nid not in source_node_ids]
# ※ target_node_ids で判定するのは誤り

# 循環のみで構成されたパイプライン（終着ノードなし）はここで検知して止める
if not sink_node_ids:
    raise ValueError("No sink node found. Pipeline may have a circular dependency.")

# あるノードへの上流エッジは1本まで。2本以上は ValueError にする
# （BasePlugin.run() が単一の input_data しか受け取れないため、
#   黙って片方を上書きして消してしまうことを防ぐ）
incoming_edges = [e for e in edges if e.target_node_id == node_id]
if len(incoming_edges) > 1:
    raise ValueError(f"Node '{node_id}' has {len(incoming_edges)} incoming edges ...")

# パス正規化（../を正しく解決する）
return os.path.normpath(os.path.join(project_root, normalized_str))

# project_root = backend/ ディレクトリ
# pipelines.py の __file__ から3階層上で計算
```

パイプライン定義の共通バリデーションは
`core/pipeline/validation.py::validate_pipeline_graph()` に集約されている。
APIスキーマの `PipelineDefinition` 生成時に、空ノード、重複ノードID、不明なノードを
参照するエッジ、複数上流入力、循環を実行前に検出する。

`backend/` 直下のmain関数相当サンプルはフレームワーク外であり、独自の実行制御を持つ。

**proxy_controlled_service.py の設計:**
- 各ステップの `params` は呼び出し側が全て明示する（自動注入なし）
- `ensure_successful_result()` で `None`、`PENDING`、失敗ステータスを共通判定する
- 中間結果は `file_paths` が空でもよい。`DataContainer.data` だけを後続プラグインへ
  渡す実装を許可する
- APIレスポンスの `primary_file` を維持するため、最終結果には `file_paths` が必要

---

### 4.4 コアレイヤー

#### DataContainer（ステップ間のデータ受け渡し）

```python
class DataContainerStatus(Enum):
    PENDING, SUCCESS, ERROR, SKIPPED, VALIDATION_FAILED, TRANSFORMED, LOADED

class DataContainer:
    data: Optional[pd.DataFrame] # メモリ経由のデータ受け渡し領域（遅延読み込みプロパティ）
    file_paths: List[str]        # 出力ファイルパス（複数可）
    metadata: Dict[str, Any]     # メタデータ
    errors: List[str]            # エラーメッセージ
    status: DataContainerStatus
```

プラグイン間はファイルパスを介してデータを受け渡すのが基本（`memory://` を指定すればファイルシステムを使わずメモリ上でやり取りできる）。これと並行して、`DataContainer.data` を使えばファイルI/Oを介さずメモリ上で直接データを受け渡すこともできる。両者はどちらか一方を選ぶものではなく、プラグインの実装次第で共存する。

`data` は遅延読み込みプロパティになっている:
- 上流プラグインが `container.data = df` と明示的に設定していれば、そのDataFrameがそのまま返る（ファイルI/O無し）。
- 未設定の場合、`file_paths[0]` から `storage_adapter.read_df()` で1回だけ読み込みキャッシュする（ファイルパス経由の出力しかないプラグインの結果にも、下流プラグインが `.data` でアクセスできるようにするフォールバック）。
- `file_paths` が空の状態で先に `.data` が参照され、その後 `add_file_path()` された
  場合は、キャッシュ済みの `None` を確定値にせず遅延読み込みを再度有効にする。
- 読み込みに失敗した場合（非表形式フォーマット・ファイル削除済み等）は例外にせず `None` を返す。
- `__repr__` は遅延読み込みを誘発しないよう内部状態を直接見る。`print(container)` や単純なログ出力だけでファイルI/Oが走ることはない。

既存プラグイン（extractors/cleansing/transformers/validators/loaders 配下の全プラグイン）はいずれも `input_data`/`container.data` に一切触れておらず、`params` のパス文字列でファイル経由のやり取りを完結させている。そのため `.data` は完全にオプトインの仕組みであり、既存プラグインの挙動には影響しない。`.data` を使いたい場合のみ、プラグイン側で `input_data.data` を読む・`container.data = df` を設定する、という実装を選べる（`run(self, input_data, container)` のシグネチャ自体は変わらない）。

#### StepExecutor（単一ステップの実行）

```python
step_executor.execute_step(step_config, inputs={"input_data": prev_container})
```

- `inputs` の `DataContainer` はそのままプラグインの `run(input_data, container)` に渡す
- パス展開は行わない。パス指定は `step_config["params"]` で明示する

#### FrameworkManager（プラグイン管理）

- 起動時に `plugins/` 以下を再帰的にスキャン
- `BasePlugin` のサブクラスをキャッシュし `get_plugin_name()` で名前引き
- `inputs` が空のときは空の `DataContainer` を渡す
- `inputs` が1件のときはその値を `input_data` として渡す
- `inputs` が2件以上のときは、単一入力のプラグインIFでデータを黙って捨てないよう
  `ValueError` にする

#### BasePlugin（プラグインの基底クラス）

```
execute(input_data)
  └── prev_execute()   # セットアップ（params取得等）
  └── run()            # プラグイン固有の処理（サブクラスで実装）
  └── post_execute()   # 後処理・DataContainerに履歴追加
```

- `input_data` は読み取り専用（変更禁止）
- `container` が出力先。`finalize_container(container, output_path, metadata)` で完成させて返す
- 例外発生時は `status=ERROR` の `DataContainer` を返す（例外を伝搬しない）

---

### 4.5 ストレージレイヤー

#### バックエンド分離パターン

```
StorageAdapter（外部IF・ルーティングのみ）
├── _get_backend(path)  スキームに応じてバックエンドを選択
├── _normalize(path)    ローカルパスのみ正規化。memory:// と s3:// はそのまま
│
├── LocalStorageBackend   ← "" / "file://" スキーム
├── S3StorageBackend      ← "s3://" スキーム（boto3/s3fs 遅延import）
└── MemoryStorageBackend  ← "memory://" スキーム
```

新しいストレージ（Azure Blob・GCS等）を追加する場合は `BaseStorageBackend` を継承したクラスを追加し、`_get_backend()` に1行追加するだけ。外部IFは一切変更不要。

#### StorageAdapter の外部IF（変更なし）

```python
storage_adapter.read_df(path)
storage_adapter.write_df(df, path)
storage_adapter.read_text(path)
storage_adapter.write_text(text, path)
storage_adapter.read_bytes(path)
storage_adapter.write_bytes(content, path)
storage_adapter.exists(path)
storage_adapter.delete(path)
storage_adapter.list_files(path)
storage_adapter.copy_file(src, dst)
storage_adapter.copy_file_raw(src, dst)
storage_adapter.move_file(src, dst)
storage_adapter.mkdir(path)
storage_adapter.is_dir(path)
storage_adapter.rename(old, new)
storage_adapter.stat(path)
storage_adapter.get_size(path)
storage_adapter.clear_memory(prefix=None)  # memory://専用
```

#### memory:// の使い方

プラグインの `params` に `memory://` パスを指定するだけ。プラグイン側はローカルと同じIFで読み書きできる。

```python
# プラグインA の output_path
"memory://pipeline_run_1/step1_output.csv"

# プラグインB の input_path（同じパスを指定するだけ）
"memory://pipeline_run_1/step1_output.csv"

# パイプライン完了後（実行制御スクリプトの finally ブロックで）
storage_adapter.clear_memory(prefix="memory://pipeline_run_1/")
```

**注意:** `os.path.dirname("memory://...")` はファイルシステムのパスとして誤解釈されるため、`ensure_dir_exists` 等に渡す前に `is_memory_path()` でガードする。

```python
from core.infrastructure.storage_path_utils import is_memory_path

def ensure_dir_exists(path):
    if is_memory_path(path):
        return  # memory://パスはスキップ
    ...
```

#### storage_path_utils.py の対応スキーム

| スキーム | 正規化 |
|---|---|
| `""` / `file://` | `project_root` 基準で解決後 `os.path.normpath` |
| `s3://` | そのまま |
| `http://` / `https://` | そのまま |
| `memory://` | そのまま |

HTTP/HTTPSは読み取り専用として扱う。`read_text()`、`read_bytes()`、
`read_df()` および `download_remote_file()` で読み込めるが、書き込み系操作は
HTTPをS3として扱わず、未対応であることを明示するエラーを返す。

JSONとJSONLは異なる形式として扱う。`.json` はJSON配列、`.jsonl` は1行1レコードで
読み書きし、拡張子に応じて `lines` オプションを切り替える。

---

### 4.6 プラグイン一覧

| カテゴリ | プラグイン名 | 概要 |
|---|---|---|
| extractors | `from_http` | HTTPエンドポイントからファイルをダウンロード |
| extractors | `from_http_with_basic_auth` | Basic認証付きHTTPダウンロード |
| extractors | `from_ftp` | FTPサーバーからダウンロード |
| extractors | `from_scp` | SCPでダウンロード |
| extractors | `receive_http` | HTTPリクエストボディを受け取り指定パスに保存 |
| cleansing | `duplicate_remover` | 重複行を除去 |
| cleansing | `encoding_converter` | エンコーディング変換 |
| cleansing | `format_detector` | ファイルフォーマット検出 |
| cleansing | `null_handler` | NULL値処理 |
| cleansing | `archive_extractor` | アーカイブ展開 |
| transformers | `with_duckdb` | DuckDB でSQL変換 |
| transformers | `with_jinja2` | Jinja2テンプレートで変換 |
| transformers | `with_spark` | Apache Sparkで変換 |
| validators | `data_quality` | データ品質チェック |
| validators | `json_schema` | JSONスキーマ検証 |
| validators | `ngsi_validator` | NGSI形式検証 |
| validators | `business_rules` | ビジネスルールチェック |
| loaders | `to_http` | HTTP POSTでデータ送信 |
| loaders | `to_ftp` | FTPでアップロード |
| loaders | `to_scp` | SCPで送信 |
| loaders | `s3_delete` | S3オブジェクト削除 |

---

## 5. 3つの公開実行パターン

### 方式1: ライブラリ呼び出し

APIサーバーを経由せず、辞書型の `step_config` を `StepExecutor.execute_step()` に
渡して実行する。`backend/run_pipeline_directly*.py` は利用例であり、
フレームワーク本体ではない。

### 方式2: コンフィグファイル実行（proxy_configured_service）

`configured_pipeline.json` にパイプラインを定義し `call_configured_service.py` でHTTPリクエストを送る。固定パイプラインの継続運用に向く。

コンフィグファイルはライブラリ内の固定ディレクトリから読み込む。設定内の相対パスを
実行用の絶対パスに正規化する処理は意図した仕様である。

### 方式3: API実行

- `pipeline_service`: REST APIのリクエストBodyでDAG定義を受け取り、Prefectで実行
- `proxy_controlled_service`: リクエストで動的なステップリストを受け取り、同期実行
- `proxy_configured_service`: 指定された固定コンフィグファイルとリクエストBodyを使って実行

`call_controlled_service.py` でステップリストをHTTPリクエストで渡す。プログラムから動的にパイプラインを制御したい場合に使う。各ステップの `params` は呼び出し側が全て明示する。

`memory://` パスを使えばファイルシステムを使わずに中間ファイルを受け渡せる。

```python
# run_pipeline_directly5.py の構成例
http_output_file   = "memory://run_pipeline_directly5/step1_output.csv"
duckdb_output_file = "memory://run_pipeline_directly5/step3_output.parquet"
jinja2_output_file = "memory://run_pipeline_directly5/step5_output.json"

try:
    step_executor.execute_step(http_step_config)
    step_executor.execute_step(duckdb_step_config, inputs={"input_data": ...})
    step_executor.execute_step(jinja2_step_config, inputs={"input_data": ...})
finally:
    storage_adapter.clear_memory(prefix="memory://run_pipeline_directly5/")
```

---

## 6. 設計上の重要な決定事項

### パラメータ・パス

1. **`params` で全パスを明示する** — フレームワーク側での `input_path` 自動注入は行わない。プラグインが必要なファイルパスは `self.params.get("input_path")` で取得する。ステップ間のデータ受け渡しの責務は呼び出し側にある。

2. **プラグインのパラメータスキーマは JSON Schema 形式** — 各プラグインは `get_parameters_schema()` で JSON Schema を返す。フロントエンドはこのスキーマを元に `ParamsSidebar` のフォームを動的生成する。

### インスタンス管理・キャッシュ

3. **シングルトンにするもの** — `framework_manager`・`storage_adapter`・`secret_resolver` の3つはモジュールレベルでシングルトンインスタンスを生成し、プロセス全体で使い回す。起動コストが高い（プラグインスキャン・環境検出・シークレット読み込み）ため1回だけ初期化する。

4. **`FrameworkManager` はクラスをキャッシュし、インスタンスは毎回生成する** — 起動時に `plugins/` 以下をスキャンして `_plugin_class_cache: Dict[str, Type[BasePlugin]]` にクラスをキャッシュする。実行時（`call_plugin_execute`）は毎回 `plugin_class(params=params)` で新しいインスタンスを生成する。これにより各ステップ実行が独立した状態を持つことを保証する。インスタンスをキャッシュして使い回すとステップ間で `params` 等の状態が混入するリスクがある。

5. **`StepExecutor` のインスタンス化タイミング** — Prefect タスク内では毎回インスタンス化（並列実行でのタスク間状態共有を避けるため）。同期実行のサービス（`proxy_configured_service`・`proxy_controlled_service`）ではループ外で1回のみ生成して使い回す。

6. **スキーマ取得用の一時インスタンスはキャッシュしない** — `/api/v1/plugins/` のレスポンス生成時、`get_parameters_schema()` を呼ぶためだけに `plugin_class(params={})` で一時インスタンスを生成する。スキーマ取得後は破棄する。

### データの不変性・コピー

7. **`input_data` は読み取り専用** — `BasePlugin.run()` の引数 `input_data` は変更禁止。変更が必要な場合は `copy.deepcopy(input_data)` してから使う。`finalize_container()` には必ず `container`（出力先）を渡し、`input_data` を渡してはならない。

8. **`StepExecutor` は `params` と `inputs` を `deepcopy` してからプラグインに渡す** — プラグインが `params` や `DataContainer` を書き換えても、呼び出し元の状態に影響しない。`DataContainer.metadata` の更新は `copy.copy(metadata)` で浅いコピーを使う（メタデータはネストが浅いため）。`DataContainer.data`（pandas DataFrame）が設定されている場合も同じ `deepcopy` の対象になり、ステップ間で独立した状態が保たれる。

8a. **`DataContainer.data` はファイルパス経由と並行するメモリ経由の受け渡し手段（オプトイン）** — `data` は遅延読み込みプロパティ。上流プラグインが `container.data = df` を明示的に設定していればそのまま返し（ファイルI/O無し）、未設定なら `file_paths[0]` から `storage_adapter.read_df()` で1回だけ読み込んでキャッシュする。読み込み失敗時は例外にせず `None` を返す。既存プラグインは全て `.data` に触れず `params` のパス文字列で完結しているため挙動に影響はなく、`.data` を使うかどうかはプラグイン実装側の選択に委ねられる。`__repr__` は内部状態を直接見ることで、`print()`/ログ出力だけでは遅延読み込みを誘発しない。

### ストレージ拡張方針

9. **新しいストレージを追加する手順** — 以下の3ステップのみ。既存コードへの変更は最小限。
   1. `storage_backends/` に `XxxBackend(BaseStorageBackend)` を追加し全抽象メソッドを実装
   2. `storage_path_utils.py` に `is_xxx_path()` と `_normalize_xxx_path()` を追加し `SCHEME_NORMALIZERS` に登録
   3. `storage_adapter.py` の `StorageAdapter.__init__` にインスタンス追加、`_get_backend()` に分岐を1行追加

10. **S3 の外部ライブラリは遅延 import** — `boto3`・`s3fs` は `S3StorageBackend` の各メソッド内で遅延 import する。これにより未インストール環境でもサーバーが起動でき、S3 を使わない場合にクラッシュしない。他のクラウドストレージを追加するときも同じ方針を取る。

### エラーハンドリング

11. **プラグインは例外を外に伝搬しない** — `BasePlugin.execute()` は例外を `try/except` で受け止め、`status=ERROR` の `DataContainer` を返す。呼び出し側（サービス層）は `result.status == ERROR` を確認してパイプラインを停止するかどうかを判断する。

11a. **コンフィグをログへ出す前に機密値をマスクする** —
`password`、`secret`、`token`、`authorization` などを含むキーは、
ネストした辞書・リストも含めて `***REDACTED***` に置き換える。

12. **サービス層の結果判定を共通化する** —
`core/pipeline/result.py::ensure_successful_result()` は `result is None`、
`status == PENDING`、`ERROR`、`SKIPPED`、`VALIDATION_FAILED` を失敗として扱う。
`TRANSFORMED`、`LOADED`、`SUCCESS` は完了結果として扱う。
中間ステップの `file_paths` は必須ではなく、`DataContainer.data` だけの結果も許可する。
外部レスポンスとして `primary_file` を返すサービスでは、最終結果に限り
`file_paths` を確認する。

### パイプライン実行

13. **シンクノードの判定は `source_node_ids`** —
`source_node_ids`（どのエッジの出発点にもなっていないノード）で末端ノードを判定する。
`target_node_ids` では逆になるため使わない。フレームワーク内のDAGサービスである
`pipeline_service.py` と `proxy_configured_service.py` がこの判定を使用する。
`backend/` 直下のmain関数相当サンプルはフレームワーク外である。

13a. **循環は共通バリデーションで拒否する** —
`validate_pipeline_graph()` が循環を実行前に検出する。DAGサービス側にも
`sink_node_ids` が空の場合の防御的なエラーチェックを残す。

13b. **ノードへの上流エッジは1本まで** — `BasePlugin.run()` は単一の `input_data` しか受け取れない設計のため、あるノードに2本以上の上流エッジがあると、後から解決された方でもう一方が黙って上書きされて消える。これを防ぐため、2本以上の上流エッジを検出した時点で `ValueError` を送出する。複数の入力を1つのノードに合流させたい場合は、この設計自体（`BasePlugin`/`FrameworkManager` が単一入力しか扱えない点）を変更する必要があり、現状は未対応。

13c. **独立ノードの順序保証は実行エンジンごとに異なる** —
同期実行の `proxy_configured_service` は `pipeline_def.nodes` の宣言順に処理を開始する。
Prefectの `.submit()` を使う `pipeline_service` では独立ノードが並列実行されるため、
開始順・完了順は保証しない。順序が必要な処理はエッジで依存関係を明示する。

14. **`receive_http` プラグインは `configured_service` 専用** — `proxy_configured_service` の `initial_container`（リクエストボディの一時ファイル）を永続パスにコピーする役割を担う。`proxy_controlled_service` では使わない。

15. **`memory://` のクリアは実行制御層の責務** — プラグインはメモリのライフタイムを管理しない。パイプライン実行スクリプトの `finally` ブロックで `storage_adapter.clear_memory()` を呼ぶ。`clear_memory` プラグインは存在しない（責務の分離のため削除済み）。

16. **`pipeline_service.py` の変更は REST API 実行時のみ影響する** — `run_pipeline_directly*.py` や `run_pipeline_with_parameter_file*.py` は `pipeline_service.py` を経由しない。

### シークレット管理

17. **実行環境によってシークレットの取得先を自動切換え** — `secret_resolver` は起動時に `is_running_on_aws()` を確認し、AWS 環境なら `AWSSecretResolver`（AWS Secrets Manager）、ローカルなら `DotEnvSecretResolver`（`.env` ファイル）を使う。プラグインは `secret_resolver` を直接呼ぶだけで環境を意識しない。

17a. **AWS Secrets Manager への書き込みは直後に読み直して検証する** — `AWSSecretResolver._write_to_secretsmanager()` は `put_secret_value`/`create_secret` の後、`_verify_secretsmanager_write()` が `get_secret_value` を最大10回・0.2秒間隔で読み直し、書き込んだ値が実際に読めるようになってから `write()` を完了させる。Secrets Manager は複数ロケーションへレプリケートする都合上、書き込み直後の読み込みがごく短時間だけ反映前の値を返すことがあるため。あるプラグインが書いたシークレットを、同一パイプライン内の後続プラグインが直後に読む、という利用パターンでの整合性を保証する（検証に失敗した場合は `SecretWriteError` を送出する）。

### ISO 25010の観点

上記以外で、ISO 25010 と照らし合わせたときに記載が不足している事項

#### 信頼性

18. **一時ファイルは `finally` で必ず削除** — `proxy_configured_service` / `proxy_controlled_service` はリクエストボディを `tempfile.mkstemp()` で一時ファイルに書き出し、`finally` ブロックで `os.remove()` する。パイプライン実行中に例外が発生しても一時ファイルがディスクに残り続けない。

19. **`is_running_on_aws()` は `@lru_cache(maxsize=1)` でキャッシュ** — 実行環境は起動後に変わらないため結果をキャッシュし2回目以降は即返す。STS や boto3 などのネットワーク呼び出しは使わず環境変数のみで判定する（誤判定防止・レイテンシ防止）。テストで挙動を変えたい場合は `is_running_on_aws.cache_clear()` を呼ぶこと。

#### 保守性

20. **`SupportedFormats` がファイル形式判定の唯一の真実の源** — ファイル形式の判定は `SupportedFormats.from_path()` に一元化されており、プラグインや `StorageAdapter` が拡張子を独自に判定しない。新しいフォーマット（例: Avro）を追加するときは `SupportedFormats` に1エントリ追加するだけで全レイヤーに反映される。

21. **プラグイン名の重複は warning で検知してもサーバーは停止しない** — 同名プラグインが複数検出された場合、後から検出されたクラスで上書きして `logger.warning()` を出す。サーバーを停止しないのは可用性を優先するため。開発時は起動ログの `WARNING` で重複を発見できる。

---

## 7. 保留中の設計判断

`StorageAdapter.rename(old, new)` で異なるストレージスキーム間を指定した場合の扱いは
未確定。現在は移動元のバックエンドで処理されるため、呼び出し側は同一スキーム内の
renameに限定して使用する。異なるスキーム間を禁止するか、コピー後削除として
フレームワークで対応するかは別途決定する。今回の修正ではIF・挙動を変更していない。

---

## 8. サーバー起動・テスト実行

### バックエンド起動

```bash
cd /home/user/HOME/sample/ETLPipelineBuilder/backend
PYTHONPATH=scripts python scripts/api/main.py
```

### フロントエンド起動

```bash
cd /home/user/HOME/sample/ETLPipelineBuilder/frontend
npm start
# Vite dev server (http://localhost:5173) + Electron が起動
```

### テスト実行

```bash
cd /home/user/HOME/sample/ETLPipelineBuilder/backend
PYTHONPATH=.:scripts env/bin/pytest -q \
  test/test_core \
  test/test_api/test_services \
  test/test_api/test_schemas \
  test/test_utils
# 723 passed（2026-07-27時点）
```

上記はフレームワークのコア、サービス、スキーマ、ユーティリティのテスト結果。
APIルーターのTestClient統合テストは、現在のローカル環境でTestClient自体が停止する
問題があるため、この件数には含めていない。
