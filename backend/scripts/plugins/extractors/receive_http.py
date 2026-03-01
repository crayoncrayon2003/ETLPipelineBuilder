import os
import shutil
from typing import Dict, Any

import pluggy

from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")


class ReceiveHttp(BasePlugin):
    """
    (Configured Service 専用) HTTP リクエストボディを受け取り、指定パスに保存する。

    proxy_configured_service.py はリクエストの body を一時ファイルに書き込み、
    initial_container として先頭ノードの input_data 引数に渡す。
    本プラグインはその input_data から一時ファイルのパスを取り出し
    output_path に永続コピーする。

    後続プラグインは StepExecutor の edges 解決により
    target_input_name="input_path" 経由で output_path を受け取れる。

    【配置例】
        nodes:
          - id: "node_receive_http"
            plugin: "receive_http"
            params:
              output_path: "../data/Step1/body.csv"

          - id: "node_with_duckdb"
            plugin: "with_duckdb"
            params:
              query_file: "..."
              output_path: "..."
              table_name: "source_data"
              # input_path は edges で自動設定されるため不要

        edges:
          - source_node_id: "node_receive_http"
            target_node_id: "node_with_duckdb"
            target_input_name: "input_path"   # 後続プラグインが期待するキー名

    【注意】
    output_path は必ず指定すること。
    proxy_configured_service.py は処理後に一時ファイルを削除するため、
    output_path を省略して一時ファイルパスをそのまま後続に流すと
    ファイルが消えた後に後続プラグインが読もうとして失敗する。
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "receive_http"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "output_path": {
                    "type": "string",
                    "title": "Output File Path",
                    "description": (
                        "リクエストボディを保存するファイルパス。"
                        "後続プラグインはこのパスを input_path として参照する。"
                        "相対パスの場合は project_root からの相対パスとして解決される。"
                    )
                }
            },
            "required": ["output_path"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        """
        input_data (= proxy_configured_service の initial_container) から
        一時ファイルのパスを取得し、output_path に永続コピーする。

        設計上のポイント:
            params["input_path"] には依存しない。
            run() の input_data 引数を直接参照することで、
            StepExecutor の inputs キー名 ("input_data") と
            プラグインが期待するパラメータ名 ("input_path") のズレを根本から回避する。
        """
        output_path = self.params.get("output_path")
        if not output_path:
            raise ValueError(
                f"[{self.get_plugin_name()}] 'output_path' is required but not specified."
            )

        # input_data から一時ファイルパスを取得
        # proxy_configured_service が body_bytes を書き込んだ一時ファイル
        try:
            source_path = input_data.get_primary_file_path()
        except (ValueError, AttributeError) as e:
            raise ValueError(
                f"[{self.get_plugin_name()}] input_data has no file path. "
                f"This plugin must be placed as the first node "
                f"in a configured pipeline. Original error: {e}"
            )

        # output_path の親ディレクトリを作成
        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        # 一時ファイルを output_path に永続コピー
        # copy2 はタイムスタンプ等のメタデータも保持する
        shutil.copy2(source_path, output_path)
        logger.info(
            f"[{self.get_plugin_name()}] "
            f"Request body saved: '{source_path}' → '{output_path}'"
        )

        return self.finalize_container(
            container,
            output_path=output_path,
            metadata={
                "source_temp_path": source_path,
                "output_path": output_path,
                "file_size_bytes": os.path.getsize(output_path),
            }
        )
