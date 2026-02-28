import os
from functools import lru_cache
from utils.logger import setup_logger

logger = setup_logger(__name__)


@lru_cache(maxsize=1)
def is_running_on_aws() -> bool:
    """
    Detects if the code is running in an AWS environment (Lambda, Glue, or ECS).
    Returns True if AWS environment is detected, False otherwise.

    【判定方針】
    AWS実行環境では以下の環境変数が必ず設定される。
    これらはWindowsローカルでは設定されないため、誤判定が発生しない。

        GLUE_VERSION          : AWS Glue ジョブで設定 (例: "4.0")
        AWS_LAMBDA_FUNCTION_NAME : AWS Lambda で設定
        AWS_EXECUTION_ENV     : Lambda / ECS / Glue 等の実行環境で設定

    【設計上の注意】
    - STS (get_caller_identity) は使用しない。
      Windows 開発環境で ~/.aws/credentials があると STS 呼び出しが成功し、
      account_id が取れてしまうため「ローカルなのに True」という誤判定になる。
    - boto3 は使用しない。
      STS へのネットワーク呼び出しはレイテンシが大きく (200-500ms)、
      この関数は SparkSessionFactory から複数回呼ばれることがある。
    - @lru_cache(maxsize=1) でキャッシュする。
      実行環境は起動後に変わらないため、2回目以降は即返す。
      キャッシュが不要なテストでは lru_cache を clear() すること。
    """
    is_aws = any([
        bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME")),  # Lambda
        bool(os.getenv("GLUE_VERSION")),              # Glue
        bool(os.getenv("AWS_EXECUTION_ENV")),         # Lambda / ECS / Glue
    ])

    if is_aws:
        logger.info("AWS environment detected.")
    else:
        logger.debug("Local environment detected (no AWS environment variables found).")

    return is_aws