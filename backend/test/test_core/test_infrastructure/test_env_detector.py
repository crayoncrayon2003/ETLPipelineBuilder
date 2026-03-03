import os
import pytest
from core.infrastructure.env_detector import is_running_on_aws


class TestIsRunningOnAws:
    """
    is_running_on_aws() の MCDC分析:

    【判定式】
        is_aws = any([P, Q, R])
        P: bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME"))
        Q: bool(os.getenv("GLUE_VERSION"))
        R: bool(os.getenv("AWS_EXECUTION_ENV"))

    【MCDCに必要なテストケース】
        1. P=True,  Q=False, R=False → True   (P だけで True になる)
        2. P=False, Q=True,  R=False → True   (Q だけで True になる)
        3. P=False, Q=False, R=True  → True   (R だけで True になる)
        4. P=False, Q=False, R=False → False  (全て False → False)

    【重要: @lru_cache(maxsize=1) の影響】
        is_running_on_aws() は結果をキャッシュする。
        環境変数を変えてもキャッシュが残ると前テストの結果が返ってしまう。
        各テスト前後に cache_clear() が必須。
    """

    # 判定対象の環境変数
    AWS_ENV_VARS = (
        "AWS_LAMBDA_FUNCTION_NAME",
        "GLUE_VERSION",
        "AWS_EXECUTION_ENV",
    )

    @pytest.fixture(autouse=True)
    def reset(self):
        """各テスト前後に環境変数と lru_cache をリセットする。

        【注意】環境変数のリセットだけでは不十分。
        @lru_cache(maxsize=1) のキャッシュが残ると
        環境変数を変えても前のテスト結果が返り続ける。
        is_running_on_aws.cache_clear() を必ず呼ぶこと。
        """
        # 前処理: 保存 & クリア
        saved = {v: os.environ.get(v) for v in self.AWS_ENV_VARS}
        for v in self.AWS_ENV_VARS:
            os.environ.pop(v, None)
        is_running_on_aws.cache_clear()

        yield

        # 後処理: 復元 & キャッシュクリア
        for v in self.AWS_ENV_VARS:
            os.environ.pop(v, None)
        for v, val in saved.items():
            if val is not None:
                os.environ[v] = val
        is_running_on_aws.cache_clear()

    # =========================================================
    # MCDC ケース1: P=True, Q=False, R=False → True
    # =========================================================

    def test_returns_true_when_only_lambda_function_name_is_set(self):
        """P=True, Q=False, R=False: AWS_LAMBDA_FUNCTION_NAME だけで True になる

        MCDC: P の独立した影響を確認。
        Q (GLUE_VERSION) と R (AWS_EXECUTION_ENV) は未設定。
        """
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "my-function"
        # GLUE_VERSION, AWS_EXECUTION_ENV は fixture でクリア済み

        assert is_running_on_aws() is True

    # =========================================================
    # MCDC ケース2: P=False, Q=True, R=False → True
    # =========================================================

    def test_returns_true_when_only_glue_version_is_set(self):
        """P=False, Q=True, R=False: GLUE_VERSION だけで True になる

        MCDC: Q の独立した影響を確認。
        P (AWS_LAMBDA_FUNCTION_NAME) と R (AWS_EXECUTION_ENV) は未設定。
        """
        os.environ["GLUE_VERSION"] = "4.0"
        # AWS_LAMBDA_FUNCTION_NAME, AWS_EXECUTION_ENV は fixture でクリア済み

        assert is_running_on_aws() is True

    # =========================================================
    # MCDC ケース3: P=False, Q=False, R=True → True
    # =========================================================

    def test_returns_true_when_only_aws_execution_env_is_set(self):
        """P=False, Q=False, R=True: AWS_EXECUTION_ENV だけで True になる

        MCDC: R の独立した影響を確認。
        P (AWS_LAMBDA_FUNCTION_NAME) と Q (GLUE_VERSION) は未設定。
        """
        os.environ["AWS_EXECUTION_ENV"] = "AWS_Lambda_python3.9"
        # AWS_LAMBDA_FUNCTION_NAME, GLUE_VERSION は fixture でクリア済み

        assert is_running_on_aws() is True

    # =========================================================
    # MCDC ケース4: P=False, Q=False, R=False → False
    # =========================================================

    def test_returns_false_when_no_aws_env_vars_set(self):
        """P=False, Q=False, R=False: 全て未設定 → False

        MCDC: 全条件が False のとき結果が False になることを確認。
        Windowsローカル環境の正常ケース。
        """
        # 全変数は fixture でクリア済み
        assert is_running_on_aws() is False

    # =========================================================
    # lru_cache の動作確認
    # =========================================================

    def test_cache_is_cleared_between_tests(self):
        """autouse fixture が lru_cache を正しくリセットしていることを確認。

        テスト順序によらず独立して動作することの保証。
        前テストで True が返った後でも、環境変数をクリアして
        cache_clear() すれば False が返ることを確認する。
        """
        # 一度 True を返す状態にする
        os.environ["GLUE_VERSION"] = "4.0"
        assert is_running_on_aws() is True

        # 環境変数を削除してキャッシュをクリアすると False になる
        os.environ.pop("GLUE_VERSION")
        is_running_on_aws.cache_clear()
        assert is_running_on_aws() is False

    def test_result_is_cached_on_repeated_calls(self):
        """同じ環境変数の状態で複数回呼んでもキャッシュから同じ結果が返る"""
        os.environ["GLUE_VERSION"] = "4.0"

        r1 = is_running_on_aws()
        r2 = is_running_on_aws()

        assert r1 is True
        assert r2 is True
        assert is_running_on_aws.cache_info().hits == 1   # 2回目はキャッシュヒット
        assert is_running_on_aws.cache_info().misses == 1

    # =========================================================
    # Windows開発環境での誤判定がないことの確認
    # =========================================================

    def test_returns_false_without_aws_env_vars_even_if_credentials_exist(self):
        """AWS認証情報があってもAWS環境変数がなければ False を返す。

        boto3/STS を使わず環境変数のみで判定するため誤判定しない。
        """
        # AWS認証情報が設定されているかのような状況でも
        # AWS_LAMBDA_FUNCTION_NAME / GLUE_VERSION / AWS_EXECUTION_ENV が
        # 未設定であれば False を返す
        assert is_running_on_aws() is False

    # =========================================================
    # 各 GLUE_VERSION の値のバリエーション確認
    # =========================================================

    @pytest.mark.parametrize("glue_version", ["1.0", "2.0", "3.0", "4.0"])
    def test_returns_true_for_all_glue_versions(self, glue_version):
        """Q=True: 全ての GLUE_VERSION 値で True を返す"""
        os.environ["GLUE_VERSION"] = glue_version
        assert is_running_on_aws() is True

    @pytest.mark.parametrize("execution_env", [
        "AWS_Lambda_python3.9",
        "AWS_Lambda_python3.11",
        "AWS_ECS_EC2",
        "AWS_ECS_FARGATE",
    ])
    def test_returns_true_for_aws_execution_env_values(self, execution_env):
        """R=True: 代表的な AWS_EXECUTION_ENV の値で True を返す"""
        os.environ["AWS_EXECUTION_ENV"] = execution_env
        assert is_running_on_aws() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])