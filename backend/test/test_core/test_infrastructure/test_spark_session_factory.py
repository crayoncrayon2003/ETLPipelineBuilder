import os
import sys
import threading
import time
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from core.infrastructure.spark_session_factory import SparkSessionFactory


class TestSparkSessionFactory:

    @pytest.fixture(autouse=True)
    def reset_factory(self):
        """各テスト前後にファクトリ状態をリセットする"""
        SparkSessionFactory._spark_session = None
        SparkSessionFactory._glue_context = None
        yield
        SparkSessionFactory._spark_session = None
        SparkSessionFactory._glue_context = None

    @pytest.fixture
    def mock_awsglue_module(self):
        """awsglue モジュールをモックする"""
        mock_awsglue = MagicMock()
        mock_context = MagicMock()
        mock_awsglue.context = mock_context
        with patch.dict("sys.modules", {
            "awsglue": mock_awsglue,
            "awsglue.context": mock_context,
        }):
            yield mock_context

    @pytest.fixture
    def clean_env(self):
        """環境変数を完全にクリーンな状態にしてテストする"""
        original = os.environ.copy()
        for key in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "JAVA_HOME"):
            os.environ.pop(key, None)
        yield
        os.environ.clear()
        os.environ.update(original)

    @pytest.fixture
    def save_restore_env(self):
        """テスト後に環境変数を復元する"""
        original = os.environ.copy()
        yield
        os.environ.clear()
        os.environ.update(original)

    # =========================================================
    # get_spark_session
    # MCDC:
    #   条件A: _spark_session is not None  (キャッシュヒット)
    #   条件B: ロック内 _spark_session is not None  (double-checked locking)
    #   条件C: is_running_on_aws()
    #   条件D: PYSPARK_PYTHON が既に設定されているか  (setdefault)
    #   ※ JAVA_HOME は spark_session_factory.py が設定しないため条件Eは削除
    # =========================================================

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_spark_session_returns_cached_session(self, mock_is_aws):
        """条件A=True: キャッシュ済みセッションをそのまま返す
        is_running_on_aws() を呼ばず初期化処理に入らないことを確認"""
        mock_spark = Mock()
        SparkSessionFactory._spark_session = mock_spark

        result = SparkSessionFactory.get_spark_session()

        assert result is mock_spark
        mock_is_aws.assert_not_called()

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_spark_session_on_aws(self, mock_is_aws, mock_awsglue_module):
        """条件A=False, C=True: AWS環境でのGlue初期化"""
        mock_is_aws.return_value = True
        mock_sc = Mock()
        with patch("pyspark.context.SparkContext") as mock_spark_ctx:
            mock_spark_ctx.getOrCreate.return_value = mock_sc
            mock_glue_ctx = Mock()
            mock_spark_session = Mock()
            mock_glue_ctx.spark_session = mock_spark_session
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            result = SparkSessionFactory.get_spark_session()

        assert result is mock_spark_session
        assert SparkSessionFactory._spark_session is mock_spark_session
        assert SparkSessionFactory._glue_context is mock_glue_ctx
        mock_spark_ctx.getOrCreate.assert_called_once()
        mock_awsglue_module.GlueContext.assert_called_once_with(mock_sc)

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_get_spark_session_on_local(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """条件A=False, C=False: ローカル環境での初期化"""
        mock_is_aws.return_value = False
        mock_conf, mock_builder, mock_spark = _make_local_mocks(
            mock_conf_cls, mock_session_cls
        )

        result = SparkSessionFactory.get_spark_session()

        assert result is mock_spark
        assert SparkSessionFactory._spark_session is mock_spark
        mock_builder.appName.assert_called_once_with("ETLFrameworkSpark")
        mock_builder.config.assert_called_once_with(conf=mock_conf)
        mock_builder.getOrCreate.assert_called_once()

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_spark_session_singleton_on_aws(self, mock_is_aws, mock_awsglue_module):
        """条件A: 複数回呼んでも同一インスタンスを返す (シングルトン)"""
        mock_is_aws.return_value = True
        mock_sc = Mock()
        with patch("pyspark.context.SparkContext") as mock_spark_ctx:
            mock_spark_ctx.getOrCreate.return_value = mock_sc
            mock_glue_ctx = Mock()
            mock_glue_ctx.spark_session = Mock()
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            r1 = SparkSessionFactory.get_spark_session()
            r2 = SparkSessionFactory.get_spark_session()

        assert r1 is r2
        mock_awsglue_module.GlueContext.assert_called_once()  # 初期化は1回のみ

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_get_spark_session_singleton_on_local(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """条件A: ローカルでも複数回呼んで同一インスタンス"""
        mock_is_aws.return_value = False
        _, _, mock_spark = _make_local_mocks(mock_conf_cls, mock_session_cls)

        r1 = SparkSessionFactory.get_spark_session()
        r2 = SparkSessionFactory.get_spark_session()

        assert r1 is r2
        mock_session_cls.builder.getOrCreate.assert_called_once()  # 初期化は1回のみ

    def test_get_spark_session_double_checked_locking(self):
        """条件B: double-checked locking の検証
        複数スレッドが同時に未初期化状態で get_spark_session() を呼んでも
        初期化処理が1回だけ実行されることを確認する。

        MCDC: ロック取得後の再チェック (条件B) が独立して初期化の多重実行を防ぐ。

        【デッドロック回避の設計】
        is_running_on_aws() はロック内で呼ばれるため、ここに barrier.wait() を
        仕込むとロックを保持したまま別スレッドを待つデッドロックになる。
        正しい設計:
          - barrier.wait() は get_spark_session() 呼び出しの「前」 (ロック外) に置く
          - 初期化の遅延は GlueContext コンストラクタ (ロック内処理) に time.sleep で入れる
          - これにより「全スレッドが同時にロック競合する状況」を作りつつデッドロックを避ける
        """
        glue_init_count = 0
        barrier = threading.Barrier(2)  # 2スレッドがロック競合するタイミングを揃える

        mock_sc = Mock()
        mock_spark_session = Mock()

        def slow_glue_context_init(sc):
            """GlueContext コンストラクタの遅延: ロック内でスレッド1が時間をかけている間に
            スレッド2がロック取得を試みる状況を作る"""
            nonlocal glue_init_count
            glue_init_count += 1
            time.sleep(0.05)  # ロック保持中に遅延 → スレッド2はロック待ちになる
            ctx = Mock()
            ctx.spark_session = mock_spark_session
            return ctx

        mock_awsglue = MagicMock()
        mock_context_module = MagicMock()
        mock_awsglue.context = mock_context_module

        with patch(
            "core.infrastructure.spark_session_factory.is_running_on_aws",
            return_value=True,
        ), patch.dict("sys.modules", {
            "awsglue": mock_awsglue,
            "awsglue.context": mock_context_module,
        }), patch("pyspark.context.SparkContext") as mock_spark_ctx:
            mock_spark_ctx.getOrCreate.return_value = mock_sc
            mock_context_module.GlueContext = Mock(side_effect=slow_glue_context_init)

            results = [None, None]

            def worker(i):
                barrier.wait()  # ロック「外」で同期: 両スレッドがほぼ同時にロック競合する
                results[i] = SparkSessionFactory.get_spark_session()

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # 両スレッドが有効なセッションを受け取る
        assert results[0] is mock_spark_session
        assert results[1] is mock_spark_session
        # GlueContext の初期化は必ず1回だけ (double-checked locking の効果)
        assert glue_init_count == 1, (
            f"GlueContext が {glue_init_count} 回初期化された。"
            "double-checked locking が機能していない可能性がある。"
        )

    # =========================================================
    # 環境変数の setdefault 動作確認
    # MCDC:
    #   条件D: PYSPARK_PYTHON が既に設定されているか (setdefault)
    #   ※ JAVA_HOME は spark_session_factory.py が設定しないため条件Eは削除
    # =========================================================

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_env_vars_set_when_not_present(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """条件D=False: 環境変数が未設定のとき sys.executable がセットされる
        PYSPARK_PYTHON にハードコードした Linux パスではなく sys.executable を使うため
        期待値は sys.executable であることを確認する。
        JAVA_HOME は setdefault を削除したため設定されない (None のままが正しい)。"""
        mock_is_aws.return_value = False
        _make_local_mocks(mock_conf_cls, mock_session_cls)

        SparkSessionFactory.get_spark_session()

        assert os.environ.get("PYSPARK_PYTHON") == sys.executable
        assert os.environ.get("PYSPARK_DRIVER_PYTHON") == sys.executable
        # JAVA_HOME は spark_session_factory.py が設定しない (PATH の java を使う)
        assert os.environ.get("JAVA_HOME") is None

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_existing_pyspark_python_is_not_overwritten(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, save_restore_env
    ):
        """条件D=True: PYSPARK_PYTHON が既に設定されているとき上書きしない
        MCDC: D=True の独立した影響 (Windows既存設定の保護)"""
        mock_is_aws.return_value = False
        _make_local_mocks(mock_conf_cls, mock_session_cls)

        os.environ["PYSPARK_PYTHON"] = "C:/Python39/python.exe"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Python39/python.exe"
        os.environ.pop("JAVA_HOME", None)

        SparkSessionFactory.get_spark_session()

        assert os.environ["PYSPARK_PYTHON"] == "C:/Python39/python.exe"
        assert os.environ["PYSPARK_DRIVER_PYTHON"] == "C:/Python39/python.exe"

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_java_home_is_never_set_by_factory(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """spark_session_factory.py は JAVA_HOME を一切設定しない。
        JAVA_HOME は PATH の java を使うため、コードからの設定は不要かつ
        Windows ではハードコードした Linux パスで壊れるリスクがある。
        事前に未設定の状態で実行後も None のままであることを確認する。"""
        mock_is_aws.return_value = False
        _make_local_mocks(mock_conf_cls, mock_session_cls)

        SparkSessionFactory.get_spark_session()

        assert os.environ.get("JAVA_HOME") is None

    # =========================================================
    # Spark設定値の確認
    # =========================================================

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_spark_configuration_values(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """SparkConf に正しい設定値がセットされる"""
        mock_is_aws.return_value = False
        mock_conf, _, _ = _make_local_mocks(mock_conf_cls, mock_session_cls)

        SparkSessionFactory.get_spark_session()

        actual_calls = [c[0] for c in mock_conf.set.call_args_list]
        for key, val in [
            ("spark.executor.memory", "2g"),
            ("spark.driver.memory", "2g"),
            ("spark.executor.cores", "4"),
            ("spark.sql.shuffle.partitions", "10"),
        ]:
            assert (key, val) in actual_calls, f"Missing config: {key}={val}"

    # =========================================================
    # stop_spark_session
    # MCDC:
    #   条件F: is_running_on_aws()
    #   条件G: _spark_session is not None
    # =========================================================

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_stop_spark_session_on_aws_is_noop(self, mock_is_aws):
        """条件F=True: AWS上では完全にno-op
        _spark_session が変更されず stop() も呼ばれない"""
        mock_is_aws.return_value = True
        mock_spark = Mock()
        mock_glue_ctx = Mock()
        SparkSessionFactory._spark_session = mock_spark
        SparkSessionFactory._glue_context = mock_glue_ctx

        SparkSessionFactory.stop_spark_session()

        mock_spark.stop.assert_not_called()
        assert SparkSessionFactory._spark_session is mock_spark    # 変化なし
        assert SparkSessionFactory._glue_context is mock_glue_ctx  # 変化なし

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_stop_spark_session_on_local(self, mock_is_aws):
        """条件F=False, G=True: ローカルで stop() + _spark_session を None化"""
        mock_is_aws.return_value = False
        mock_spark = Mock()
        SparkSessionFactory._spark_session = mock_spark

        SparkSessionFactory.stop_spark_session()

        mock_spark.stop.assert_called_once()
        assert SparkSessionFactory._spark_session is None

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_stop_spark_session_also_resets_glue_context(self, mock_is_aws):
        """条件F=False, G=True: _glue_context もリセットされる
        MCDC: stop後の _glue_context=None が独立して影響する"""
        mock_is_aws.return_value = False
        mock_spark = Mock()
        mock_glue_ctx = Mock()
        SparkSessionFactory._spark_session = mock_spark
        SparkSessionFactory._glue_context = mock_glue_ctx

        SparkSessionFactory.stop_spark_session()

        assert SparkSessionFactory._spark_session is None
        assert SparkSessionFactory._glue_context is None

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_stop_spark_session_when_no_session(self, mock_is_aws):
        """条件F=False, G=False: セッションなし → 例外が発生しない"""
        mock_is_aws.return_value = False
        SparkSessionFactory._spark_session = None

        SparkSessionFactory.stop_spark_session()  # 例外が出ないことを確認

        assert SparkSessionFactory._spark_session is None

    # =========================================================
    # get_glue_context
    # MCDC:
    #   条件H: is_running_on_aws()
    #   条件I: _glue_context is None
    # =========================================================

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_glue_context_on_local_raises_runtime_error(self, mock_is_aws):
        """条件H=False: ローカル環境では RuntimeError を raise"""
        mock_is_aws.return_value = False

        with pytest.raises(RuntimeError, match="GlueContext is only available on AWS Glue"):
            SparkSessionFactory.get_glue_context()

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_glue_context_on_aws_initializes_via_get_spark_session(
        self, mock_is_aws, mock_awsglue_module
    ):
        """条件H=True, I=True: _glue_context が None のとき get_spark_session() 経由で初期化"""
        mock_is_aws.return_value = True
        mock_sc = Mock()
        with patch("pyspark.context.SparkContext") as mock_spark_ctx:
            mock_spark_ctx.getOrCreate.return_value = mock_sc
            mock_glue_ctx = Mock()
            mock_glue_ctx.spark_session = Mock()
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            result = SparkSessionFactory.get_glue_context()

        assert result is mock_glue_ctx
        assert SparkSessionFactory._glue_context is mock_glue_ctx

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_glue_context_returns_cached_context(self, mock_is_aws, mock_awsglue_module):
        """条件H=True, I=False: キャッシュ済み _glue_context をそのまま返す
        MCDC: I=False の独立した影響 (get_spark_session() を呼ばない)"""
        mock_is_aws.return_value = True
        mock_glue_ctx = Mock()
        SparkSessionFactory._glue_context = mock_glue_ctx

        result = SparkSessionFactory.get_glue_context()

        assert result is mock_glue_ctx
        mock_awsglue_module.GlueContext.assert_not_called()  # 再初期化しない

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    def test_get_glue_context_after_stop_reinitializes(self, mock_is_aws, mock_awsglue_module):
        """stop_spark_session() 後に get_glue_context() で再初期化できる
        条件H=True, I=True (stopによって _glue_context が None に戻った状態)"""
        mock_is_aws.return_value = False

        # stop で両方 None になる
        mock_spark = Mock()
        SparkSessionFactory._spark_session = mock_spark
        SparkSessionFactory.stop_spark_session()
        assert SparkSessionFactory._glue_context is None

        # AWS に切り替えて再初期化
        mock_is_aws.return_value = True
        mock_sc = Mock()
        with patch("pyspark.context.SparkContext") as mock_spark_ctx:
            mock_spark_ctx.getOrCreate.return_value = mock_sc
            mock_glue_ctx = Mock()
            mock_glue_ctx.spark_session = Mock()
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            result = SparkSessionFactory.get_glue_context()

        assert result is mock_glue_ctx

    # =========================================================
    # スレッドセーフの追加確認
    # =========================================================

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_concurrent_calls_return_same_session(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """複数スレッドが同時に呼んでも同一セッションが返され、初期化は1回だけ行われる"""
        mock_is_aws.return_value = False
        mock_conf, mock_builder, mock_spark = _make_local_mocks(
            mock_conf_cls, mock_session_cls
        )

        results = [None] * 10
        barrier = threading.Barrier(10)

        def worker(i):
            barrier.wait()  # 全スレッドを同時に解放
            results[i] = SparkSessionFactory.get_spark_session()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r is mock_spark for r in results)
        mock_builder.getOrCreate.assert_called_once()  # 初期化は必ず1回

    @patch("core.infrastructure.spark_session_factory.is_running_on_aws")
    @patch("pyspark.sql.SparkSession")
    @patch("pyspark.SparkConf")
    def test_stop_then_reinitialize(
        self, mock_conf_cls, mock_session_cls, mock_is_aws, clean_env
    ):
        """stop → 再 get_spark_session() の流れが正常に動く"""
        mock_is_aws.return_value = False
        mock_conf, mock_builder, mock_spark_1 = _make_local_mocks(
            mock_conf_cls, mock_session_cls
        )

        s1 = SparkSessionFactory.get_spark_session()
        assert s1 is mock_spark_1

        SparkSessionFactory.stop_spark_session()
        assert SparkSessionFactory._spark_session is None

        # 再初期化用のモックを差し替え
        mock_spark_2 = Mock()
        mock_builder.getOrCreate.return_value = mock_spark_2

        s2 = SparkSessionFactory.get_spark_session()
        assert s2 is mock_spark_2
        assert s2 is not s1  # 別インスタンス


# =========================================================
# ヘルパー
# =========================================================

def _make_local_mocks(mock_conf_cls, mock_session_cls):
    """ローカル環境の SparkConf / SparkSession をセットアップするヘルパー"""
    mock_conf = Mock()
    mock_conf.set.return_value = mock_conf
    mock_conf_cls.return_value = mock_conf

    mock_spark = Mock()
    mock_builder = Mock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_spark
    mock_session_cls.builder = mock_builder

    return mock_conf, mock_builder, mock_spark


if __name__ == "__main__":
    pytest.main([__file__, "-v"])