import pytest
from unittest.mock import MagicMock, patch
from core.data_container.container import DataContainer, DataContainerStatus
from scripts.core.plugin_manager.base_plugin import BasePlugin


# ======================================================================
# テスト用サブクラス
# ======================================================================

class NormalPlugin(BasePlugin):
    """run() が finalize_container() を呼ぶ正しい実装"""

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        return self.finalize_container(container, metadata={"ran": True})

    def get_plugin_name(self) -> str:
        return "NormalPlugin"

    def get_parameters_schema(self):
        return {"type": "object"}


class ExceptionPlugin(BasePlugin):
    """run() が例外を raise する実装"""

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        raise RuntimeError("run failed")

    def get_plugin_name(self) -> str:
        return "ExceptionPlugin"

    def get_parameters_schema(self):
        return {}


class PrevExceptionPlugin(BasePlugin):
    """prev_execute() が例外を raise する実装"""

    def prev_execute(self, input_data: DataContainer, container: DataContainer) -> None:
        raise RuntimeError("prev_execute failed")

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        return self.finalize_container(container)

    def get_plugin_name(self) -> str:
        return "PrevExceptionPlugin"

    def get_parameters_schema(self):
        return {}


class MutatingPlugin(BasePlugin):
    """input_data を変更する (悪い実装の例: 動作確認用)"""

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_data.add_error("mutated by plugin")   # input_data を変更
        return self.finalize_container(container)

    def get_plugin_name(self) -> str:
        return "MutatingPlugin"

    def get_parameters_schema(self):
        return {}


# ======================================================================
# __init__ / set_params
# MCDC:
#   条件E: params が truthy か (None or {} → {}, dict → そのまま)
# ======================================================================
class TestInit:

    def test_e_true_params_set(self):
        """E=True: params が dict → そのまま保持される"""
        plugin = NormalPlugin(params={"key": "value"})
        assert plugin.params == {"key": "value"}

    def test_e_false_params_none_becomes_empty_dict(self):
        """E=False: params=None → {} に変換される"""
        plugin = NormalPlugin(params=None)
        assert plugin.params == {}

    def test_e_false_params_empty_dict(self):
        """E=False: params={} → {} のまま"""
        plugin = NormalPlugin(params={})
        assert plugin.params == {}


class TestSetParams:

    def test_set_params_with_dict(self):
        """params を dict で更新できる"""
        plugin = NormalPlugin(params={})
        plugin.set_params({"new_key": 42})
        assert plugin.params == {"new_key": 42}

    def test_set_params_with_none_becomes_empty_dict(self):
        """params=None → {} に変換される"""
        plugin = NormalPlugin(params={"old": "data"})
        plugin.set_params(None)
        assert plugin.params == {}


# ======================================================================
# execute()
# MCDC:
#   条件A: run() が正常終了か例外か
#   条件B: prev_execute() が正常終了か例外か
# ======================================================================
class TestExecute:

    @pytest.fixture
    def input_dc(self):
        dc = DataContainer()
        dc.add_file_path("/input/data.jsonl")
        return dc

    def test_a_true_b_true_success(self, input_dc):
        """A=True, B=True: 正常実行 → SUCCESS が返る"""
        plugin = NormalPlugin(params={})
        result = plugin.execute(input_dc)

        assert isinstance(result, DataContainer)
        assert result.status == DataContainerStatus.SUCCESS

    def test_a_true_b_true_history_added_by_post_execute(self, input_dc):
        """A=True, B=True: post_execute() が history に plugin_name を追加する"""
        plugin = NormalPlugin(params={})
        result = plugin.execute(input_dc)

        assert "NormalPlugin" in result.history

    def test_a_false_run_raises_returns_error_container(self, input_dc):
        """A=False: run() が例外 → 新規 error_container が返る"""
        plugin = ExceptionPlugin(params={})
        result = plugin.execute(input_dc)

        assert isinstance(result, DataContainer)
        assert result.status == DataContainerStatus.ERROR
        assert any("run failed" in e for e in result.errors)

    def test_a_false_error_container_is_new_object(self, input_dc):
        """A=False: 返る error_container は execute() 内の container とは別オブジェクト
        → run() が変更途中だった container が次のステップに流れない"""
        plugin = ExceptionPlugin(params={})
        result = plugin.execute(input_dc)

        # エラー情報のみ含む新規オブジェクトであること
        assert result.file_paths == []
        assert result.history == []
        assert result.data is None

    def test_b_false_prev_execute_raises_returns_error_container(self, input_dc):
        """B=False: prev_execute() が例外 → error_container が返る"""
        plugin = PrevExceptionPlugin(params={})
        result = plugin.execute(input_dc)

        assert result.status == DataContainerStatus.ERROR
        assert any("prev_execute failed" in e for e in result.errors)

    def test_input_data_is_not_modified_by_execute(self):
        """input_data は execute() を通じても変更されない (読み取り専用の意図確認)
        注: StepExecutor が deepcopy して渡すため通常は保護されるが
            base_plugin 単体でも変更しない実装であることを確認する"""
        input_dc = DataContainer()
        input_dc.add_file_path("/original/path.jsonl")
        original_paths = list(input_dc.file_paths)
        original_errors = list(input_dc.errors)

        plugin = NormalPlugin(params={})
        plugin.execute(input_dc)

        assert input_dc.file_paths == original_paths
        assert input_dc.errors == original_errors

    def test_mutating_plugin_does_not_affect_caller_input(self):
        """悪い実装 (input_data を変更) でも StepExecutor の deepcopy があれば
        呼び出し元の DataContainer は保護される (設計意図の確認)
        ※ このテストは base_plugin 単体では input_data が破壊されることを示す
           → StepExecutor の deepcopy が必要な理由"""
        input_dc = DataContainer()
        original_errors = list(input_dc.errors)

        plugin = MutatingPlugin(params={})
        plugin.execute(input_dc)  # input_dc が変更される

        # base_plugin 単体では input_data への変更を防げない
        assert "mutated by plugin" in input_dc.errors, (
            "base_plugin 単体では input_data の変更を防げない。"
            "StepExecutor の deepcopy による保護が必要。"
        )


# ======================================================================
# finalize_container()
# MCDC:
#   条件C: output_path が truthy か
#   条件D: metadata が truthy か
# ======================================================================
class TestFinalizeContainer:

    @pytest.fixture
    def plugin(self):
        return NormalPlugin(params={})

    @pytest.fixture
    def container(self):
        return DataContainer()

    def test_c_true_d_true_both_set(self, plugin, container):
        """C=True, D=True: output_path と metadata が両方設定される"""
        result = plugin.finalize_container(
            container,
            output_path="/tmp/output.csv",
            metadata={"key": "value"}
        )
        assert result.status == DataContainerStatus.SUCCESS
        assert "/tmp/output.csv" in result.file_paths
        assert result.metadata.get("key") == "value"

    def test_c_true_d_false_only_path_set(self, plugin, container):
        """C=True, D=False: output_path のみ設定、metadata は変わらない"""
        result = plugin.finalize_container(
            container,
            output_path="/tmp/output.csv",
            metadata=None
        )
        assert "/tmp/output.csv" in result.file_paths
        assert result.metadata == {}

    def test_c_false_d_true_only_metadata_set(self, plugin, container):
        """C=False, D=True: metadata のみ設定、file_paths は増えない"""
        result = plugin.finalize_container(
            container,
            output_path=None,
            metadata={"source": "db"}
        )
        assert result.file_paths == []
        assert result.metadata.get("source") == "db"

    def test_c_false_d_false_only_status_set(self, plugin, container):
        """C=False, D=False: status=SUCCESS のみ、他は変化なし"""
        result = plugin.finalize_container(container)
        assert result.status == DataContainerStatus.SUCCESS
        assert result.file_paths == []
        assert result.metadata == {}

    def test_d_true_external_metadata_dict_not_mutated(self, plugin, container):
        """D=True: 呼び出し元の metadata dict は変更されない"""
        external_meta = {"source": "db", "version": 1}
        plugin.finalize_container(container, metadata=external_meta)

        # 呼び出し元の dict は変化しない
        assert external_meta == {"source": "db", "version": 1}

    def test_d_true_metadata_merged_with_existing(self, plugin):
        """D=True: 既存の metadata とマージされる"""
        container = DataContainer(metadata={"existing": "kept"})
        plugin.finalize_container(container, metadata={"new": "added"})
        assert container.metadata["existing"] == "kept"
        assert container.metadata["new"] == "added"

    def test_returns_same_container_object(self, plugin, container):
        """finalize_container は引数の container をそのまま返す"""
        result = plugin.finalize_container(container)
        assert result is container


# ======================================================================
# get_plugin_name / get_parameters_schema (抽象メソッドの実装確認)
# ======================================================================
class TestAbstractMethods:

    def test_get_plugin_name(self):
        plugin = NormalPlugin(params={})
        assert plugin.get_plugin_name() == "NormalPlugin"

    def test_get_parameters_schema(self):
        plugin = NormalPlugin(params={})
        schema = plugin.get_parameters_schema()
        assert isinstance(schema, dict)

    def test_cannot_instantiate_base_plugin_directly(self):
        """BasePlugin は抽象クラスのため直接インスタンス化できない"""
        with pytest.raises(TypeError):
            BasePlugin(params={})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])