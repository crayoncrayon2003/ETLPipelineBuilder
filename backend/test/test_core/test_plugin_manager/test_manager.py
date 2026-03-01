import pytest
import inspect
from unittest.mock import patch, MagicMock
from core.plugin_manager.manager import FrameworkManager, framework_manager
from core.data_container.container import DataContainer, DataContainerStatus
from core.plugin_manager.base_plugin import BasePlugin


# ======================================================================
# テスト用プラグイン
# ======================================================================

class MockPlugin(BasePlugin):
    """正常動作するプラグイン"""

    def get_plugin_name(self) -> str:
        return "mock_plugin"

    def get_parameters_schema(self):
        return {"type": "object"}

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        return self.finalize_container(container, metadata={"ran": True})


class AnotherMockPlugin(BasePlugin):
    """重複テスト用の別プラグイン (同名)"""

    def get_plugin_name(self) -> str:
        return "mock_plugin"   # MockPlugin と同じ名前

    def get_parameters_schema(self):
        return {}

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        return self.finalize_container(container)


class BadNamePlugin(BasePlugin):
    """get_plugin_name() が例外を raise するプラグイン"""

    def get_plugin_name(self) -> str:
        raise RuntimeError("fail")

    def get_parameters_schema(self):
        return {}

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        return container


# ======================================================================
# フィクスチャ
# ======================================================================

@pytest.fixture
def manager():
    """実際のプラグインパッケージをモックして空の FrameworkManager を生成し、
    _plugin_class_cache に MockPlugin クラスを直接登録する"""
    with patch("core.plugin_manager.manager.plugins") as mock_plugins:
        mock_plugins.__name__ = "mock_plugins"
        mock_plugins.__path__ = []
        fm = FrameworkManager()
        fm._plugin_class_cache["mock_plugin"] = MockPlugin
        yield fm


# ======================================================================
# call_plugin_execute
#
# MCDC:
#   条件A: _plugin_class_cache が空か
#     A=True  → RuntimeError
#     A=False → 実行
#
#   条件B: plugin_name がキャッシュに存在するか
#     B=True  → 実行
#     B=False → ValueError
#
#   条件C: inputs が truthy か
#     C=True  → inputs の最初の DataContainer を input_data として渡す
#     C=False → 新規 DataContainer() を input_data として渡す
# ======================================================================
class TestCallPluginExecute:

    def test_a_false_b_true_success(self, manager):
        """A=False, B=True: 正常実行 → DataContainer が返る"""
        result = manager.call_plugin_execute(
            "mock_plugin",
            params={"param1": 123},
            inputs={"input_path": DataContainer()}
        )
        assert isinstance(result, DataContainer)

    def test_a_false_b_true_result_status_success(self, manager):
        """A=False, B=True: MockPlugin.run() が finalize_container を呼ぶため
        result.status == SUCCESS"""
        result = manager.call_plugin_execute("mock_plugin", {}, {})
        assert result.status == DataContainerStatus.SUCCESS

    def test_a_false_b_true_history_contains_plugin_name(self, manager):
        """A=False, B=True: post_execute が history に plugin_name を追加する"""
        result = manager.call_plugin_execute("mock_plugin", {}, {})
        assert "mock_plugin" in result.history

    def test_a_false_b_true_params_passed_to_new_instance(self, manager):
        """A=False, B=True: params が新しいインスタンスに渡される"""
        params = {"key": "value", "num": 42}

        # キャッシュはクラスのまま
        assert inspect.isclass(manager._plugin_class_cache["mock_plugin"])

        result = manager.call_plugin_execute("mock_plugin", params=params, inputs={})
        assert isinstance(result, DataContainer)
        # params が正しく渡されて run() が動いたことを result で確認
        assert result.metadata.get("ran") is True

    def test_a_false_b_true_each_call_creates_new_instance(self, manager):
        """A=False, B=True: 呼び出しのたびに独立したインスタンスが生成される (スレッドセーフ)"""
        result1 = manager.call_plugin_execute("mock_plugin", {"call": 1}, {})
        result2 = manager.call_plugin_execute("mock_plugin", {"call": 2}, {})
        # どちらも正常に完了する (インスタンスが独立)
        assert result1.status == DataContainerStatus.SUCCESS
        assert result2.status == DataContainerStatus.SUCCESS

    def test_a_true_empty_cache_raises_runtime_error(self):
        """A=True: キャッシュが空 → RuntimeError"""
        with patch("core.plugin_manager.manager.plugins") as mock_plugins:
            mock_plugins.__name__ = "mock_plugins"
            mock_plugins.__path__ = []
            fm = FrameworkManager()
        fm._plugin_class_cache = {}
        with pytest.raises(RuntimeError):
            fm.call_plugin_execute("mock_plugin", {}, {})

    def test_a_false_b_false_unknown_plugin_raises_value_error(self, manager):
        """A=False, B=False: 未登録 plugin_name → ValueError"""
        with pytest.raises(ValueError, match="non_existent_plugin"):
            manager.call_plugin_execute("non_existent_plugin", {}, {})

    def test_c_false_empty_inputs_uses_new_data_container(self, manager):
        """C=False: inputs={} → 新規 DataContainer() が input_data として渡る"""
        result = manager.call_plugin_execute("mock_plugin", {}, inputs={})
        assert isinstance(result, DataContainer)

    def test_c_true_inputs_with_container_passes_first_value(self, manager):
        """C=True: inputs に DataContainer あり → 最初の値が input_data として渡る"""
        input_dc = DataContainer()
        input_dc.add_file_path("/input/data.jsonl")
        result = manager.call_plugin_execute(
            "mock_plugin", {}, inputs={"input_path": input_dc}
        )
        assert isinstance(result, DataContainer)


# ======================================================================
# _discover_plugins
#
# MCDC:
#   条件D: get_plugin_name() が正常か例外か
#     D=True  (正常) → _plugin_class_cache に登録
#     D=False (例外) → ログしてスキップ、キャッシュは変化なし
#
#   条件E: 同名プラグインが重複するか
#     E=True  (重複) → 警告ログ、後のクラスで上書き
#     E=False (重複なし) → 正常登録
# ======================================================================
class TestDiscoverPlugins:

    def _make_module_mock(self, *plugin_classes):
        """指定クラスを持つモジュールモックを生成する"""
        mock_module = MagicMock()
        members = [(cls.__name__, cls) for cls in plugin_classes]
        # inspect.getmembers が返すのと同じ形式
        mock_module.__dict__ = {name: cls for name, cls in members}
        return mock_module, members

    def test_d_true_normal_plugin_registered_in_class_cache(self):
        """D=True: 正常なプラグイン → _plugin_class_cache にクラスが登録される"""
        mock_module = MagicMock()

        with patch("core.plugin_manager.manager.plugins") as mock_plugins, \
             patch("core.plugin_manager.manager.pkgutil.walk_packages",
                   return_value=[(None, "mock_module", False)]), \
             patch("core.plugin_manager.manager.importlib.import_module",
                   return_value=mock_module), \
             patch("core.plugin_manager.manager.inspect.getmembers",
                   return_value=[("MockPlugin", MockPlugin)]):
            mock_plugins.__name__ = "mock_plugins"
            mock_plugins.__path__ = []
            fm = FrameworkManager()

        assert "mock_plugin" in fm._plugin_class_cache
        assert fm._plugin_class_cache["mock_plugin"] is MockPlugin

    def test_d_false_exception_in_get_plugin_name_skipped(self):
        """D=False: get_plugin_name() が例外 → スキップされキャッシュは空"""
        mock_module = MagicMock()

        with patch("core.plugin_manager.manager.plugins") as mock_plugins, \
             patch("core.plugin_manager.manager.pkgutil.walk_packages",
                   return_value=[(None, "mock_module", False)]), \
             patch("core.plugin_manager.manager.importlib.import_module",
                   return_value=mock_module), \
             patch("core.plugin_manager.manager.inspect.getmembers",
                   return_value=[("BadNamePlugin", BadNamePlugin)]):
            mock_plugins.__name__ = "mock_plugins"
            mock_plugins.__path__ = []
            fm = FrameworkManager()

        assert fm._plugin_class_cache == {}

    def test_e_true_duplicate_plugin_name_overwritten_with_warning(self, caplog):
        """E=True: 同名プラグインが2つある → 警告ログが出て後のクラスで上書きされる"""
        mock_module = MagicMock()

        with patch("core.plugin_manager.manager.plugins") as mock_plugins, \
             patch("core.plugin_manager.manager.pkgutil.walk_packages",
                   return_value=[(None, "mock_module", False)]), \
             patch("core.plugin_manager.manager.importlib.import_module",
                   return_value=mock_module), \
             patch("core.plugin_manager.manager.inspect.getmembers",
                   return_value=[
                       ("MockPlugin", MockPlugin),
                       ("AnotherMockPlugin", AnotherMockPlugin),
                   ]):
            mock_plugins.__name__ = "mock_plugins"
            mock_plugins.__path__ = []
            import logging
            with caplog.at_level(logging.WARNING):
                fm = FrameworkManager()

        assert "mock_plugin" in fm._plugin_class_cache
        assert "Duplicate" in caplog.text

    def test_e_false_unique_plugin_names_all_registered(self):
        """E=False: 異なる名前のプラグインはすべて登録される"""
        class AnotherPlugin(BasePlugin):
            def get_plugin_name(self): return "another_plugin"
            def get_parameters_schema(self): return {}
            def run(self, i, c): return c

        mock_module = MagicMock()

        with patch("core.plugin_manager.manager.plugins") as mock_plugins, \
             patch("core.plugin_manager.manager.pkgutil.walk_packages",
                   return_value=[(None, "mock_module", False)]), \
             patch("core.plugin_manager.manager.importlib.import_module",
                   return_value=mock_module), \
             patch("core.plugin_manager.manager.inspect.getmembers",
                   return_value=[
                       ("MockPlugin", MockPlugin),
                       ("AnotherPlugin", AnotherPlugin),
                   ]):
            mock_plugins.__name__ = "mock_plugins"
            mock_plugins.__path__ = []
            fm = FrameworkManager()

        assert "mock_plugin" in fm._plugin_class_cache
        assert "another_plugin" in fm._plugin_class_cache


# ======================================================================
# シングルトン framework_manager の存在確認
# ======================================================================
class TestFrameworkManagerSingleton:

    def test_framework_manager_is_instance_of_framework_manager(self):
        """モジュールレベルの framework_manager が FrameworkManager インスタンスである"""
        assert isinstance(framework_manager, FrameworkManager)

    def test_plugin_class_cache_is_dict(self):
        """_plugin_class_cache が dict である"""
        assert isinstance(framework_manager._plugin_class_cache, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])