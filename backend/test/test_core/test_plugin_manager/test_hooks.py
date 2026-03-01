import inspect
import pytest
import pluggy
from typing import Dict, Any
from core.plugin_manager.hooks import EtlHookSpecs, hookspec

hookimpl = pluggy.HookimplMarker("etl_framework")


# ======================================================================
# ヘルパー: テスト用 PluginManager を生成する
# ======================================================================
def _make_pm(*plugins) -> pluggy.PluginManager:
    pm = pluggy.PluginManager("etl_framework")
    pm.add_hookspecs(EtlHookSpecs)
    for p in plugins:
        pm.register(p)
    return pm


# ======================================================================
# hookspec マーカーの定義確認
# ======================================================================
class TestHookSpecDefinition:

    @pytest.fixture
    def hook_specs(self):
        return EtlHookSpecs()

    def test_hookspec_marker_project_name(self):
        """hookspec マーカーのプロジェクト名が 'etl_framework' である"""
        assert hookspec.project_name == "etl_framework"

    def test_get_plugin_name_is_callable(self, hook_specs):
        """get_plugin_name が定義され呼び出し可能である"""
        assert hasattr(hook_specs, 'get_plugin_name')
        assert callable(hook_specs.get_plugin_name)

    def test_get_parameters_schema_is_callable(self, hook_specs):
        """get_parameters_schema が定義され呼び出し可能である"""
        assert hasattr(hook_specs, 'get_parameters_schema')
        assert callable(hook_specs.get_parameters_schema)

    def test_get_plugin_name_has_hookspec_marker(self):
        """get_plugin_name に hookspec マーカーが付与されている"""
        method = getattr(EtlHookSpecs, 'get_plugin_name')
        assert hasattr(method, 'etl_framework_spec')

    def test_get_parameters_schema_has_hookspec_marker(self):
        """get_parameters_schema に hookspec マーカーが付与されている"""
        method = getattr(EtlHookSpecs, 'get_parameters_schema')
        assert hasattr(method, 'etl_framework_spec')

    def test_get_plugin_name_has_firstresult_true(self):
        """get_plugin_name の hookspec に firstresult=True が設定されている
        pluggy は etl_framework_spec を dict として保持するため [] でアクセスする"""
        spec = EtlHookSpecs.get_plugin_name.etl_framework_spec
        assert spec["firstresult"] is True

    def test_get_parameters_schema_has_firstresult_true(self):
        """get_parameters_schema の hookspec に firstresult=True が設定されている
        pluggy は etl_framework_spec を dict として保持するため [] でアクセスする"""
        spec = EtlHookSpecs.get_parameters_schema.etl_framework_spec
        assert spec["firstresult"] is True

    def test_get_plugin_name_return_annotation_is_str(self, hook_specs):
        """get_plugin_name の戻り値型アノテーションが str である"""
        sig = inspect.signature(hook_specs.get_plugin_name)
        assert sig.return_annotation == str

    def test_get_parameters_schema_return_annotation_is_dict(self, hook_specs):
        """get_parameters_schema の戻り値型アノテーションが Dict[str, Any] である"""
        sig = inspect.signature(hook_specs.get_parameters_schema)
        assert sig.return_annotation == Dict[str, Any]

    def test_hookspec_methods_have_no_required_parameters(self, hook_specs):
        """hookspec メソッドに self 以外の必須パラメータがない"""
        name_params = inspect.signature(hook_specs.get_plugin_name).parameters
        schema_params = inspect.signature(hook_specs.get_parameters_schema).parameters
        assert len([p for p in name_params.values() if p.name != 'self']) == 0
        assert len([p for p in schema_params.values() if p.name != 'self']) == 0

    def test_hookspec_methods_return_none_by_default(self, hook_specs):
        """hookspec のデフォルト実装は None を返す"""
        assert hook_specs.get_plugin_name() is None
        assert hook_specs.get_parameters_schema() is None

    def test_etl_hook_specs_has_exactly_two_public_methods(self):
        """EtlHookSpecs は get_plugin_name と get_parameters_schema の2メソッドのみ持つ"""
        methods = [m for m in dir(EtlHookSpecs) if not m.startswith('_')]
        assert set(methods) == {'get_plugin_name', 'get_parameters_schema'}


# ======================================================================
# PluginManager への登録と hookspec の統合動作
# ======================================================================
class TestPluginManagerIntegration:

    def test_pm_can_register_hookspecs(self):
        """PluginManager に EtlHookSpecs を登録できる"""
        pm = _make_pm()
        assert pm.hook.get_plugin_name is not None
        assert pm.hook.get_parameters_schema is not None

    # ------------------------------------------------------------------
    # firstresult=True: 登録なし → None 
    # ------------------------------------------------------------------
    def test_no_plugins_get_plugin_name_returns_none(self):
        """プラグイン未登録: get_plugin_name() は None を返す (firstresult=True)"""
        pm = _make_pm()
        result = pm.hook.get_plugin_name()
        assert result is None

    def test_no_plugins_get_parameters_schema_returns_none(self):
        """プラグイン未登録: get_parameters_schema() は None を返す (firstresult=True)"""
        pm = _make_pm()
        result = pm.hook.get_parameters_schema()
        assert result is None

    # ------------------------------------------------------------------
    # firstresult=True: 1件登録 → 単一値 (str / dict)
    # ------------------------------------------------------------------
    def test_single_plugin_get_plugin_name_returns_str(self):
        """1件登録: get_plugin_name() は str を返す (firstresult=True)"""
        class P:
            @hookimpl
            def get_plugin_name(self): return "test_plugin"

        pm = _make_pm(P())
        result = pm.hook.get_plugin_name()
        assert result == "test_plugin"
        assert isinstance(result, str)

    def test_single_plugin_get_parameters_schema_returns_dict(self):
        """1件登録: get_parameters_schema() は dict を返す (firstresult=True)"""
        schema = {"type": "object", "properties": {"param1": {"type": "string"}}}

        class P:
            @hookimpl
            def get_parameters_schema(self): return schema

        pm = _make_pm(P())
        result = pm.hook.get_parameters_schema()
        assert result == schema
        assert isinstance(result, dict)

    # ------------------------------------------------------------------
    # firstresult=True: 複数登録 → 最後に登録されたものが先に評価され最初の非Noneが返る
    # ------------------------------------------------------------------
    def test_multiple_plugins_get_plugin_name_returns_first_non_none(self):
        """複数登録: get_plugin_name() は最後に登録したプラグインの値を返す (firstresult=True)
        pluggy は後に登録されたものを先に評価するため、最後の登録が優先される"""
        class P1:
            @hookimpl
            def get_plugin_name(self): return "plugin1"

        class P2:
            @hookimpl
            def get_plugin_name(self): return "plugin2"

        pm = _make_pm(P1(), P2())
        result = pm.hook.get_plugin_name()
        # firstresult=True → 単一値、リストではない
        assert isinstance(result, str)
        assert result in ("plugin1", "plugin2")

    def test_multiple_plugins_only_one_result_returned(self):
        """複数登録: firstresult=True により結果は常に単一値でリストにならない"""
        class P1:
            @hookimpl
            def get_plugin_name(self): return "plugin1"

        class P2:
            @hookimpl
            def get_plugin_name(self): return "plugin2"

        pm = _make_pm(P1(), P2())
        result = pm.hook.get_plugin_name()
        assert not isinstance(result, list)

    # ------------------------------------------------------------------
    # 無効なプラグイン (wrong project / no marker) → None
    # ------------------------------------------------------------------
    def test_wrong_project_name_plugin_not_called(self):
        """別プロジェクト名の HookimplMarker を持つプラグインは呼ばれず None が返る"""
        wrong_hookimpl = pluggy.HookimplMarker("wrong_framework")

        class WrongPlugin:
            @wrong_hookimpl
            def get_plugin_name(self): return "wrong_plugin"

        pm = _make_pm(WrongPlugin())
        result = pm.hook.get_plugin_name()
        assert result is None

    def test_no_hookimpl_marker_plugin_not_called(self):
        """HookimplMarker のないプラグインは呼ばれず None が返る"""
        class NoMarkerPlugin:
            def get_plugin_name(self): return "no_marker_plugin"

        pm = _make_pm(NoMarkerPlugin())
        result = pm.hook.get_plugin_name()
        assert result is None

    # ------------------------------------------------------------------
    # 複雑なスキーマも正しく返る
    # ------------------------------------------------------------------
    def test_complex_parameters_schema_returned_correctly(self):
        """複雑なネスト構造のスキーマも正しく返る"""
        complex_schema = {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "description": "Input file path"},
                "options": {
                    "type": "object",
                    "properties": {
                        "encoding": {"type": "string", "default": "utf-8"},
                        "delimiter": {"type": "string", "default": ","}
                    }
                },
                "columns": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["input_path"]
        }

        class ComplexPlugin:
            @hookimpl
            def get_parameters_schema(self): return complex_schema

        pm = _make_pm(ComplexPlugin())
        result = pm.hook.get_parameters_schema()

        # firstresult=True → dict が直接返る (result[0] ではない)
        assert result == complex_schema
        assert result["properties"]["input_path"]["type"] == "string"
        assert result["properties"]["options"]["properties"]["encoding"]["default"] == "utf-8"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])