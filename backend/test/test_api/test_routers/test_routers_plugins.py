import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import FastAPI
from api.routers.plugins import router, _get_plugin_type


# ======================================================================
# ヘルパー
# ======================================================================

def _make_plugin_class_mock(module: str, doc: str, schema: dict):
    """
        mock_class.__doc__      → description に使われる (クラス側)
        mock_class.__module__   → _get_plugin_type に使われる (クラス側)
        mock_class(params={})   → mock_instance を返す (クラスとして呼ばれる)
        mock_instance.get_parameters_schema() → schema を返す (インスタンス側)
    """
    mock_instance = Mock()
    mock_instance.get_parameters_schema.return_value = schema

    mock_class = Mock()
    mock_class.__doc__ = doc
    mock_class.__module__ = module
    mock_class.return_value = mock_instance  # plugin_class(params={}) → mock_instance

    return mock_class


# ======================================================================
# _get_plugin_type
# ======================================================================
class TestGetPluginType:

    def test_get_plugin_type_extractor(self):
        mock_class = Mock()
        mock_class.__module__ = "plugins.extractors.csv_reader"
        assert _get_plugin_type(mock_class) == "extractor"

    def test_get_plugin_type_cleanser(self):
        mock_class = Mock()
        mock_class.__module__ = "plugins.cleansing.duplicate_remover"
        assert _get_plugin_type(mock_class) == "cleanser"

    def test_get_plugin_type_transformer(self):
        mock_class = Mock()
        mock_class.__module__ = "plugins.transformers.normalizer"
        assert _get_plugin_type(mock_class) == "transformer"

    def test_get_plugin_type_validator(self):
        mock_class = Mock()
        mock_class.__module__ = "plugins.validators.quality_checker"
        assert _get_plugin_type(mock_class) == "validator"

    def test_get_plugin_type_loader(self):
        mock_class = Mock()
        mock_class.__module__ = "plugins.loaders.database_writer"
        assert _get_plugin_type(mock_class) == "loader"

    def test_get_plugin_type_unknown(self):
        mock_class = Mock()
        mock_class.__module__ = "plugins.custom.my_plugin"
        assert _get_plugin_type(mock_class) == "unknown"

    def test_get_plugin_type_with_nested_module(self):
        mock_class = Mock()
        mock_class.__module__ = "my_app.plugins.extractors.advanced.csv_reader"
        assert _get_plugin_type(mock_class) == "extractor"


# ======================================================================
# TestPluginsRouter
# ======================================================================
class TestPluginsRouter:

    @pytest.fixture
    def client(self):
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def mock_framework_manager(self):
        with patch('api.routers.plugins.framework_manager') as mock_manager:
            yield mock_manager

    def test_get_available_plugins_empty(self, client, mock_framework_manager):
        """プラグインが未登録のとき空リストを返す"""
        mock_framework_manager._plugin_class_cache = {}
        response = client.get("/plugins/")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_available_plugins_single_plugin(self, client, mock_framework_manager):
        """プラグインが1件のとき正しく返す"""
        mock_class = _make_plugin_class_mock(
            module="plugins.extractors.test",
            doc="Test plugin description",
            schema={"type": "object", "properties": {"path": {"type": "string"}}}
        )
        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "test_plugin"
        assert data[0]["type"] == "extractor"
        assert data[0]["description"] == "Test plugin description"
        assert data[0]["parameters_schema"]["type"] == "object"

    def test_get_available_plugins_multiple_plugins(self, client, mock_framework_manager):
        """複数プラグインが全件返る"""
        mock_framework_manager._plugin_class_cache = {
            "csv_extractor": _make_plugin_class_mock("plugins.extractors.csv", "CSV extractor", {}),
            "db_loader":     _make_plugin_class_mock("plugins.loaders.db",    "Database loader", {}),
            "normalizer":    _make_plugin_class_mock("plugins.transformers.normalize", "Data normalizer", {}),
        }

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        names = [p["name"] for p in data]
        assert names == sorted(names)

    def test_get_available_plugins_sorted_by_name(self, client, mock_framework_manager):
        """プラグインが名前の昇順でソートされている"""
        mock_framework_manager._plugin_class_cache = {
            "z_plugin": _make_plugin_class_mock("plugins.extractors.z", "Z plugin", {}),
            "a_plugin": _make_plugin_class_mock("plugins.extractors.a", "A plugin", {}),
        }

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["name"] == "a_plugin"
        assert data[1]["name"] == "z_plugin"

    def test_get_available_plugins_without_schema_method(self, client, mock_framework_manager):
        """get_parameters_schema を持たないプラグインはエラースキーマを返す。
        旧実装は hasattr でチェックして空 dict を返していたが、
        新実装は try/except で AttributeError を捕捉しエラースキーマを返す設計になった。
        BasePlugin のサブクラスは get_parameters_schema を abstract method として持つため
        正常なプラグインでこのケースは発生しないが、不正なプラグインへの安全策として動作する。"""
        # spec=[] でメソッドを持たないインスタンスを返すクラスモック
        mock_instance = Mock(spec=[])
        mock_class = Mock()
        mock_class.__doc__ = "Test plugin"
        mock_class.__module__ = "plugins.extractors.test"
        mock_class.return_value = mock_instance

        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        # get_parameters_schema がない → AttributeError → エラースキーマが返る
        assert "error" in data[0]["parameters_schema"]["properties"]
        assert "Could not load schema" in data[0]["parameters_schema"]["properties"]["error"]["default"]

    def test_get_available_plugins_schema_method_raises_exception(self, client, mock_framework_manager):
        """get_parameters_schema が例外を raise したときエラースキーマが返る"""
        mock_instance = Mock()
        mock_instance.get_parameters_schema.side_effect = Exception("Schema error")
        mock_class = Mock()
        mock_class.__doc__ = "Test plugin"
        mock_class.__module__ = "plugins.extractors.test"
        mock_class.return_value = mock_instance

        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert "error" in data[0]["parameters_schema"]["properties"]
        assert "Could not load schema" in data[0]["parameters_schema"]["properties"]["error"]["default"]

    def test_get_available_plugins_without_docstring(self, client, mock_framework_manager):
        """docstring が None のとき 'No description provided.' を返す"""
        mock_class = _make_plugin_class_mock("plugins.extractors.test", None, {})
        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "No description provided."

    def test_get_available_plugins_with_whitespace_docstring(self, client, mock_framework_manager):
        """docstring の前後の空白・改行が strip される"""
        mock_class = _make_plugin_class_mock(
            "plugins.extractors.test", "  Test plugin with spaces  \n", {}
        )
        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "Test plugin with spaces"

    def test_get_available_plugins_complex_schema(self, client, mock_framework_manager):
        """複雑なネスト構造のスキーマがそのまま返る"""
        schema = {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path"},
                "options": {
                    "type": "object",
                    "properties": {
                        "encoding": {"type": "string", "default": "utf-8"},
                        "delimiter": {"type": "string", "default": ","}
                    }
                }
            },
            "required": ["path"]
        }
        mock_class = _make_plugin_class_mock("plugins.extractors.test", "Complex plugin", schema)
        mock_framework_manager._plugin_class_cache = {"complex_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["parameters_schema"]["properties"]["path"]["type"] == "string"
        assert data[0]["parameters_schema"]["required"] == ["path"]
        assert data[0]["parameters_schema"]["properties"]["options"]["properties"]["encoding"]["default"] == "utf-8"

    def test_get_available_plugins_all_types(self, client, mock_framework_manager):
        """全プラグインタイプが正しく分類される"""
        type_module_pairs = [
            ("extractor",    "plugins.extractors.test"),
            ("cleanser",     "plugins.cleansing.test"),
            ("transformer",  "plugins.transformers.test"),
            ("validator",    "plugins.validators.test"),
            ("loader",       "plugins.loaders.test"),
            ("unknown",      "plugins.custom.test"),
        ]
        mock_framework_manager._plugin_class_cache = {
            f"{t}_plugin": _make_plugin_class_mock(m, f"{t} plugin", {})
            for t, m in type_module_pairs
        }

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 6
        plugin_types = {p["name"]: p["type"] for p in data}
        for plugin_type, _ in type_module_pairs:
            assert plugin_types[f"{plugin_type}_plugin"] == plugin_type

    def test_get_available_plugins_response_model(self, client, mock_framework_manager):
        """レスポンスの構造と型が仕様通り"""
        mock_class = _make_plugin_class_mock("plugins.extractors.test", "Test plugin", {"type": "object"})
        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        plugin = response.json()[0]
        assert "name" in plugin
        assert "type" in plugin
        assert "description" in plugin
        assert "parameters_schema" in plugin
        assert isinstance(plugin["name"], str)
        assert isinstance(plugin["type"], str)
        assert isinstance(plugin["description"], str)
        assert isinstance(plugin["parameters_schema"], dict)

    def test_router_prefix_and_tags(self):
        """router の prefix と tags が正しく設定されている"""
        assert router.prefix == "/plugins"
        assert "Plugins" in router.tags

    def test_get_available_plugins_with_unicode_docstring(self, client, mock_framework_manager):
        """Unicode の docstring がそのまま返る"""
        mock_class = _make_plugin_class_mock(
            "plugins.extractors.test", "日本語の説明文です", {}
        )
        mock_framework_manager._plugin_class_cache = {"japanese_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "日本語の説明文です"

    def test_get_available_plugins_empty_string_docstring(self, client, mock_framework_manager):
        """空文字列の docstring は 'No description provided.' を返す"""
        mock_class = _make_plugin_class_mock("plugins.extractors.test", "", {})
        mock_framework_manager._plugin_class_cache = {"test_plugin": mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "No description provided."


# ======================================================================
# TestPluginsRouterEdgeCases
# ======================================================================
class TestPluginsRouterEdgeCases:

    @pytest.fixture
    def client(self):
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def mock_framework_manager(self):
        with patch('api.routers.plugins.framework_manager') as mock_manager:
            yield mock_manager

    def test_get_available_plugins_with_very_long_name(self, client, mock_framework_manager):
        """プラグイン名が非常に長くても正しく返る"""
        long_name = "a" * 1000
        mock_class = _make_plugin_class_mock("plugins.extractors.test", "Test", {})
        mock_framework_manager._plugin_class_cache = {long_name: mock_class}

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["name"] == long_name

    def test_get_available_plugins_mixed_sorting(self, client, mock_framework_manager):
        """数字・アルファベット混在の名前が正しくソートされる"""
        names = ["plugin10", "plugin2", "plugin1", "pluginA", "pluginB"]
        mock_framework_manager._plugin_class_cache = {
            name: _make_plugin_class_mock("plugins.extractors.test", f"{name} description", {})
            for name in names
        }

        response = client.get("/plugins/")

        assert response.status_code == 200
        data = response.json()
        result_names = [p["name"] for p in data]
        assert result_names == sorted(names)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])