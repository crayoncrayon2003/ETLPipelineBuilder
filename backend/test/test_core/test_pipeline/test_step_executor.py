import copy
import pytest
from unittest.mock import Mock, patch, ANY
from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.step_executor import StepExecutor


# ======================================================================
# ヘルパー
# ======================================================================

def _make_container(*file_paths: str) -> Mock:
    """指定したファイルパスを返す DataContainer モックを作成する"""
    c = Mock(spec=DataContainer)
    c.get_file_paths.return_value = list(file_paths)
    return c


def _called_params(mock_call) -> dict:
    """mock_call の実際の params 引数を返す"""
    return mock_call.call_args[1]['params']


def _called_inputs(mock_call) -> dict:
    """mock_call の実際の inputs 引数を返す"""
    return mock_call.call_args[1]['inputs']


PATCH_TARGET = 'core.plugin_manager.manager.framework_manager.call_plugin_execute'


# ======================================================================
# StepExecutor
#
# MCDC 条件:
#   条件A: plugin_name が truthy か
#     A=True  → 実行
#     A=False → ValueError
#
#   条件B: params が None か (キーあり値 None)
#     B=True  (None)  → {} として扱う
#     B=False (dict)  → deepcopy して使う
#
#   条件C: inputs が truthy か
#     C=True  → ファイルパスを resolved_params に展開
#     C=False → 展開しない
#
#   条件D: container が truthy か (inputs の各値)
#     D=True  → get_file_paths() を呼ぶ
#     D=False → スキップ
#
#   条件E: len(file_paths) == 0
#     E=True  → warning ログのみ、params に追加しない
#
#   条件F: len(file_paths) == 1
#     F=True  → resolved_params[input_name] = file_paths[0]  (文字列)
#     F=False (>1) → resolved_params[input_name] = file_paths (リスト)
#
#   【仕様】inputs のキー名は変換せずそのまま params キーになる。
#           inputs={"input_path": container} → params["input_path"] = "/path/to/file"
#           inputs を deepcopy して safe_inputs として call_plugin_execute に渡す。
# ======================================================================
class TestStepExecutor:

    @pytest.fixture
    def executor(self):
        return StepExecutor()

    @pytest.fixture
    def mock_output(self):
        return Mock(spec=DataContainer)

    # ------------------------------------------------------------------
    # 条件A: plugin_name の有無
    # ------------------------------------------------------------------

    def test_a_false_missing_plugin_raises_value_error(self, executor):
        """A=False: 'plugin' キーなし → ValueError"""
        with pytest.raises(ValueError, match="'plugin' key is missing"):
            executor.execute_step({'params': {}})

    def test_a_false_empty_plugin_raises_value_error(self, executor):
        """A=False: plugin='' → ValueError"""
        with pytest.raises(ValueError):
            executor.execute_step({'plugin': '', 'params': {}})

    def test_a_false_none_plugin_raises_value_error(self, executor):
        """A=False: plugin=None → ValueError"""
        with pytest.raises(ValueError):
            executor.execute_step({'plugin': None, 'params': {}})

    # ------------------------------------------------------------------
    # 条件B: params=None のとき空 dict として扱う
    # ------------------------------------------------------------------

    def test_b_true_params_none_treated_as_empty_dict(self, executor, mock_output):
        """B=True: params=None → 空 dict として call_plugin_execute に渡る"""
        step_config = {'plugin': 'test_plugin', 'params': None}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config)
            assert _called_params(mock_call) == {}

    def test_b_false_params_dict_passed_correctly(self, executor, mock_output):
        """B=False: params が dict → deepcopy されて渡る"""
        step_config = {'plugin': 'test_plugin', 'params': {'key': 'value'}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config)
            assert _called_params(mock_call) == {'key': 'value'}

    def test_b_false_original_params_not_mutated(self, executor, mock_output):
        """B=False: step_config['params'] は deepcopy されるため元 dict が変更されない"""
        nested = {'nested': {'key': 'original'}}
        step_config = {'plugin': 'test_plugin', 'params': nested}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config)
        assert nested == {'nested': {'key': 'original'}}

    # ------------------------------------------------------------------
    # 条件C: inputs なし
    # ------------------------------------------------------------------

    def test_c_false_no_inputs(self, executor, mock_output):
        """C=False: inputs=None → params はそのまま"""
        step_config = {'plugin': 'test_plugin', 'params': {'key': 'val'}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            result = executor.execute_step(step_config, inputs=None)
        assert result == mock_output
        assert _called_params(mock_call) == {'key': 'val'}

    def test_c_false_empty_inputs_dict(self, executor, mock_output):
        """C=False: inputs={} → params はそのまま"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={})
        assert _called_params(mock_call) == {}

    # ------------------------------------------------------------------
    # 条件D: container が None / falsy のとき
    # ------------------------------------------------------------------

    def test_d_false_none_container_skipped(self, executor, mock_output):
        """D=False: inputs の値が None → スキップされ params に追加されない"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': None})
        assert 'input_path' not in _called_params(mock_call)

    # ------------------------------------------------------------------
    # 条件E: file_paths が空
    # ------------------------------------------------------------------

    def test_e_true_empty_file_paths_not_added_to_params(self, executor, mock_output):
        """E=True: file_paths=[] → warning のみ、params に追加されない"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        container = _make_container()   # パスなし
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': container})
        assert 'input_path' not in _called_params(mock_call)

    # ------------------------------------------------------------------
    # 条件F: file_paths が 1件 vs 複数件
    # 【仕様】inputs のキー名 = params のキー名 (変換なし)
    # ------------------------------------------------------------------

    def test_f_true_single_file_path_set_as_string(self, executor, mock_output):
        """F=True: file_paths が 1件 → params[input_name] に文字列でセットされる
        【仕様】inputs={"input_path": container} → params["input_path"] = "/path/to/file.csv"
        """
        step_config = {
            'plugin': 'test_plugin',
            'params': {'param1': 'value1'},
            'name': 'test_step'
        }
        container = _make_container('/path/to/file.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            result = executor.execute_step(step_config, inputs={'input_path': container})

        assert result == mock_output
        assert _called_params(mock_call) == {
            'param1': 'value1',
            'input_path': '/path/to/file.csv',   # キー名はそのまま
        }

    def test_f_false_multiple_file_paths_set_as_list(self, executor, mock_output):
        """F=False: file_paths が複数 → params[input_name] にリストでセットされる"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        file_paths = ['/path/to/file1.csv', '/path/to/file2.csv']
        container = _make_container(*file_paths)
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': container})

        assert _called_params(mock_call) == {'input_path': file_paths}

    # ------------------------------------------------------------------
    # キー名がそのまま params に反映されること (複数・カスタム名)
    # ------------------------------------------------------------------

    def test_custom_input_name_used_as_param_key(self, executor, mock_output):
        """inputs のキー名がそのまま params キーになる (変換・サフィックス付加なし)
        旧仕様: inputs["custom_input"] → params["custom_input_path"]  ← 廃止
        新仕様: inputs["custom_input"] → params["custom_input"]
        """
        step_config = {'plugin': 'test_plugin', 'params': {}}
        container = _make_container('/path/to/custom.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'custom_input': container})

        assert _called_params(mock_call) == {'custom_input': '/path/to/custom.csv'}

    def test_multiple_inputs_each_key_used_as_param_key(self, executor, mock_output):
        """複数 inputs: それぞれのキー名がそのまま params キーになる"""
        step_config = {'plugin': 'test_plugin', 'params': {'param1': 'value1'}}
        container1 = _make_container('/path/to/input1.csv')
        container2 = _make_container('/path/to/input2.csv')
        inputs = {
            'input_path':     container1,
            'reference_path': container2,
        }
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs=inputs)

        called = _called_params(mock_call)
        assert called['param1'] == 'value1'
        assert called['input_path'] == '/path/to/input1.csv'
        assert called['reference_path'] == '/path/to/input2.csv'

    def test_input_key_overwrites_same_named_param(self, executor, mock_output):
        """inputs のキーと params のキーが同名の場合、inputs の値で上書きされる
        例: params={"input_path": "/original"}, inputs={"input_path": container}
            → params["input_path"] = container の file_paths[0]
        """
        step_config = {
            'plugin': 'test_plugin',
            'params': {'input_path': '/original/path.csv'},
        }
        container = _make_container('/new/path.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': container})

        assert _called_params(mock_call)['input_path'] == '/new/path.csv'

    # ------------------------------------------------------------------
    # safe_inputs (deepcopy) の検証
    # inputs は deepcopy されて call_plugin_execute に渡るため
    # 元オブジェクトとの同一性ではなく内容で比較する
    # ------------------------------------------------------------------

    def test_safe_inputs_passed_to_call_plugin_execute(self, executor, mock_output):
        """inputs は deepcopy されて渡るため、元オブジェクトとは別インスタンス
        内容（file_paths）は等しい"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        original_dc = DataContainer()
        original_dc.add_file_path('/path/to/file.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': original_dc})

        safe_inputs = _called_inputs(mock_call)
        # 別インスタンス (deepcopy)
        assert safe_inputs['input_path'] is not original_dc
        # 内容は同じ
        assert safe_inputs['input_path'].get_file_paths() == ['/path/to/file.csv']

    def test_original_input_container_not_mutated_by_plugin(self, executor, mock_output):
        """safe_inputs が deepcopy であるため、plugin が inputs を変更しても
        呼び出し元の DataContainer は影響を受けない"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        original_dc = DataContainer()
        original_dc.add_file_path('/path/to/file.csv')

        def mutating_call(**kwargs):
            # plugin が safe_inputs を変更しても元 container は保護される
            kwargs['inputs']['input_path'].add_error('mutated')
            return mock_output

        with patch(PATCH_TARGET, side_effect=mutating_call):
            executor.execute_step(step_config, inputs={'input_path': original_dc})

        assert original_dc.errors == []   # 元は変更されていない

    # ------------------------------------------------------------------
    # step_name フォールバック
    # ------------------------------------------------------------------

    def test_step_name_defaults_to_plugin_name(self, executor, mock_output):
        """'name' キーがない場合は plugin_name が step_name になる (ログ確認)"""
        step_config = {'plugin': 'my_plugin', 'params': {}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config)
        mock_call.assert_called_once_with(
            plugin_name='my_plugin',
            params={},
            inputs={}
        )

    # ------------------------------------------------------------------
    # 例外の re-raise
    # ------------------------------------------------------------------

    def test_exception_from_plugin_is_reraised(self, executor):
        """call_plugin_execute が例外を raise → そのまま re-raise される"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        with patch(PATCH_TARGET, side_effect=RuntimeError("plugin error")):
            with pytest.raises(RuntimeError, match="plugin error"):
                executor.execute_step(step_config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])