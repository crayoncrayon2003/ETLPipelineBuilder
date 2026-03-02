import copy
import pytest
from unittest.mock import Mock, patch, ANY
from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.step_executor import StepExecutor


# ======================================================================
# ヘルパー
# ======================================================================

def _make_container(*file_paths: str) -> DataContainer:
    """指定したファイルパスを持つ DataContainer を作成する"""
    c = DataContainer()
    for p in file_paths:
        c.add_file_path(p)
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
#   条件C: inputs が None か空 dict か
#     C=False (None/{}) → safe_inputs={} として渡す
#     C=True  (dict あり) → deepcopy して safe_inputs として渡す
#
#   【仕様】
#     inputs の DataContainer はパスに展開しない。
#     resolved_params への注入は行わない。
#     inputs は deepcopy して safe_inputs として call_plugin_execute に渡す。
#     params のパス指定は呼び出し元が明示的に行う責任を持つ。
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
    # 条件C: inputs なし → safe_inputs={} で渡る
    # ------------------------------------------------------------------

    def test_c_false_no_inputs_params_passed_as_is(self, executor, mock_output):
        """C=False: inputs=None → params はそのまま、safe_inputs={} で渡る"""
        step_config = {'plugin': 'test_plugin', 'params': {'key': 'val'}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            result = executor.execute_step(step_config, inputs=None)
        assert result == mock_output
        assert _called_params(mock_call) == {'key': 'val'}
        assert _called_inputs(mock_call) == {}

    def test_c_false_empty_inputs_dict(self, executor, mock_output):
        """C=False: inputs={} → params はそのまま、safe_inputs={} で渡る"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={})
        assert _called_params(mock_call) == {}
        assert _called_inputs(mock_call) == {}

    # ------------------------------------------------------------------
    # 【新仕様】inputs の DataContainer は params に展開しない
    # ------------------------------------------------------------------

    def test_inputs_container_not_expanded_into_params(self, executor, mock_output):
        """inputs に DataContainer を渡しても resolved_params には展開されない。
        params のパス指定は呼び出し元が明示的に行う責任を持つ。"""
        step_config = {
            'plugin': 'test_plugin',
            'params': {'output_path': '/out/result.parquet'},
        }
        container = _make_container('/path/to/input.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': container})

        called = _called_params(mock_call)
        # input_path は params に展開されない
        assert 'input_path' not in called
        # params に明示した値はそのまま渡る
        assert called['output_path'] == '/out/result.parquet'

    def test_inputs_container_not_expanded_multiple_file_paths(self, executor, mock_output):
        """複数 file_paths の DataContainer も params に展開されない"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        container = _make_container('/path/file1.csv', '/path/file2.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': container})

        assert 'input_path' not in _called_params(mock_call)

    def test_explicit_params_used_as_is_regardless_of_inputs(self, executor, mock_output):
        """params に明示した input_path は inputs があっても上書きされない。
        呼び出し元が params で指定した値が常に使われる。"""
        step_config = {
            'plugin': 'test_plugin',
            'params': {'input_path': '/explicit/path.csv'},
        }
        container = _make_container('/container/path.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': container})

        # params の明示値が保持される (inputs で上書きされない)
        assert _called_params(mock_call)['input_path'] == '/explicit/path.csv'

    # ------------------------------------------------------------------
    # safe_inputs (deepcopy) の検証
    # inputs は DataContainer のまま deepcopy されて渡る
    # ------------------------------------------------------------------

    def test_safe_inputs_passed_as_container_not_path(self, executor, mock_output):
        """inputs は DataContainer のまま deepcopy されて call_plugin_execute に渡る。
        パス文字列ではなく DataContainer インスタンスが safe_inputs の値になる。"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        original_dc = _make_container('/path/to/file.csv')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(step_config, inputs={'input_path': original_dc})

        safe_inputs = _called_inputs(mock_call)
        # DataContainer インスタンスとして渡る
        assert isinstance(safe_inputs['input_path'], DataContainer)
        # deepcopy のため別インスタンス
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
            kwargs['inputs']['input_path'].add_error('mutated')
            return mock_output

        with patch(PATCH_TARGET, side_effect=mutating_call):
            executor.execute_step(step_config, inputs={'input_path': original_dc})

        assert original_dc.errors == []

    def test_multiple_inputs_all_passed_as_containers(self, executor, mock_output):
        """複数 inputs が全て DataContainer のまま safe_inputs に渡る"""
        step_config = {'plugin': 'test_plugin', 'params': {}}
        container1 = _make_container('/path/input1.csv')
        container2 = _make_container('/path/input2.parquet')
        with patch(PATCH_TARGET) as mock_call:
            mock_call.return_value = mock_output
            executor.execute_step(
                step_config,
                inputs={'input_path': container1, 'reference_path': container2}
            )

        safe_inputs = _called_inputs(mock_call)
        assert isinstance(safe_inputs['input_path'], DataContainer)
        assert isinstance(safe_inputs['reference_path'], DataContainer)
        assert safe_inputs['input_path'].get_file_paths() == ['/path/input1.csv']
        assert safe_inputs['reference_path'].get_file_paths() == ['/path/input2.parquet']

    # ------------------------------------------------------------------
    # step_name フォールバック
    # ------------------------------------------------------------------

    def test_step_name_defaults_to_plugin_name(self, executor, mock_output):
        """'name' キーがない場合は plugin_name が step_name になる"""
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