import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from core.pipeline import StepExecutor
from core.data_container.container import DataContainer


class TestStepExecutor:
    """Test class for StepExecutor"""

    @pytest.fixture
    def executor(self):
        """Fixture providing a StepExecutor instance"""
        return StepExecutor()

    def test_execute_step_basic(self, executor):
        """Test basic step execution"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {'param1': 'value1'},
            'name': 'test_step'
        }

        mock_output = Mock(spec=DataContainer)

        # Mock framework_manager's call_plugin_execute method
        with patch.object(
            executor,
            'execute_step',
            wraps=executor.execute_step
        ):
            with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
                mock_call.return_value = mock_output

                # Act
                result = executor.execute_step(step_config)

                # Assert
                assert result == mock_output
                mock_call.assert_called_once_with(
                    plugin_name='test_plugin',
                    params={'param1': 'value1'},
                    inputs={}
                )

    def test_execute_step_without_name(self, executor):
        """Test when step name is not provided"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {}
        }

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config)

            # Assert
            assert result == mock_output

    def test_execute_step_without_params(self, executor):
        """Test when params are not provided"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'name': 'test_step'
        }

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config)

            # Assert
            assert result == mock_output
            mock_call.assert_called_once_with(
                plugin_name='test_plugin',
                params={},
                inputs={}
            )

    def test_execute_step_with_single_file_input(self, executor):
        """Test when there is a single file input"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {'param1': 'value1'},
            'name': 'test_step'
        }

        mock_container = Mock(spec=DataContainer)
        mock_container.get_file_paths.return_value = ['/path/to/file.csv']

        inputs = {'input_data': mock_container}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            mock_call.assert_called_once_with(
                plugin_name='test_plugin',
                params={
                    'param1': 'value1',
                    'input_path': '/path/to/file.csv'
                },
                inputs=inputs
            )
            mock_container.get_file_paths.assert_called_once()

    def test_execute_step_with_multiple_file_input(self, executor):
        """Test when there are multiple file inputs"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {},
            'name': 'test_step'
        }

        mock_container = Mock(spec=DataContainer)
        file_paths = ['/path/to/file1.csv', '/path/to/file2.csv']
        mock_container.get_file_paths.return_value = file_paths

        inputs = {'input_data': mock_container}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            mock_call.assert_called_once_with(
                plugin_name='test_plugin',
                params={'input_path': file_paths},
                inputs=inputs
            )

    def test_execute_step_with_custom_input_name(self, executor):
        """Test with a custom input name"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {},
            'name': 'test_step'
        }

        mock_container = Mock(spec=DataContainer)
        mock_container.get_file_paths.return_value = ['/path/to/custom.csv']

        inputs = {'custom_input': mock_container}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            mock_call.assert_called_once_with(
                plugin_name='test_plugin',
                params={'custom_input_path': '/path/to/custom.csv'},
                inputs=inputs
            )

    def test_execute_step_with_multiple_inputs(self, executor):
        """Test when there are multiple inputs"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {'param1': 'value1'},
            'name': 'test_step'
        }

        mock_container1 = Mock(spec=DataContainer)
        mock_container1.get_file_paths.return_value = ['/path/to/input1.csv']

        mock_container2 = Mock(spec=DataContainer)
        mock_container2.get_file_paths.return_value = ['/path/to/input2.csv']

        inputs = {
            'input_data': mock_container1,
            'reference_data': mock_container2
        }

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            called_params = mock_call.call_args[1]['params']
            assert called_params['param1'] == 'value1'
            assert called_params['input_path'] == '/path/to/input1.csv'
            assert called_params['reference_data_path'] == '/path/to/input2.csv'

    def test_execute_step_with_none_container(self, executor):
        """Test when the container is None"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {},
            'name': 'test_step'
        }

        inputs = {'input_data': None}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            mock_call.assert_called_once_with(
                plugin_name='test_plugin',
                params={},
                inputs=inputs
            )

    def test_execute_step_preserves_original_params(self, executor):
        """Test that original params are not modified"""
        # Arrange
        original_params = {'param1': 'value1'}
        step_config = {
            'plugin': 'test_plugin',
            'params': original_params,
            'name': 'test_step'
        }

        mock_container = Mock(spec=DataContainer)
        mock_container.get_file_paths.return_value = ['/path/to/file.csv']

        inputs = {'input_data': mock_container}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            # Confirm that original params are not changed
            assert original_params == {'param1': 'value1'}
            assert 'input_path' not in original_params

    def test_execute_step_raises_exception(self, executor):
        """Test when an exception occurs during plugin execution"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {},
            'name': 'test_step'
        }

        error_message = "Plugin execution failed"

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.side_effect = Exception(error_message)

            # Act & Assert
            with pytest.raises(Exception) as exc_info:
                executor.execute_step(step_config)

            assert str(exc_info.value) == error_message

    def test_execute_step_with_empty_inputs_dict(self, executor):
        """Test when the input dictionary is empty"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {'param1': 'value1'},
            'name': 'test_step'
        }

        inputs = {}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            mock_call.assert_called_once_with(
                plugin_name='test_plugin',
                params={'param1': 'value1'},
                inputs={}
            )

    def test_execute_step_returns_none(self, executor):
        """Test when the plugin returns None"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {},
            'name': 'test_step'
        }

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = None

            # Act
            result = executor.execute_step(step_config)

            # Assert
            assert result is None

    def test_execute_step_with_complex_params(self, executor):
        """Test with complex params"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {
                'string_param': 'value',
                'int_param': 123,
                'list_param': [1, 2, 3],
                'dict_param': {'key': 'value'},
                'bool_param': True
            },
            'name': 'test_step'
        }

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config)

            # Assert
            assert result == mock_output
            called_params = mock_call.call_args[1]['params']
            assert called_params['string_param'] == 'value'
            assert called_params['int_param'] == 123
            assert called_params['list_param'] == [1, 2, 3]
            assert called_params['dict_param'] == {'key': 'value'}
            assert called_params['bool_param'] is True

    def test_execute_step_input_data_overwrites_param(self, executor):
        """Test when input data overwrites a parameter"""
        # Arrange
        step_config = {
            'plugin': 'test_plugin',
            'params': {'input_path': '/original/path.csv'},
            'name': 'test_step'
        }

        mock_container = Mock(spec=DataContainer)
        mock_container.get_file_paths.return_value = ['/new/path.csv']

        inputs = {'input_data': mock_container}

        mock_output = Mock(spec=DataContainer)

        with patch('core.plugin_manager.manager.framework_manager.call_plugin_execute') as mock_call:
            mock_call.return_value = mock_output

            # Act
            result = executor.execute_step(step_config, inputs)

            # Assert
            assert result == mock_output
            called_params = mock_call.call_args[1]['params']
            # The path from input data should overwrite the original param
            assert called_params['input_path'] == '/new/path.csv'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
