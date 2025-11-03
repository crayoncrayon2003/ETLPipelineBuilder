import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
from core.infrastructure.spark_session_factory import SparkSessionFactory


class TestSparkSessionFactory:
    """Test class for SparkSessionFactory"""

    @pytest.fixture(autouse=True)
    def reset_factory(self):
        """Reset the factory state before and after each test"""
        SparkSessionFactory._spark_session = None
        SparkSessionFactory._glue_context = None
        yield
        SparkSessionFactory._spark_session = None
        SparkSessionFactory._glue_context = None

    @pytest.fixture
    def mock_awsglue_module(self):
        """Fixture to mock the awsglue module"""
        mock_awsglue = MagicMock()
        mock_context = MagicMock()
        mock_awsglue.context = mock_context

        with patch.dict('sys.modules', {
            'awsglue': mock_awsglue,
            'awsglue.context': mock_context
        }):
            yield mock_context

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_get_spark_session_on_aws(self, mock_is_aws, mock_awsglue_module):
        """Test obtaining SparkSession on AWS environment"""
        # Arrange
        mock_is_aws.return_value = True

        # Mock SparkContext
        mock_sc = Mock()
        with patch('pyspark.context.SparkContext') as mock_spark_context:
            mock_spark_context.getOrCreate.return_value = mock_sc

            # Mock GlueContext
            mock_glue_ctx = Mock()
            mock_spark_session = Mock()
            mock_glue_ctx.spark_session = mock_spark_session
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            # Act
            result = SparkSessionFactory.get_spark_session()

            # Assert
            assert result == mock_spark_session
            assert SparkSessionFactory._spark_session == mock_spark_session
            assert SparkSessionFactory._glue_context == mock_glue_ctx
            mock_spark_context.getOrCreate.assert_called_once()
            mock_awsglue_module.GlueContext.assert_called_once_with(mock_sc)

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    @patch('pyspark.sql.SparkSession')
    @patch('pyspark.SparkConf')
    def test_get_spark_session_on_local(
        self,
        mock_spark_conf,
        mock_spark_session_class,
        mock_is_aws
    ):
        """Test obtaining SparkSession on local environment"""
        # Arrange
        mock_is_aws.return_value = False

        mock_conf = Mock()
        mock_conf.set.return_value = mock_conf
        mock_spark_conf.return_value = mock_conf

        mock_builder = Mock()
        mock_spark_session = Mock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark_session
        mock_spark_session_class.builder = mock_builder

        # Act
        original_env = os.environ.copy()
        try:
            result = SparkSessionFactory.get_spark_session()

            # Assert
            assert result == mock_spark_session
            assert SparkSessionFactory._spark_session == mock_spark_session
            assert "PYSPARK_PYTHON" in os.environ
            assert "PYSPARK_DRIVER_PYTHON" in os.environ
            assert os.environ.get("JAVA_HOME") == "/usr/lib/jvm/java-11-openjdk-amd64"

            # Verify Spark configuration
            assert mock_conf.set.call_count >= 4
            mock_builder.appName.assert_called_once_with("ETLFrameworkSpark")
            mock_builder.config.assert_called_once_with(conf=mock_conf)
            mock_builder.getOrCreate.assert_called_once()
        finally:
            # Restore environment variables
            os.environ.clear()
            os.environ.update(original_env)

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_get_spark_session_returns_cached_session(self, mock_is_aws):
        """Test returning cached SparkSession if it already exists"""
        # Arrange
        mock_is_aws.return_value = False
        mock_spark_session = Mock()
        SparkSessionFactory._spark_session = mock_spark_session

        # Act
        result = SparkSessionFactory.get_spark_session()

        # Assert
        assert result == mock_spark_session

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_stop_spark_session_on_local(self, mock_is_aws):
        """Test stopping SparkSession on local environment"""
        # Arrange
        mock_is_aws.return_value = False
        mock_spark_session = Mock()
        SparkSessionFactory._spark_session = mock_spark_session

        # Act
        SparkSessionFactory.stop_spark_session()

        # Assert
        mock_spark_session.stop.assert_called_once()
        assert SparkSessionFactory._spark_session is None

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_stop_spark_session_on_aws_does_not_stop(self, mock_is_aws):
        """Test that SparkSession is not stopped on AWS environment"""
        # Arrange
        mock_is_aws.return_value = True
        mock_spark_session = Mock()
        SparkSessionFactory._spark_session = mock_spark_session

        # Act
        SparkSessionFactory.stop_spark_session()

        # Assert
        mock_spark_session.stop.assert_not_called()
        assert SparkSessionFactory._spark_session is not None

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_stop_spark_session_when_no_session_exists(self, mock_is_aws):
        """Test stopping SparkSession when no session exists"""
        # Arrange
        mock_is_aws.return_value = False
        SparkSessionFactory._spark_session = None

        # Act & Assert (verify no exception occurs)
        SparkSessionFactory.stop_spark_session()
        assert SparkSessionFactory._spark_session is None

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_get_glue_context_on_aws(self, mock_is_aws, mock_awsglue_module):
        """Test obtaining GlueContext on AWS environment"""
        # Arrange
        mock_is_aws.return_value = True

        mock_sc = Mock()
        with patch('pyspark.context.SparkContext') as mock_spark_context:
            mock_spark_context.getOrCreate.return_value = mock_sc

            mock_glue_ctx = Mock()
            mock_spark_session = Mock()
            mock_glue_ctx.spark_session = mock_spark_session
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            # Act
            result = SparkSessionFactory.get_glue_context()

            # Assert
            assert result == mock_glue_ctx
            assert SparkSessionFactory._glue_context == mock_glue_ctx

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    @patch('pyspark.sql.SparkSession')
    @patch('pyspark.SparkConf')
    def test_get_glue_context_on_local_returns_none(
        self,
        mock_spark_conf,
        mock_spark_session_class,
        mock_is_aws
    ):
        """Test that getting GlueContext on local environment returns None"""
        # Arrange
        mock_is_aws.return_value = False

        mock_conf = Mock()
        mock_conf.set.return_value = mock_conf
        mock_spark_conf.return_value = mock_conf

        mock_builder = Mock()
        mock_spark_session = Mock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark_session
        mock_spark_session_class.builder = mock_builder

        # Act
        result = SparkSessionFactory.get_glue_context()

        # Assert
        assert result is None

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_get_glue_context_returns_cached_context(self, mock_is_aws, mock_awsglue_module):
        """Test returning cached GlueContext if it already exists"""
        # Arrange
        mock_glue_ctx = Mock()
        SparkSessionFactory._glue_context = mock_glue_ctx

        # Act
        result = SparkSessionFactory.get_glue_context()

        # Assert
        assert result == mock_glue_ctx
        # Verify that a new context is not created
        mock_awsglue_module.GlueContext.assert_not_called()

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    @patch('pyspark.sql.SparkSession')
    @patch('pyspark.SparkConf')
    def test_spark_configuration_values(
        self,
        mock_spark_conf,
        mock_spark_session_class,
        mock_is_aws
    ):
        """Test that Spark configuration values are correctly set"""
        # Arrange
        mock_is_aws.return_value = False

        mock_conf = Mock()
        mock_conf.set.return_value = mock_conf
        mock_spark_conf.return_value = mock_conf

        mock_builder = Mock()
        mock_spark_session = Mock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark_session
        mock_spark_session_class.builder = mock_builder

        # Act
        SparkSessionFactory.get_spark_session()

        # Assert - Verify that each configuration is called correctly
        expected_calls = [
            ("spark.executor.memory", "2g"),
            ("spark.driver.memory", "2g"),
            ("spark.executor.cores", "4"),
            ("spark.sql.shuffle.partitions", "10")
        ]

        actual_calls = [call[0] for call in mock_conf.set.call_args_list]
        for expected_key, expected_value in expected_calls:
            assert (expected_key, expected_value) in actual_calls

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    def test_get_spark_session_on_aws_multiple_calls(self, mock_is_aws, mock_awsglue_module):
        """Test that singleton is maintained when called multiple times on AWS"""
        # Arrange
        mock_is_aws.return_value = True

        mock_sc = Mock()
        with patch('pyspark.context.SparkContext') as mock_spark_context:
            mock_spark_context.getOrCreate.return_value = mock_sc

            mock_glue_ctx = Mock()
            mock_spark_session = Mock()
            mock_glue_ctx.spark_session = mock_spark_session
            mock_awsglue_module.GlueContext.return_value = mock_glue_ctx

            # Act
            result1 = SparkSessionFactory.get_spark_session()
            result2 = SparkSessionFactory.get_spark_session()

            # Assert
            assert result1 == result2
            assert result1 == mock_spark_session
            # GlueContext should be created only once
            mock_awsglue_module.GlueContext.assert_called_once()

    @patch('core.infrastructure.spark_session_factory.is_running_on_aws')
    @patch('pyspark.sql.SparkSession')
    @patch('pyspark.SparkConf')
    def test_environment_variables_set_correctly(
        self,
        mock_spark_conf,
        mock_spark_session_class,
        mock_is_aws
    ):
        """Test that environment variables are correctly set"""
        # Arrange
        mock_is_aws.return_value = False

        mock_conf = Mock()
        mock_conf.set.return_value = mock_conf
        mock_spark_conf.return_value = mock_conf

        mock_builder = Mock()
        mock_spark_session = Mock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark_session
        mock_spark_session_class.builder = mock_builder

        # Save environment variables
        original_env = os.environ.copy()

        try:
            # Act
            SparkSessionFactory.get_spark_session()

            # Assert
            assert "PYSPARK_PYTHON" in os.environ
            assert "PYSPARK_DRIVER_PYTHON" in os.environ
            assert os.environ["JAVA_HOME"] == "/usr/lib/jvm/java-11-openjdk-amd64"

            # Verify that Python path is set as PYSPARK_PYTHON
            # (Only existence is checked since actual value depends on environment)
            pyspark_python = os.environ.get("PYSPARK_PYTHON")
            assert pyspark_python is not None
            assert "python" in pyspark_python.lower()
        finally:
            # Restore environment variables
            os.environ.clear()
            os.environ.update(original_env)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
