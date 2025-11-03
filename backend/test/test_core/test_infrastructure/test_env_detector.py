import os
import pytest
from unittest.mock import patch, MagicMock
from scripts.core.infrastructure.env_detector import is_running_on_aws


class TestIsRunningOnAWS:
    """Test class for the is_running_on_aws function"""

    @pytest.fixture(autouse=True)
    def clear_aws_env(self):
        """Clear AWS-related environment variables and restore them before and after the test"""
        env_vars = ["AWS_LAMBDA_FUNCTION_NAME", "GLUE_VERSION", "AWS_EXECUTION_ENV", "AWS_REGION"]
        old_env = {var: os.environ.get(var) for var in env_vars}
        for var in env_vars:
            os.environ.pop(var, None)
        yield
        for var, val in old_env.items():
            if val is not None:
                os.environ[var] = val

    def setup_mock_boto3_session(self):
        """Mock boto3.Session and STS"""
        mock_session = patch("boto3.Session").start()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_session.return_value.client.return_value = mock_sts
        mock_session.return_value.region_name = "us-east-1"
        return patch.stopall, mock_session  # stopall will be used later for teardown

    def test_returns_true_when_lambda_env(self):
        """Test that True is returned when AWS Lambda environment variables are set"""
        # Arrange
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "my_lambda"
        os.environ["AWS_REGION"] = "us-east-1"

        # Act
        with patch("boto3.Session") as mock_session:
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
            mock_session.return_value.client.return_value = mock_sts
            mock_session.return_value.region_name = "us-east-1"

            result = is_running_on_aws()

        # Assert
        assert result is True

    def test_returns_true_when_glue_env(self):
        """Test that True is returned when AWS Glue environment variables are set"""
        # Arrange
        os.environ["GLUE_VERSION"] = "2.0"
        os.environ["AWS_REGION"] = "us-east-1"

        # Act
        with patch("boto3.Session") as mock_session:
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
            mock_session.return_value.client.return_value = mock_sts
            mock_session.return_value.region_name = "us-east-1"

            result = is_running_on_aws()

        # Assert
        assert result is True

    def test_returns_true_when_aws_execution_env(self):
        """Test that True is returned when AWS_EXECUTION_ENV is set"""
        # Arrange
        os.environ["AWS_EXECUTION_ENV"] = "AWS_Lambda_python3.9"
        os.environ["AWS_REGION"] = "us-east-1"

        # Act
        with patch("boto3.Session") as mock_session:
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
            mock_session.return_value.client.return_value = mock_sts
            mock_session.return_value.region_name = "us-east-1"

            result = is_running_on_aws()

        # Assert
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
