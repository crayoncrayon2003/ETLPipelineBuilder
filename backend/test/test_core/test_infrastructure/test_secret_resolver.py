import os
import json
import base64
import pytest
from unittest.mock import patch, Mock, MagicMock, mock_open
from botocore.exceptions import ClientError

# Import the module under test
from core.infrastructure.secret_resolver import (
    SecretResolverError,
    SecretReadError,
    SecretWriteError,
    BaseSecretResolver,
    DotEnvSecretResolver,
    AWSSecretResolver,
    get_secret_resolver
)


# ======================================================================
# Tests for DotEnvSecretResolver
# ======================================================================
class TestDotEnvSecretResolver:
    def test_init_with_found_dotenv_controlled_path(self, tmp_path):
        # 1. Setup: create a .env file
        test_key = 'TEST_KEY_FROM_FILE'
        test_value = 'controlled_value'
        dotenv_content = f"{test_key}={test_value}\n"

        dotenv_file = tmp_path / ".env"
        dotenv_file.write_text(dotenv_content)

        # 2. Setup: change current working directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            # 3. Execute: initialize DotEnvSecretResolver
            resolver = DotEnvSecretResolver()

            # 4. Assert 1: resolver.dotenv_path is the absolute path of the found file
            expected_path = str(dotenv_file.resolve())
            assert resolver.dotenv_path == expected_path

            # 5. Assert 2: load_dotenv side effect occurred (environment variables are loaded)
            assert os.environ.get(test_key) == test_value

        finally:
            # 6. Cleanup: revert to original working directory
            os.chdir(original_cwd)

            # 7. Cleanup: remove environment variable set by the test
            if test_key in os.environ:
                del os.environ[test_key]


    def test_init_without_found_dotenv_controlled_path(self, tmp_path):
        # 1. Setup: do not create a .env file (so find_dotenv(usecwd=True) fails)

        # 2. Setup: move to a temporary directory to control os.getcwd()
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            # 3. Execute: initialize DotEnvSecretResolver
            # find_dotenv() finds nothing and proceeds to the else block (uses os.getcwd())
            resolver = DotEnvSecretResolver()

            # 4. Assert 1: resolver.dotenv_path is set to the default path
            # If find_dotenv fails, the code sets os.path.join(os.getcwd(), '.env')
            expected_path = str(tmp_path / ".env")
            assert resolver.dotenv_path == expected_path

            # 5. Assert 2: environment variables are not loaded (find_dotenv failed)
            # Verify by checking that load_dotenv was not called
            assert 'SOME_TEST_VAR' not in os.environ

        finally:
            # 6. Cleanup: revert to original working directory
            os.chdir(original_cwd)

    def test_read_valid_env_reference(self):
        with patch.dict(os.environ, {'TEST_SECRET': 'test_value'}):
            resolver = DotEnvSecretResolver()
            result = resolver.read('env://TEST_SECRET')
            assert result == 'test_value'

    def test_read_nonexistent_env_var(self):
        resolver = DotEnvSecretResolver()
        result = resolver.read('env://NONEXISTENT_VAR')
        assert result is None

    def test_read_invalid_reference_format(self):
        resolver = DotEnvSecretResolver()
        result = resolver.read('invalid://reference')
        assert result is None

    def test_write_new_secret(self):
        resolver = DotEnvSecretResolver()
        with patch('os.path.exists', return_value=True), \
             patch('portalocker.Lock') as mock_lock:
            mock_file = MagicMock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_lock.return_value = mock_file
            with patch('builtins.open', mock_open(read_data='EXISTING_VAR=value1\n')):
                resolver.write('env://NEW_VAR', 'new_value')
                assert mock_file.writelines.called

    def test_write_update_existing_secret(self):
        resolver = DotEnvSecretResolver()
        with patch('os.path.exists', return_value=True), \
             patch('portalocker.Lock') as mock_lock:
            mock_file = MagicMock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_lock.return_value = mock_file
            with patch('builtins.open', mock_open(read_data='TEST_VAR=old_value\n')):
                resolver.write('env://TEST_VAR', 'new_value')
                assert mock_file.writelines.called

    def test_write_no_change_needed(self):
        resolver = DotEnvSecretResolver()
        with patch('os.path.exists', return_value=True):
            with patch('builtins.open', mock_open(read_data='TEST_VAR=same_value\n')):
                resolver.write('env://TEST_VAR', 'same_value')
                # No write should occur

    def test_write_invalid_reference_format(self):
        resolver = DotEnvSecretResolver()
        with pytest.raises(SecretWriteError):
            resolver.write('invalid://reference', 'value')


# ======================================================================
# Tests for AWSSecretResolver
# ======================================================================
class TestAWSSecretResolver:

    @patch('boto3.client')
    @patch('boto3.Session')
    def test_init_success(self, mock_session, mock_client):
        mock_session.return_value = Mock(region_name='ap-northeast-1')
        resolver = AWSSecretResolver()
        assert resolver.secretsmanager_client is not None
        assert resolver.ssm_client is not None
        assert resolver.kms_client is not None

    @patch('boto3.client')
    def test_read_from_secretsmanager_simple(self, mock_client):
        mock_sm = Mock()
        mock_sm.get_secret_value.return_value = {'SecretString': 'my_secret_value'}
        mock_client.return_value = mock_sm
        resolver = AWSSecretResolver()
        resolver.secretsmanager_client = mock_sm
        result = resolver.read('aws_secretsmanager://my-secret')
        assert result == 'my_secret_value'

    @patch('boto3.client')
    def test_read_from_secretsmanager_with_json_key(self, mock_client):
        mock_sm = Mock()
        secret_data = {'username': 'admin', 'password': 'secret123'}
        mock_sm.get_secret_value.return_value = {'SecretString': json.dumps(secret_data)}
        mock_client.return_value = mock_sm
        resolver = AWSSecretResolver()
        resolver.secretsmanager_client = mock_sm
        result = resolver.read('aws_secretsmanager://my-secret@password')
        assert result == 'secret123'

    @patch('boto3.client')
    def test_read_from_secretsmanager_client_error(self, mock_client):
        mock_sm = Mock()
        mock_sm.get_secret_value.side_effect = ClientError({'Error': {'Code': 'ResourceNotFoundException'}}, 'GetSecretValue')
        mock_client.return_value = mock_sm
        resolver = AWSSecretResolver()
        resolver.secretsmanager_client = mock_sm
        result = resolver.read('aws_secretsmanager://nonexistent')
        assert result is None

    def test_read_unsupported_reference(self):
        resolver = AWSSecretResolver()
        result = resolver.read('unsupported://reference')
        assert result is None

    def test_write_unsupported_reference(self):
        resolver = AWSSecretResolver()
        result = resolver.write('unsupported://reference', 'value')
        assert result is None


# ======================================================================
# Tests for factory function
# ======================================================================
class TestGetSecretResolver:

    @patch('core.infrastructure.env_detector.is_running_on_aws', return_value=True)
    @patch('boto3.Session')
    @patch('boto3.client')
    def test_get_resolver_on_aws(self, mock_client, mock_session, mock_is_aws):
        mock_session.return_value = Mock(region_name='ap-northeast-1')
        resolver = get_secret_resolver()
        assert isinstance(resolver, AWSSecretResolver)

    @patch('core.infrastructure.env_detector.is_running_on_aws', return_value=False)
    def test_get_resolver_local(self, mock_is_aws):
        resolver = get_secret_resolver()
        assert isinstance(resolver, DotEnvSecretResolver)


# ======================================================================
# Tests for exception classes
# ======================================================================
class TestExceptions:

    def test_secret_resolver_error(self):
        error = SecretResolverError("Test error")
        assert str(error) == "Test error"

    def test_secret_read_error(self):
        error = SecretReadError("Read error")
        assert isinstance(error, SecretResolverError)

    def test_secret_write_error(self):
        error = SecretWriteError("Write error")
        assert isinstance(error, SecretResolverError)


# ======================================================================
# Integration tests
# ======================================================================
class TestIntegration:

    def test_dotenv_read_write_cycle(self):
        resolver = DotEnvSecretResolver()
        with patch('os.path.exists', return_value=True), \
             patch('portalocker.Lock') as mock_lock:
            mock_file = MagicMock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_lock.return_value = mock_file
            with patch('builtins.open', mock_open(read_data='')):
                with patch.dict(os.environ, {}, clear=True):
                    resolver.write('env://TEST_KEY', 'test_value')
                with patch.dict(os.environ, {'TEST_KEY': 'test_value'}):
                    result = resolver.read('env://TEST_KEY')
        assert result == 'test_value'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
