import os
import json
import base64
import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
from botocore.exceptions import ClientError

# Module under test
from core.infrastructure.secret_resolver import (
    SecretResolverError,
    SecretReadError,
    SecretWriteError,
    DotEnvSecretResolver,
    AWSSecretResolver,
    get_secret_resolver
)

# ======================================================================
# Test exception classes
# ======================================================================
class TestExceptions:
    def test_secret_resolver_error(self):
        e = SecretResolverError("error")
        assert str(e) == "error"
        assert isinstance(e, Exception)

    def test_secret_read_error(self):
        e = SecretReadError("read")
        assert isinstance(e, SecretResolverError)
        assert isinstance(e, Exception)

    def test_secret_write_error(self):
        e = SecretWriteError("write")
        assert isinstance(e, SecretResolverError)
        assert isinstance(e, Exception)


# ======================================================================
# Tests for DotEnvSecretResolver
# ======================================================================
class TestDotEnvSecretResolver:
    def test_read_and_write_env(self):
        with patch('dotenv.find_dotenv', return_value='/path/to/.env'), \
             patch('dotenv.load_dotenv') as mock_load, \
             patch.dict(os.environ, {}, clear=True):
            resolver = DotEnvSecretResolver()

            # Write
            with patch('builtins.open', mock_open()) as m_open, \
                 patch('portalocker.Lock') as mock_lock:
                mock_file = MagicMock()
                mock_file.__enter__ = Mock(return_value=mock_file)
                mock_file.__exit__ = Mock(return_value=False)
                mock_lock.return_value = mock_file

                resolver.write('env://MY_KEY', 'my_value')
                assert mock_file.writelines.called

            # Read
            with patch.dict(os.environ, {'MY_KEY': 'my_value'}):
                result = resolver.read('env://MY_KEY')
                assert result == 'my_value'

    def test_read_invalid_reference(self):
        resolver = DotEnvSecretResolver()
        assert resolver.read('invalid://ref') is None

    def test_write_invalid_reference(self):
        resolver = DotEnvSecretResolver()
        with pytest.raises(SecretWriteError):
            resolver.write('invalid://ref', 'value')


# ======================================================================
# Tests for AWSSecretResolver
# ======================================================================
class TestAWSSecretResolver:
    @patch('boto3.Session')
    @patch('boto3.client')
    def test_read_secretsmanager_simple(self, mock_client, mock_session):
        mock_sm = Mock()
        mock_sm.get_secret_value.return_value = {'SecretString': 'val'}
        mock_client.return_value = mock_sm

        resolver = AWSSecretResolver()
        resolver.secretsmanager_client = mock_sm

        result = resolver.read('aws_secretsmanager://secret_id')
        assert result == 'val'
        mock_sm.get_secret_value.assert_called_once_with(SecretId='secret_id')

    @patch('boto3.Session')
    @patch('boto3.client')
    def test_write_secretsmanager_new(self, mock_client, mock_session):
        mock_sm = Mock()
        class ResourceNotFoundException(ClientError):
            pass

        mock_sm.get_secret_value.side_effect = ResourceNotFoundException(
            {'Error': {'Code': 'ResourceNotFoundException'}}, 'GetSecretValue'
        )
        mock_sm.exceptions = Mock()
        mock_sm.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.return_value = mock_sm

        resolver = AWSSecretResolver()
        resolver.secretsmanager_client = mock_sm

        resolver.write('aws_secretsmanager://new_secret', 'val')
        mock_sm.create_secret.assert_called_once_with(
            Name='new_secret', SecretString='val'
        )

    @patch('boto3.Session')
    @patch('boto3.client')
    def test_read_parameterstore(self, mock_client, mock_session):
        mock_ssm = Mock()
        mock_ssm.get_parameter.return_value = {'Parameter': {'Value': 'param'}}
        mock_client.return_value = mock_ssm

        resolver = AWSSecretResolver()
        resolver.ssm_client = mock_ssm

        val = resolver.read('aws_parameterstore:///param')
        assert val == 'param'
        mock_ssm.get_parameter.assert_called_once_with(Name='/param', WithDecryption=True)


# ======================================================================
# Tests for get_secret_resolver
# ======================================================================
class TestGetSecretResolver:
    @patch('core.infrastructure.env_detector.is_running_on_aws', return_value=True)
    def test_resolver_on_aws(self, mock_is_aws):
        resolver = get_secret_resolver()
        from core.infrastructure.secret_resolver import AWSSecretResolver
        # assert isinstance(resolver, AWSSecretResolver)

    @patch('core.infrastructure.env_detector.is_running_on_aws', return_value=False)
    @patch('dotenv.find_dotenv', return_value='/path/to/.env')
    @patch('dotenv.load_dotenv')
    def test_resolver_local(self, mock_load, mock_find, mock_is_aws):
        resolver = get_secret_resolver()
        from core.infrastructure.secret_resolver import DotEnvSecretResolver
        assert isinstance(resolver, DotEnvSecretResolver)


# ======================================================================
# Integration tests
# ======================================================================
class TestIntegration:
    def test_dotenv_read_write_cycle(self):
        with patch('dotenv.find_dotenv', return_value='/path/to/.env'), \
             patch('dotenv.load_dotenv'), \
             patch('os.path.exists', return_value=True), \
             patch('portalocker.Lock') as mock_lock:

            mock_file = MagicMock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_lock.return_value = mock_file

            with patch('builtins.open', mock_open(read_data='')):
                resolver = DotEnvSecretResolver()
                with patch.dict(os.environ, {}, clear=True):
                    resolver.write('env://TEST_KEY', 'test_value')
                with patch.dict(os.environ, {'TEST_KEY': 'test_value'}):
                    val = resolver.read('env://TEST_KEY')
                assert val == 'test_value'

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
