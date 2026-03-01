import os
import re
import json
import base64
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

import boto3
import portalocker
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv

from core.infrastructure.env_detector import is_running_on_aws
from utils.logger import setup_logger

logger = setup_logger(__name__)


# ==============================================================================
# Common Exception Class
# ==============================================================================
class SecretResolverError(Exception):
    pass

class SecretReadError(SecretResolverError):
    """Custom exception for secret read/resolution failures."""
    pass

class SecretWriteError(SecretResolverError):
    """Custom exception for secret writing failures."""
    pass


# ==============================================================================
# Abstract Base Class
# ==============================================================================
class BaseSecretResolver(ABC):
    @abstractmethod
    def read(self, secret_reference: str) -> Optional[str]:
        pass

    @abstractmethod
    def write(self, secret_reference: str, secret_value: str, **kwargs: Any) -> None:
        pass


# ==============================================================================
# Resolving secrets via .env file
# ==============================================================================
class DotEnvSecretResolver(BaseSecretResolver):
    """
    Reads secrets from a local .env file for local development.
    """
    def __init__(self):
        self.dotenv_path = find_dotenv(usecwd=True)

        if self.dotenv_path:
            logger.info(f"Loading secrets from local .env file: {self.dotenv_path}")
            load_dotenv(dotenv_path=self.dotenv_path, override=True)
        else:
            logger.warning(".env file not found. Will rely on existing environment variables.")
            self.dotenv_path = os.path.join(os.getcwd(), '.env')
            logger.info(f"No existing .env file found. New '.env' file will be created at '{self.dotenv_path}' if writing is attempted.")

    def _resolve_nested_key(self, value: str, key_path: str) -> Optional[str]:
        try:
            data = json.loads(value)
            for key in key_path.split("."):
                if isinstance(data, str):
                    data = json.loads(data)
                if not isinstance(data, dict):
                    return None
                data = data.get(key)
                if data is None:
                    return None
            return str(data)
        except (json.JSONDecodeError, AttributeError):
            logger.warning(f"Failed to resolve nested key '{key_path}'")
            return None

    def read(self, secret_reference: str) -> Optional[str]:
        env_match = re.match(r"^env://([^@]+)(?:@(.+))?$", secret_reference)
        if env_match:
            env_var_name = env_match.group(1)
            json_key_path = env_match.group(2)
            value = os.getenv(env_var_name)
            if value is None:
                return None
            if json_key_path:
                return self._resolve_nested_key(value, json_key_path)
            return value
        else:
            logger.warning(f"DotEnvSecretResolver received unsupported reference format for read: '{secret_reference}'.")
            return None

    def write(self, secret_reference: str, secret_value: str, **kwargs: Any) -> None:
        env_match = re.match(r"^env://(.+)$", secret_reference)
        if not env_match:
            raise SecretWriteError(f"DotEnvSecretResolver received unsupported reference format for writing: '{secret_reference}'.")

        env_var_name = env_match.group(1)

        if not self.dotenv_path:
            raise SecretWriteError("No .env file path could be determined for writing.")

        env_lines = []
        if os.path.exists(self.dotenv_path):
            with open(self.dotenv_path, 'r', encoding='utf-8') as f:
                env_lines = f.readlines()

        new_env_lines = []
        updated = False
        for line in env_lines:
            if line.strip().startswith(f"{env_var_name}="):
                current_value = line.strip().split("=", 1)[1]
                if current_value == secret_value:
                    logger.debug(f"No change for {env_var_name}, skipping write.")
                    return
                new_env_lines.append(f"{env_var_name}={secret_value}\n")
                updated = True
            else:
                new_env_lines.append(line)

        if not updated:
            if new_env_lines and not new_env_lines[-1].endswith('\n'):
                new_env_lines.append('\n')
            new_env_lines.append(f"{env_var_name}={secret_value}\n")

        try:
            with portalocker.Lock(self.dotenv_path, 'w', timeout=3) as f:
                f.writelines(new_env_lines)
            logger.info(f"Secret '{env_var_name}' successfully written to .env file: {self.dotenv_path}")
            os.environ[env_var_name] = secret_value
            logger.info(f"Secret '{env_var_name}' updated in current process environment variables.")

        except IOError as e:
            raise SecretWriteError(f"Failed to write to .env file: {e}")
        except Exception as e:
            raise SecretWriteError(f"Failed to write .env secret: {e}")


# ==============================================================================
# Reading/Writing secrets via AWS service
# ==============================================================================
class AWSSecretResolver(BaseSecretResolver):

    def __init__(self):
        super().__init__()
        try:
            session = boto3.Session()
            region = session.region_name or "ap-northeast-1"
            self.secretsmanager_client = boto3.client("secretsmanager", region_name=region)
            self.ssm_client = boto3.client("ssm", region_name=region)
            self.kms_client = boto3.client("kms", region_name=region)
            logger.info(f"AWSSecretResolver initialized (region={region})")
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            self.secretsmanager_client = None
            self.ssm_client = None
            self.kms_client = None

    def read(self, secret_reference: str) -> Optional[str]:
        if secret_reference.startswith("aws_secretsmanager://"):
            return self._read_from_secretsmanager(secret_reference)
        elif secret_reference.startswith("aws_parameterstore://"):
            return self._read_from_parameterstore(secret_reference)
        elif secret_reference.startswith("aws_kms_decrypt://"):
            return self._decrypt_with_kms(secret_reference)
        else:
            logger.warning(f"Unsupported secret reference format: {secret_reference}")
            return None

    def write(self, secret_reference: str, secret_value: str, **kwargs: Any) -> None:
        if secret_reference.startswith("aws_secretsmanager://"):
            return self._write_to_secretsmanager(secret_reference, secret_value)
        elif secret_reference.startswith("aws_parameterstore://"):
            return self._write_to_parameterstore(secret_reference, secret_value)
        elif secret_reference.startswith("aws_kms_encrypt://"):
            return self._encrypt_with_kms(secret_reference, secret_value, kwargs.get("encryption_context"))
        else:
            # DotEnvSecretResolver.write() との一貫性のため SecretWriteError を raise する
            raise SecretWriteError(
                f"AWSSecretResolver received unsupported reference format "
                f"for writing: '{secret_reference}'."
            )

    def _resolve_nested_key(self, value: str, key_path: str) -> Optional[str]:
        try:
            data = json.loads(value)
            for key in key_path.split("."):
                if isinstance(data, str):
                    data = json.loads(data)
                if not isinstance(data, dict):
                    return None
                data = data.get(key)
                if data is None:
                    return None
            return str(data)
        except (json.JSONDecodeError, AttributeError):
            logger.warning(f"Failed to resolve nested key '{key_path}'")
            return None

    @staticmethod
    def _set_nested_key(data: dict, key_path: str, value: str) -> dict:
        """
        key_path ("a.b.c" 形式) に従ってネストした辞書に値をセットする。

        _write_to_secretsmanager() のネストキー対応
        """
        keys = key_path.split(".")
        target = data
        for key in keys[:-1]:
            if key not in target or not isinstance(target[key], dict):
                target[key] = {}
            target = target[key]
        target[keys[-1]] = value
        return data

    # ------------------------------------------------------------------
    # AWS Secrets Manager
    # ------------------------------------------------------------------
    def _read_from_secretsmanager(self, secret_reference: str) -> Optional[str]:
        sm_match = re.match(r"^aws_secretsmanager://([^@]+)(?:@(.+))?$", secret_reference)
        if not sm_match:
            return None
        secret_name = sm_match.group(1)
        json_key_path = sm_match.group(2)
        try:
            response = self.secretsmanager_client.get_secret_value(SecretId=secret_name)
            secret_string = response.get("SecretString")
            if not secret_string:
                return None
            if json_key_path:
                return self._resolve_nested_key(secret_string, json_key_path)
            return secret_string
        except ClientError as e:
            logger.error(f"Failed to read secret '{secret_name}': {e}")
            return None

    def _write_to_secretsmanager(self, secret_reference: str, secret_value: str) -> None:
        sm_match = re.match(r"^aws_secretsmanager://([^@]+)(?:@(.+))?$", secret_reference)
        if not sm_match:
            raise SecretWriteError(f"Invalid Secrets Manager reference: {secret_reference}")

        secret_name, json_key = sm_match.groups()

        try:
            try:
                resp = self.secretsmanager_client.get_secret_value(SecretId=secret_name)
                current_secret = resp.get("SecretString", "{}")
            except self.secretsmanager_client.exceptions.ResourceNotFoundException:
                # 新規作成: json_key がある場合は適切なネスト構造で作成
                if json_key:
                    payload = json.dumps(self._set_nested_key({}, json_key, secret_value))
                else:
                    payload = secret_value
                self.secretsmanager_client.create_secret(Name=secret_name, SecretString=payload)
                logger.info(f"Secret '{secret_name}' created successfully.")
                return

            if json_key:
                try:
                    data = json.loads(current_secret)
                except json.JSONDecodeError:
                    data = {}
                # "a.b" をフラットキーとして使うのではなくネスト構造で書き込む
                # _set_nested_key() でネスト階層をたどって書き込む
                self._set_nested_key(data, json_key, secret_value)
                secret_value_to_put = json.dumps(data)
            else:
                secret_value_to_put = secret_value

            self.secretsmanager_client.put_secret_value(SecretId=secret_name, SecretString=secret_value_to_put)
            logger.info(f"Secret '{secret_name}' updated successfully.")

        except Exception as e:
            logger.error(f"Failed to write secret '{secret_name}': {e}")
            raise SecretWriteError(f"Failed to write secret '{secret_name}': {e}")

    # ------------------------------------------------------------------
    # AWS Parameter Store
    # ------------------------------------------------------------------
    def _read_from_parameterstore(self, secret_reference: str) -> Optional[str]:
        ps_match = re.match(r"^aws_parameterstore://([^@]+)(?:@(.+))?$", secret_reference)
        if not ps_match:
            return None
        param_name = ps_match.group(1)
        json_key_path = ps_match.group(2)
        try:
            response = self.ssm_client.get_parameter(Name=param_name, WithDecryption=True)
            value = response["Parameter"]["Value"]
            if json_key_path:
                return self._resolve_nested_key(value, json_key_path)
            return value
        except ClientError as e:
            logger.error(f"Failed to read parameter '{param_name}': {e}")
            return None

    def _write_to_parameterstore(self, secret_reference: str, secret_value: str) -> None:
        ps_match = re.match(r"^aws_parameterstore://(.+)$", secret_reference)
        if not ps_match:
            raise SecretWriteError(f"Invalid Parameter Store reference: {secret_reference}")

        param_name = ps_match.group(1)

        try:
            self.ssm_client.put_parameter(
                Name=param_name,
                Value=secret_value,
                Type="SecureString",
                Overwrite=True
            )
            logger.info(f"Parameter '{param_name}' updated successfully.")
        except Exception as e:
            logger.error(f"Failed to write parameter '{param_name}': {e}")
            raise SecretWriteError(f"Failed to write parameter '{param_name}': {e}")

    # ------------------------------------------------------------------
    # AWS KMS
    # ------------------------------------------------------------------
    def _decrypt_with_kms(self, secret_reference: str) -> Optional[str]:
        kms_match = re.match(r"^aws_kms_decrypt://([^@]+)(?:@(.+))?$", secret_reference)
        if not kms_match:
            return None
        ciphertext_b64 = kms_match.group(1)
        json_key_path = kms_match.group(2)
        try:
            ciphertext = base64.b64decode(ciphertext_b64)
            response = self.kms_client.decrypt(CiphertextBlob=ciphertext)
            decrypted_value = response["Plaintext"].decode("utf-8")
            if json_key_path:
                return self._resolve_nested_key(decrypted_value, json_key_path)
            return decrypted_value
        except Exception as e:
            logger.error(f"Failed to decrypt value with KMS: {e}")
            return None

    def _encrypt_with_kms(self, secret_reference: str, plaintext: str, encryption_context: Optional[Dict[str, str]] = None) -> str:
        kms_match = re.match(r"^aws_kms_encrypt://(.+)$", secret_reference)
        if not kms_match:
            raise SecretWriteError(f"Invalid KMS encrypt reference: {secret_reference}")

        kms_key_id = kms_match.group(1)

        try:
            response = self.kms_client.encrypt(
                KeyId=kms_key_id,
                Plaintext=plaintext.encode("utf-8"),
                EncryptionContext=encryption_context or {}
            )
            ciphertext_b64 = base64.b64encode(response["CiphertextBlob"]).decode("utf-8")
            logger.info(f"KMS encryption successful for key '{kms_key_id}'.")
            return ciphertext_b64
        except Exception as e:
            logger.error(f"Failed to encrypt value with KMS key '{kms_key_id}': {e}")
            raise SecretWriteError(f"Failed to encrypt value with KMS key '{kms_key_id}': {e}")


# ==============================================================================
# Factory
# ==============================================================================
def get_secret_resolver() -> BaseSecretResolver:
    if is_running_on_aws():
        return AWSSecretResolver()
    else:
        return DotEnvSecretResolver()

# Singleton instance
secret_resolver = get_secret_resolver()