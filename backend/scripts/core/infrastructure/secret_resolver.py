import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv, find_dotenv
from typing import Dict, Any, Optional
import json
import re
import base64
import boto3
from botocore.exceptions import ClientError
import portalocker
from core.infrastructure.env_detector import is_running_on_aws

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

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
    """
    Abstract base class for secret resolution strategies.
    """
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

    def read(self, secret_reference: str) -> Optional[str]:
        env_match = re.match(r"^env://(.+)$", secret_reference)
        if env_match:
            env_var_name = env_match.group(1)
            return os.getenv(env_var_name)
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
    """
    A Secret Resolver compatible with AWS Secrets Manager, Parameter Store, and KMS,
    supporting both read and write operations
    """

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

    # ---------------------------------------------------------------------
    # Public Interface (BaseSecretResolver Implementation)
    # ---------------------------------------------------------------------
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
            logger.warning(f"Unsupported write target: {secret_reference}")
            return None

    # ---------------------------------------------------------------------
    # AWS Secrets Manager
    # ---------------------------------------------------------------------
    def _read_from_secretsmanager(self, secret_reference: str) -> Optional[str]:
        sm_match = re.match(r"^aws_secretsmanager://([^@]+)(?:@(.+))?$", secret_reference)
        if not sm_match:
            return None
        secret_name = sm_match.group(1)
        json_key = sm_match.group(2)
        try:
            response = self.secretsmanager_client.get_secret_value(SecretId=secret_name)
            secret_string = response.get("SecretString")
            if not secret_string:
                return None
            if json_key:
                try:
                    secret_data = json.loads(secret_string)
                    return str(secret_data.get(json_key))
                except json.JSONDecodeError:
                    logger.warning(f"Secret '{secret_name}' is not JSON but key '{json_key}' requested.")
                    return None
            return secret_string
        except ClientError as e:
            logger.error(f"Failed to read secret '{secret_name}': {e}")
            return None

    def _write_to_secretsmanager(self, secret_reference: str, secret_value: str) -> None:
        sm_match = re.match(r"^aws_secretsmanager://([^@]+)(?:@(.+))?$", secret_reference)
        if not sm_match:
            logger.error(f"Invalid Secrets Manager reference: {secret_reference}")
            return
        secret_name = sm_match.group(1)
        json_key = sm_match.group(2)
        try:
            current_secret = None
            try:
                resp = self.secretsmanager_client.get_secret_value(SecretId=secret_name)
                current_secret = resp.get("SecretString")
            except self.secretsmanager_client.exceptions.ResourceNotFoundException:
                logger.info(f"Creating new secret '{secret_name}'...")
                self.secretsmanager_client.create_secret(Name=secret_name, SecretString=secret_value)
                return
            secret_payload = secret_value
            if json_key:
                try:
                    data = json.loads(current_secret or "{}")
                except json.JSONDecodeError:
                    data = {}
                data[json_key] = secret_value
                secret_payload = json.dumps(data)
            self.secretsmanager_client.put_secret_value(SecretId=secret_name, SecretString=secret_payload)
            logger.info(f"Secret '{secret_name}' updated successfully.")
        except Exception as e:
            logger.error(f"Failed to write secret '{secret_name}': {e}")
            raise

    # ---------------------------------------------------------------------
    # AWS Parameter Store
    # ---------------------------------------------------------------------
    def _read_from_parameterstore(self, secret_reference: str) -> Optional[str]:
        ps_match = re.match(r"^aws_parameterstore://(.+)$", secret_reference)
        if not ps_match:
            return None
        param_name = ps_match.group(1)
        try:
            response = self.ssm_client.get_parameter(Name=param_name, WithDecryption=True)
            return response["Parameter"]["Value"]
        except ClientError as e:
            logger.error(f"Failed to read parameter '{param_name}': {e}")
            return None

    def _write_to_parameterstore(self, secret_reference: str, secret_value: str) -> None:
        ps_match = re.match(r"^aws_parameterstore://(.+)$", secret_reference)
        if not ps_match:
            logger.error(f"Invalid Parameter Store reference: {secret_reference}")
            return
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
            raise

    # ---------------------------------------------------------------------
    # AWS KMS Encrypt/Decrypt
    # ---------------------------------------------------------------------
    def _decrypt_with_kms(self, secret_reference: str) -> Optional[str]:
        kms_match = re.match(r"^aws_kms_decrypt://(.+)$", secret_reference)
        if not kms_match:
            return None
        ciphertext_b64 = kms_match.group(1)
        try:
            ciphertext = base64.b64decode(ciphertext_b64)
            response = self.kms_client.decrypt(CiphertextBlob=ciphertext)
            return response["Plaintext"].decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to decrypt value with KMS: {e}")
            return None

    def _encrypt_with_kms(self, secret_reference: str, plaintext: str, encryption_context: Optional[Dict[str, str]] = None) -> str:
        kms_match = re.match(r"^aws_kms_encrypt://(.+)$", secret_reference)
        if not kms_match:
            raise ValueError(f"Invalid KMS encrypt reference: {secret_reference}")
        kms_key_id = kms_match.group(1)
        try:
            response = self.kms_client.encrypt(
                KeyId=kms_key_id,
                Plaintext=plaintext.encode("utf-8"),
                EncryptionContext=encryption_context or {}
            )
            return base64.b64encode(response["CiphertextBlob"]).decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to encrypt value with KMS: {e}")
            raise

# ==============================================================================
# Factory
# ==============================================================================
def get_secret_resolver() -> BaseSecretResolver:
    is_aws_env = is_running_on_aws()
    if is_aws_env:
        return AWSSecretResolver()
    else:
        return DotEnvSecretResolver()

# Singleton instance
secret_resolver = get_secret_resolver()
