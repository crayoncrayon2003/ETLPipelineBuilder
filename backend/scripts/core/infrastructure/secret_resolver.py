import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from typing import Dict, Any, Optional
import json
import re
import base64
import boto3
from botocore.exceptions import ClientError

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

# ==============================================================================
# Common Exception Class
# ==============================================================================
class SecretResolutionError(Exception):
    """Custom exception for secret resolution failures."""
    pass

# ==============================================================================
# Abstract Base Class
# ==============================================================================
class BaseSecretResolver(ABC):
    """
    Abstract base class for secret resolution strategies.
    """
    @abstractmethod
    def resolve(self, secret_reference: str) -> Optional[str]:
        """
        Resolves a secret by its reference string.
        Returns the secret value as a string, or None if not found.
        """
        pass

# ==============================================================================
# Resolving secrets via .env file
# ==============================================================================
class DotEnvSecretResolver(BaseSecretResolver):
    """
    Resolves secrets from a local .env file for local development.
    Supports references like "${env://MY_VARIABLE}".
    """
    def __init__(self):
        current_path = os.path.abspath(__file__)
        for _ in range(3):
            current_path = os.path.dirname(current_path)
        dotenv_path = os.path.join(current_path, '.env')

        if os.path.exists(dotenv_path):
            logger.info(f"Loading secrets from local .env file: {dotenv_path}")
            load_dotenv(dotenv_path=dotenv_path)
        else:
            logger.info(f"Warning: .env file not found at {dotenv_path}. "
                  "Will rely on existing environment variables.")

    def resolve(self, secret_reference: str) -> Optional[str]:
        # 'env://' プレフィックスを想定
        env_match = re.match(r"^env://(.+)$", secret_reference)
        if env_match:
            env_var_name = env_match.group(1)
            return os.getenv(env_var_name)
        else:
            logger.warning(f"DotEnvSecretResolver received unsupported reference format: '{secret_reference}'. Expected 'env://VARIABLE_NAME'.")
            return None


# ==============================================================================
# Resolving secrets via AWS service
# ==============================================================================
class AWSSecretResolver(BaseSecretResolver):
    """
    Resolves secrets from AWS Secrets Manager, Parameter Store, and decrypts with KMS.
    Supports explicit reference formats:
    - "aws_secretsmanager://SECRET_NAME[@JSON_KEY]"
    - "aws_parameterstore://PARAMETER_NAME[?with_decryption=false]"
    - "aws_kms_decrypt://BASE64_ENCODED_CIPHERTEXT"
    """
    def __init__(self):
        self._cache: Dict[str, str] = {}
        self.secretsmanager_client = None
        self.ssm_client = None
        self.kms_client = None

        try:
            session = boto3.Session()
            region = session.region_name or os.getenv("AWS_REGION", "ap-northeast-1")
            logger.info(f"AWS region used for boto3 clients: {region}")

            self.secretsmanager_client = boto3.client('secretsmanager',region_name=region)
            self.ssm_client = boto3.client('ssm',region_name=region)
            self.kms_client = boto3.client('kms',region_name=region)
            logger.info("AWS Secret Resolver initialized with Secrets Manager, Parameter Store, and KMS clients.")
        except ImportError:
            logger.error("Error: boto3 is not installed. AWSSecretResolver cannot be used.")
            self.secretsmanager_client = None
            self.ssm_client = None
            self.kms_client = None
        except Exception as e:
            logger.error(f"Error initializing boto3 clients: {e}")
            self.secretsmanager_client = None
            self.ssm_client = None
            self.kms_client = None

    def _get_secret_from_secrets_manager(self, secret_name: str, json_key: Optional[str] = None) -> Optional[str]:
        """
        Helper to get a secret from AWS Secrets Manager.
        """
        if not self.secretsmanager_client:
            raise SecretResolutionError("Secrets Manager client not initialized.")

        try:
            if secret_name in self._cache:
                secret_string = self._cache[secret_name]
            else:
                logger.info(f"Fetching secret '{secret_name}' from AWS Secrets Manager...")
                response = self.secretsmanager_client.get_secret_value(SecretId=secret_name)
                secret_string = response.get('SecretString')
                if secret_string:
                    self._cache[secret_name] = secret_string

            if not secret_string:
                return None

            if json_key:
                secret_data = json.loads(secret_string)
                if json_key not in secret_data:
                    logger.warning(f"Key '{json_key}' not found in secret '{secret_name}' (Secrets Manager).")
                    return None
                return str(secret_data.get(json_key))
            else:
                return secret_string

        except self.secretsmanager_client.exceptions.ResourceNotFoundException:
            logger.warning(f"Secret '{secret_name}' not found in AWS Secrets Manager.")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse or find key '{json_key}' in secret '{secret_name}' (Secrets Manager): {e}")
            raise SecretResolutionError(f"Failed to process Secrets Manager secret '{secret_name}': {e}")
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{secret_name}' from Secrets Manager: {e}")
            raise SecretResolutionError(f"Failed to retrieve Secrets Manager secret '{secret_name}': {e}")

    def _get_parameter_from_parameter_store(self, parameter_name: str, with_decryption: bool = True) -> Optional[str]:
        """
        Helper to get a parameter from AWS Systems Manager Parameter Store.
        """
        if not self.ssm_client:
            raise SecretResolutionError("Parameter Store client not initialized.")

        cache_key = f"ssm://{parameter_name}?dec={with_decryption}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            logger.info(f"Fetching parameter '{parameter_name}' from AWS Parameter Store (decryption: {with_decryption})...")
            response = self.ssm_client.get_parameter(Name=parameter_name, WithDecryption=with_decryption)
            param_value = response['Parameter']['Value']
            self._cache[cache_key] = param_value
            return param_value
        except self.ssm_client.exceptions.ParameterNotFound:
            logger.warning(f"Parameter '{parameter_name}' not found in AWS Parameter Store.")
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve parameter '{parameter_name}' from Parameter Store: {e}")
            raise SecretResolutionError(f"Failed to retrieve Parameter Store parameter '{parameter_name}': {e}")

    def _decrypt_with_kms(self, ciphertext_blob_b64: str) -> Optional[str]:
        """
        Helper to decrypt a Base64 encoded ciphertext using AWS KMS.
        """
        if not self.kms_client:
            raise SecretResolutionError("KMS client not initialized.")

        try:
            ciphertext_blob = base64.b64decode(ciphertext_blob_b64)
            logger.info("Decrypting data with AWS KMS...")
            response = self.kms_client.decrypt(CiphertextBlob=ciphertext_blob)
            return response['Plaintext'].decode('utf-8')
        except base64.binascii.Error as e:
            raise SecretResolutionError(f"Invalid Base64 string for KMS decryption: {e}")
        except Exception as e:
            logger.error(f"Failed to decrypt with KMS: {e}")
            raise SecretResolutionError(f"Failed to decrypt with KMS: {e}")

    def resolve(self, secret_reference: str) -> Optional[str]:
        if not (self.secretsmanager_client and self.ssm_client and self.kms_client):
            logger.error("One or more AWS clients are not initialized. Cannot resolve AWS secrets.")
            return None

        # Secrets Manager
        sm_match = re.match(r"^aws_secretsmanager://([^@]+)(?:@(.+))?$", secret_reference)
        if sm_match:
            secret_name = sm_match.group(1)
            json_key = sm_match.group(2)
            return self._get_secret_from_secrets_manager(secret_name, json_key)

        # Parameter Store
        ps_match = re.match(r"^aws_parameterstore://([^?]+)(?:\?(.*))?$", secret_reference)
        if ps_match:
            parameter_name = ps_match.group(1)
            query_string = ps_match.group(2)
            with_decryption = True
            if query_string:
                query_params = dict(re.findall(r"([^=]+)=([^&]+)", query_string))
                if 'with_decryption' in query_params:
                    with_decryption = query_params['with_decryption'].lower() == 'true'
            return self._get_parameter_from_parameter_store(parameter_name, with_decryption)

        # KMS Decrypt
        kms_match = re.match(r"^aws_kms_decrypt://(.+)$", secret_reference)
        if kms_match:
            ciphertext_b64 = kms_match.group(1)
            return self._decrypt_with_kms(ciphertext_b64)

        logger.warning(f"Unsupported secret reference format for AWSResolver: '{secret_reference}'. "
                       "Expected 'aws_secretsmanager://...', 'aws_parameterstore://...', or 'aws_kms_decrypt://...'.")
        return None

# ==============================================================================
# Factory
# ==============================================================================
def is_running_on_aws() -> bool:
    try:
        session = boto3.Session()
        region = session.region_name or os.getenv("AWS_REGION")
        if not region:
            logger.warning("AWS region not found in boto3 session or environment variables.")
            return False

        sts = session.client("sts", region_name=region)
        account_id = sts.get_caller_identity().get("Account")

        is_aws_env = any([
            os.getenv("AWS_LAMBDA_FUNCTION_NAME"),
            os.getenv("GLUE_VERSION"),
            os.getenv("AWS_EXECUTION_ENV"),
            bool(account_id)
        ])

        return is_aws_env
    except Exception as e:
        logger.warning(f"Failed to detect AWS environment: {e}")

    return False


def get_secret_resolver() -> BaseSecretResolver:
    """
    Factory function that returns the appropriate secret resolver
    based on the execution environment.
    """
    is_aws_env = is_running_on_aws()
    if is_aws_env:
        logger.info("AWS environment detected. Using AWSSecretResolver for secret resolution.")
        return AWSSecretResolver()
    else:
        logger.info("Local environment detected. Using DotEnvSecretResolver for secret resolution.")
        return DotEnvSecretResolver()

# Create a singleton instance of the appropriate resolver.
secret_resolver = get_secret_resolver()
