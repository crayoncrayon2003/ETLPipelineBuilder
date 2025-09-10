import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, Any, Optional
import json
import os

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

class BaseSecretResolver(ABC):
    """
    Abstract base class for secret resolution strategies.
    """
    @abstractmethod
    # def resolve(self, secret_reference: str) -> str | None:
    def resolve(self, secret_reference: str) -> Optional[str]:
        """
        Resolves a secret by its reference string.
        Returns the secret value as a string, or None if not found.
        """
        pass

class DotEnvSecretResolver(BaseSecretResolver):
    """
    Resolves secrets from a local .env file for local development.
    It does not support the @key syntax, as .env is a flat structure.
    """
    def __init__(self):
        # Path assumes this file is at backend/scripts/core/secrets/
        # and the .env file is at backend/.env
        dotenv_path = Path(__file__).resolve().parents[3] / '.env'

        if dotenv_path.exists():
            logger.info(f"Loading secrets from local .env file: {dotenv_path}")
            load_dotenv(dotenv_path=dotenv_path)
        else:
            logger.info(f"Warning: .env file not found at {dotenv_path}. "
                  "Will rely on existing environment variables.")

    # def resolve(self, secret_reference: str) -> str | None:
    def resolve(self, secret_reference: str) -> Optional[str]:
        # For .env files, we ignore any @key part and use the full reference as the key
        secret_name = secret_reference.split('@', 1)[0]
        return os.getenv(secret_name)

class AWSSecretResolver(BaseSecretResolver):
    """
    Resolves secrets from AWS Secrets Manager.
    Supports simple string secrets and extracting a key from a JSON secret
    using the format "SECRET_NAME@JSON_KEY".
    """
    def __init__(self):
        # A cache to store secrets already fetched from AWS during a single run
        self._cache: Dict[str, str] = {}

        try:
            import boto3
            # boto3 will use credentials from the execution environment's IAM role
            self.client = boto3.client('secretsmanager')
            logger.info("AWS Secret Resolver initialized.")
        except ImportError:
            logger.error("Error: boto3 is not installed. AWSSecretResolver cannot be used.")
            self.client = None
        except Exception as e:
            logger.error(f"Error initializing boto3 client: {e}")
            self.client = None

    # def resolve(self, secret_reference: str) -> str | None:
    def resolve(self, secret_reference: str) -> Optional[str]:
        if not self.client:
            return None

        # Parse the reference string into SECRET_NAME and optional JSON_KEY
        secret_name = secret_reference
        json_key = None
        if '@' in secret_reference:
            secret_name, json_key = secret_reference.split('@', 1)

        try:
            # Check the cache first to avoid redundant API calls
            if secret_name in self._cache:
                secret_string = self._cache[secret_name]
            else:
                # If not in cache, fetch from AWS Secrets Manager
                logger.info(f"Fetching secret '{secret_name}' from AWS Secrets Manager...")
                response = self.client.get_secret_value(SecretId=secret_name)
                secret_string = response.get('SecretString')

                if secret_string:
                    # Store the fetched secret string in the cache
                    self._cache[secret_name] = secret_string

            if not secret_string:
                return None

            # If a json_key was specified, parse the JSON and return the specific value
            if json_key:
                secret_data = json.loads(secret_string)
                if json_key not in secret_data:
                     logger.info(f"Key '{json_key}' not found in secret '{secret_name}'.")
                     return None
                return str(secret_data.get(json_key)) # Ensure the result is a string
            else:
                # If no key is specified, return the entire secret string
                return secret_string

        except self.client.exceptions.ResourceNotFoundException:
            logger.error(f"Secret '{secret_name}' not found in AWS Secrets Manager.")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse or find key '{json_key}' in secret '{secret_name}': {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{secret_reference}' from AWS: {e}")
            return None

def get_secret_resolver() -> BaseSecretResolver:
    """
    Factory function that returns the appropriate secret resolver
    based on the execution environment.
    """
    is_lambda = os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None
    is_glue = os.getenv("GLUE_VERSION") is not None

    if is_lambda or is_glue:
        logger.info("AWS environment detected. Using AWS Secrets Manager for secret resolution.")
        return AWSSecretResolver()
    else:
        logger.info("Local environment detected. Using .env file for secret resolution.")
        return DotEnvSecretResolver()

# Create a singleton instance of the appropriate resolver.
secret_resolver = get_secret_resolver()