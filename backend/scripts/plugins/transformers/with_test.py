import os
import pluggy
from typing import Dict, Any
from core.data_container.container import DataContainer
from core.plugin_manager.base_plugin import BasePlugin
from core.infrastructure.secret import read_secret, write_secret

from utils.logger import setup_logger

logger = setup_logger(__name__)

hookimpl = pluggy.HookimplMarker("etl_framework")

class SecretManagerReadThenWritePlugin(BasePlugin):
    """
    Plugin to read a secret from AWS Secrets Manager and write its value
    to another secret in AWS Secrets Manager. Ensures read/write consistency.
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "secret_manager_read_then_write"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "source_secret": {"type": "string", "title": "Source Secret Reference"},
                "target_secret": {"type": "string", "title": "Target Secret Reference"}
            },
            "required": ["source_secret", "target_secret"]
        }

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        source_secret = self.params.get("source_secret")
        target_secret = self.params.get("target_secret")

        if not all([source_secret, target_secret]):
            raise ValueError("Missing required parameters: 'source_secret' and 'target_secret'.")

        # Read secret
        logger.info(f"[{self.get_plugin_name()}] Reading secret '{source_secret}'...")
        try:
            read_val = read_secret(source_secret)
            logger.info(f"[{self.get_plugin_name()}] Secret read successfully. Value: {read_val}")
        except Exception as e:
            logger.error(f"[{self.get_plugin_name()}] Failed to read secret: {e}")
            raise

        # Write secret to target
        logger.info(f"[{self.get_plugin_name()}] Writing secret '{target_secret}'...")
        try:
            write_secret(target_secret, read_val)
            logger.info(f"[{self.get_plugin_name()}] Secret written successfully.")
        except Exception as e:
            logger.error(f"[{self.get_plugin_name()}] Failed to write secret: {e}")
            raise

        return self.finalize_container(
            container,
            metadata={}
        )
