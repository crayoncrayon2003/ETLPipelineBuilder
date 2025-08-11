# backend/core/config/validator.py

from typing import Dict, Any, Type, Optional
from pydantic import BaseModel, ValidationError

class ConfigValidator:
    """
    Validates configuration dictionaries against a Pydantic model.

    This class provides a simple interface to check if a loaded configuration
    (e.g., from a YAML file) conforms to a predefined schema, ensuring that
    all required fields are present and have the correct data types.
    """

    def __init__(self, schema_model: Type[BaseModel]):
        """
        Initializes the validator with a Pydantic model as the schema.

        Args:
            schema_model (Type[BaseModel]): The Pydantic model class that
                defines the expected structure and types of the configuration.
        """
        if not issubclass(schema_model, BaseModel):
            raise TypeError("schema_model must be a Pydantic BaseModel subclass.")
        self.schema_model = schema_model

    def validate(self, config_data: Dict[str, Any]) -> Optional[BaseModel]:
        """
        Validates the given configuration data against the schema.

        Args:
            config_data (Dict[str, Any]): The configuration dictionary to validate.

        Returns:
            Optional[BaseModel]: A Pydantic model instance populated with the
                validated and type-coerced data if validation is successful.
                Returns None if validation fails.
        """
        try:
            validated_config = self.schema_model.model_validate(config_data)
            print("Configuration validation successful.")
            return validated_config
        except ValidationError as e:
            # The error message from Pydantic is very detailed and user-friendly.
            print(f"Configuration validation failed:\n{e}")
            return None

