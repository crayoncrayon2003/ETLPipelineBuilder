from typing import Any, Dict, Optional, List, Tuple
import re

def read_secret_in_dict(params: Dict[str, Any], resolver=None) -> Dict[str, Any]:
    """
    Recursively resolves secret references within a dictionary.
    Supports secret references embedded within strings.
    """
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    resolved_params = {}

    for k, v in params.items():
        if isinstance(v, str):
            # Resolve secret references within the string
            resolved_params[k] = read_secret(v, resolver=resolver)
        elif isinstance(v, dict):
            # Recursively process dictionaries
            resolved_params[k] = read_secret_in_dict(v, resolver)
        elif isinstance(v, list):
            # Process elements within lists
            resolved_params[k] = [
                read_secret_in_dict(item, resolver) if isinstance(item, dict)
                else read_secret(item, resolver) if isinstance(item, str)
                else item
                for item in v
            ]
        else:
            resolved_params[k] = v

    return resolved_params


def extract_secret_references(text: str) -> List[Tuple[str, int, int]]:
    """
    Extracts all ${secrets.xxx} patterns from a string.

    Parameters:
        text: The string to search

    Returns:
        List[Tuple[str, int, int]]:
            - Element 1: Full secret reference (e.g., "${secrets.env://MY_KEY}")
            - Element 2: Start position
            - Element 3: End position
    """
    pattern = re.compile(r"\$\{secrets\.([^}]+)\}")
    matches = []

    for match in pattern.finditer(text):
        full_reference = match.group(0)  # Full "${secrets.env://MY_KEY}"
        start_pos = match.start()
        end_pos = match.end()
        matches.append((full_reference, start_pos, end_pos))

    return matches


def read_secret(param_value: str, resolver=None, **kwargs: Any) -> str:
    """
    Detects and resolves ${secrets.xxx} patterns within a string.

    Processing flow:
    1. Extract all ${secrets.xxx} references
    2. Loop through the list
       2-1. Resolve the detected ${secrets.xxx}
       2-2. If resolved successfully, replace the detected part with the resolved value
       2-3. If resolution fails, raise an error (or skip to next)

    Parameters:
        param_value: The string to process
        resolver: Secret resolver (uses default if None)
        **kwargs: Additional parameters to pass to the resolver

    Returns:
        String with secrets resolved

    Examples:
        "Bearer ${secrets.env://MY_TOKEN}"             -> "Bearer abc123token"

        "${secrets.env://USER}:${secrets.env://PASS}"  -> "test_user:secret_pass_123"
    """
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    if not isinstance(param_value, str):
        return param_value

    # Step 1: Extract all ${secrets.xxx} references
    secret_references = extract_secret_references(param_value)

    if not secret_references:
        # Return as-is if no secret references found
        return param_value

    # Step 2: Loop through the list (process from back to front to avoid index shifting)
    result = param_value
    for full_reference, start_pos, end_pos in reversed(secret_references):
        # Extract "env://MY_KEY" part from ${secrets.env://MY_KEY}
        secret_key = full_reference.replace("${secrets.", "").replace("}", "")

        try:
            # Step 2-1: Resolve the detected ${secrets.xxx}
            resolved_value = resolver.read(secret_key, **kwargs)

            if resolved_value is None:
                raise RuntimeError(f"Secret '{secret_key}' returned None")

            # Step 2-2: If resolved successfully, replace with the resolved value
            result = result[:start_pos] + str(resolved_value) + result[end_pos:]

        except Exception as e:
            # Step 2-3: Raise an error if resolution fails
            raise RuntimeError(f"Failed to read secret '{secret_key}': {e}") from e

    return result


def write_secret(secret_reference: str, secret_value: str, resolver=None, **kwargs: Any) -> None:
    """
    Writes a secret.
    Supports ${secrets.xxx} format references.
    """
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    if isinstance(secret_reference, str):
        match = re.fullmatch(r"\$\{secrets\.([^}]+)\}", secret_reference)
        if match:
            secret_ref = match.group(1)
        else:
            secret_ref = secret_reference

        try:
            resolver.write(secret_ref, secret_value, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to write secret '{secret_ref}': {e}") from e