from typing import Any, Dict, Optional
import re

def read_secret_in_dict(params: Dict[str, Any], resolver=None) -> Dict[str, Any]:
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    secret_pattern = re.compile(r"\${secrets\.([^}]+)}")
    resolved_params = {}

    for k, v in params.items():
        if isinstance(v, str):
            match = secret_pattern.fullmatch(v)
            if match:
                secret_ref = match.group(1)
                resolved_params[k] = read_secret(secret_ref, resolver=resolver)
            else:
                resolved_params[k] = v
        elif isinstance(v, dict):
            resolved_params[k] = read_secret_in_dict(v, resolver)
        elif isinstance(v, list):
            resolved_params[k] = [
                read_secret_in_dict(item, resolver) if isinstance(item, dict) else item
                for item in v
            ]
        else:
            resolved_params[k] = v

    return resolved_params

def read_secret(param_value: str, resolver=None, **kwargs: Any) -> Optional[str]:
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    if isinstance(param_value, str):
        match = re.fullmatch(r"\${secrets\.([^}]+)}", param_value)
        if match:
            secret_ref = match.group(1)
            try:
                return resolver.read(secret_ref, **kwargs)
            except Exception as e:
                raise RuntimeError(f"Failed to read secret '{secret_ref}': {e}") from e
    return param_value

def write_secret(secret_reference: str, secret_value: str, resolver=None, **kwargs: Any) -> None:
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    try:
        resolver.write(secret_reference, secret_value, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Failed to write secret '{secret_reference}': {e}") from e
