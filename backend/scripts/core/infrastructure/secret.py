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
            resolved_params[k] = read_secret(v, resolver=resolver)
        elif isinstance(v, dict):
            resolved_params[k] = read_secret_in_dict(v, resolver)
        elif isinstance(v, list):
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

    Returns:
        List[Tuple[str, int, int]]:
            - Element 1: Full secret reference (e.g., "${secrets.env://MY_KEY}")
            - Element 2: Start position
            - Element 3: End position
    """
    pattern = re.compile(r"\$\{secrets\.([^}]+)\}")
    matches = []

    for match in pattern.finditer(text):
        full_reference = match.group(0)
        start_pos = match.start()
        end_pos = match.end()
        matches.append((full_reference, start_pos, end_pos))

    return matches


def read_secret(param_value: Any, resolver=None, **kwargs: Any) -> Optional[Any]:
    """
    Detects and resolves ${secrets.xxx} patterns within a string.

    Processing flow:
    1. If param_value is not a str, return as-is
    2. Extract all ${secrets.xxx} references
    3. If none found, return as-is
    4. Resolve from back to front (avoids index shifting on replacement)
       4-1. Resolve each reference via resolver.read()
       4-2. If resolved, replace the matched portion with the resolved value
       4-3. If None is returned, return None immediately
       4-4. If an exception occurs, raise RuntimeError

    Parameters:
        param_value: The value to process (any type; only str is processed)
        resolver: Secret resolver (uses default singleton if None)
        **kwargs: Additional parameters passed to the resolver

    Returns:
        - Resolved string if all references resolved successfully
        - None if any reference resolved to None
        - Original value (non-str) unchanged

    Examples:
        "Bearer ${secrets.env://MY_TOKEN}"            -> "Bearer abc123token"
        "${secrets.env://USER}:${secrets.env://PASS}" -> "test_user:secret_pass"

    修正4: 型アノテーションを実装に合わせて修正
    修正前: (param_value: str, ...) -> str
      - str でない引数をそのまま返す実装があるにもかかわらず引数型が str
      - resolver.read() が None を返す場合があるにもかかわらず戻り値型が str
      → mypy が誤った安全性を示していた
    修正後: (param_value: Any, ...) -> Optional[Any]
      - 実装の実際の動作を正確に表現する
    """
    if resolver is None:
        from core.infrastructure.secret_resolver import secret_resolver as resolver

    # str 以外はそのまま返す
    if not isinstance(param_value, str):
        return param_value

    # ${secrets.xxx} パターンを全て抽出
    secret_references = extract_secret_references(param_value)

    if not secret_references:
        return param_value

    # 後ろから順に置換 (インデックスずれ防止)
    result = param_value
    for full_reference, start_pos, end_pos in reversed(secret_references):
        secret_key = full_reference.replace("${secrets.", "").replace("}", "")

        try:
            resolved_value = resolver.read(secret_key, **kwargs)

            if resolved_value is None:
                return None

            result = result[:start_pos] + str(resolved_value) + result[end_pos:]

        except Exception as e:
            raise RuntimeError(f"Failed to read secret '{secret_key}': {e}") from e

    return result


def write_secret(secret_reference: str, secret_value: str, resolver=None, **kwargs: Any) -> None:
    """
    Writes a secret.
    Supports both ${secrets.xxx} format and direct references (e.g. env://MY_VAR).
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
