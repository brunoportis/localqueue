from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse


class SchemaError(ValueError):
    pass


def _resolve(root: Mapping[str, Any], reference: str) -> Mapping[str, Any]:
    if not reference.startswith("#/"):
        raise SchemaError(f"unsupported schema reference: {reference}")
    value: Any = root
    for part in reference[2:].split("/"):
        if not isinstance(value, Mapping) or part not in value:
            raise SchemaError(f"unknown schema reference: {reference}")
        value = value[part]
    if not isinstance(value, Mapping):
        raise SchemaError(f"schema reference is not an object: {reference}")
    return value


def _matches_type(value: object, expected: str) -> bool:
    return {
        "object": isinstance(value, Mapping),
        "array": isinstance(value, Sequence) and not isinstance(value, (str, bytes)),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }.get(expected, False)


def validate_json_schema(
    instance: object,
    schema: Mapping[str, Any],
    *,
    root: Mapping[str, Any] | None = None,
    path: str = "$",
) -> None:
    root = schema if root is None else root
    if "$ref" in schema:
        validate_json_schema(
            instance, _resolve(root, str(schema["$ref"])), root=root, path=path
        )
        return
    if "const" in schema and instance != schema["const"]:
        raise SchemaError(f"{path} must equal {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        raise SchemaError(f"{path} is not an allowed value")
    expected_type = schema.get("type")
    if expected_type is not None:
        types = [expected_type] if isinstance(expected_type, str) else expected_type
        if not any(_matches_type(instance, str(item)) for item in types):
            raise SchemaError(f"{path} has the wrong JSON type")
    if isinstance(instance, Mapping):
        required = schema.get("required", [])
        missing = set(required) - instance.keys()
        if missing:
            raise SchemaError(f"{path} is missing required fields: {sorted(missing)}")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extras = set(instance) - set(properties)
            if extras:
                raise SchemaError(f"{path} has unexpected fields: {sorted(extras)}")
        for key, value in instance.items():
            child_schema = properties.get(key)
            if child_schema is not None:
                validate_json_schema(
                    value, child_schema, root=root, path=f"{path}.{key}"
                )
            elif isinstance(schema.get("additionalProperties"), Mapping):
                validate_json_schema(
                    value,
                    schema["additionalProperties"],
                    root=root,
                    path=f"{path}.{key}",
                )
    if isinstance(instance, Sequence) and not isinstance(instance, (str, bytes)):
        minimum = schema.get("minItems")
        if isinstance(minimum, int) and len(instance) < minimum:
            raise SchemaError(f"{path} must contain at least {minimum} items")
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for index, item in enumerate(instance):
                validate_json_schema(
                    item, item_schema, root=root, path=f"{path}[{index}]"
                )
    if isinstance(instance, str):
        minimum = schema.get("minLength")
        if isinstance(minimum, int) and len(instance) < minimum:
            raise SchemaError(f"{path} is shorter than {minimum}")
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.fullmatch(pattern, instance) is None:
            raise SchemaError(f"{path} does not match {pattern}")
        if schema.get("format") == "uri":
            parsed = urlparse(instance)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise SchemaError(f"{path} is not an absolute HTTP(S) URI")
        if schema.get("format") == "date-time":
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", instance) is None:
                raise SchemaError(f"{path} is not a normalized UTC timestamp")
    if isinstance(instance, int) and not isinstance(instance, bool):
        minimum = schema.get("minimum")
        if isinstance(minimum, int) and instance < minimum:
            raise SchemaError(f"{path} must be at least {minimum}")
