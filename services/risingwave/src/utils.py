import re


def dict_to_table_schema(schema: dict[str, str]) -> str:
    columns = []
    for column, column_type in schema.items():
        validate_params(column)
        validate_params(column_type)
        column_def = f"{column} {column_type}"
        columns.append(column_def)

    return ", ".join(columns)


def validate_params(value: str):
    # Only allow alphanumeric characters and underscores
    """
    Validate a table name.

    Only alphanumeric characters and underscores are allowed in a table name.
    The first character must be a letter or an underscore.
    """
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", value):
        return value
    else:
        raise ValueError(f"Invalid table name: {value}")
