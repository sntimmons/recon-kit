import re

def sanitize_column_name(name: str) -> str:
    """
    Sanitize a column name for SQL safety and deterministic mapping.
    - Lowercase
    - Strip whitespace
    - Replace spaces and special chars with '_'
    - Allow only [a-z0-9_]
    - Collapse multiple '_'
    - Prefix with 'col_' if starts with number
    - Max length 64 chars
    """
    name = name.strip().lower()
    # Replace non-alphanumeric with _
    name = re.sub(r"[^a-z0-9]", "_", name)
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    # Remove leading/trailing underscores
    name = name.strip("_")
    # Prefix if starts with number
    if name and name[0].isdigit():
        name = f"col_{name}"
    # Truncate to max 64 chars
    name = name[:64]
    if not name:
        raise ValueError("Sanitized column name is empty.")
    return name
