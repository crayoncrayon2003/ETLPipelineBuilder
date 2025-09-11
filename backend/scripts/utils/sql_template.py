import os
from jinja2 import Environment, FileSystemLoader
from typing import Dict, Any

def render_sql_template(template_path: str, context: Dict[str, Any]) -> str:
    """
    Renders a SQL query from a Jinja2 template file.

    This utility allows for dynamic SQL generation by substituting variables
    in a .sql template file with values from a context dictionary.

    Args:
        template_path (str): The path to the SQL template file.
        context (Dict[str, Any]): A dictionary of variables to be made
                                  available within the template.

    Returns:
        str: The rendered, ready-to-execute SQL query string.

    Raises:
        FileNotFoundError: If the template file does not exist.
        Exception: If there is an error during template rendering.
    """
    abs_path = os.path.abspath(template_path)

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"SQL template file not found at: {abs_path}")

    template_dir = os.path.dirname(abs_path)
    template_file = os.path.basename(abs_path)

    env = Environment(
        loader=FileSystemLoader(template_dir),
        trim_blocks=True,
        lstrip_blocks=True
    )

    try:
        template = env.get_template(template_file)
        rendered_sql = template.render(context)
        return rendered_sql
    except Exception as e:
        raise Exception(f"Failed to render SQL template '{template_file}': {e}")
