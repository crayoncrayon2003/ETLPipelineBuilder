# backend/utils/sql_template.py

from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, Any

def render_sql_template(
    template_path: str | Path,
    context: Dict[str, Any]
) -> str:
    """
    Renders a SQL query from a Jinja2 template file.

    This utility allows for dynamic SQL generation by substituting variables
    in a .sql template file with values from a context dictionary.

    Args:
        template_path (str | Path): The path to the SQL template file.
        context (Dict[str, Any]): A dictionary of variables to be made
                                  available within the template.

    Returns:
        str: The rendered, ready-to-execute SQL query string.
    
    Raises:
        FileNotFoundError: If the template file does not exist.
        Exception: If there is an error during template rendering.
    """
    path = Path(template_path)

    if not path.is_file():
        raise FileNotFoundError(f"SQL template file not found at: {path}")

    # Set up a Jinja2 environment that loads templates from the same
    # directory as the specified template file.
    env = Environment(
        loader=FileSystemLoader(str(path.parent)),
        trim_blocks=True,
        lstrip_blocks=True
    )
    
    try:
        template = env.get_template(path.name)
        rendered_sql = template.render(context)
        return rendered_sql
    except Exception as e:
        # Catch potential Jinja2 errors (e.g., undefined variables)
        raise Exception(f"Failed to render SQL template '{path.name}': {e}")


# --- Example Usage ---
#
# Assume you have a file 'my_query.sql' with the following content:
#
#   SELECT *
#   FROM {{ table_name }}
#   WHERE event_date = '{{ date_filter }}'
#     AND status IN {{ status_list | tojson }}
#
# You could use this utility like so:
#
# if __name__ == '__main__':
#     from pathlib import Path
#     
#     # Create a dummy template file for the example
#     p = Path("./my_query.sql")
#     p.write_text("SELECT * FROM {{ table_name }} WHERE event_date = '{{ date_filter }}'")
#
#     params = {
#         'table_name': 'sales_data',
#         'date_filter': '2025-08-04'
#     }
#     
#     sql_query = render_sql_template('my_query.sql', params)
#     print(sql_query)
#     # Expected output:
#     # SELECT * FROM sales_data WHERE event_date = '2025-08-04'
#     
#     p.unlink() # Clean up the dummy file