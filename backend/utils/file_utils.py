# backend/utils/file_utils.py

from pathlib import Path

def ensure_directory_exists(dir_path: str | Path) -> Path:
    """
    Ensures that a directory exists, creating it if necessary.

    This is a safe way to make sure a target directory for writing files
    is available before attempting to write.

    Args:
        dir_path (str | Path): The path to the directory.

    Returns:
        Path: The Path object of the ensured directory.
    """
    path = Path(dir_path)
    # The `parents=True` argument ensures that intermediate directories
    # are also created (like `mkdir -p`).
    # `exist_ok=True` prevents an error from being raised if the
    # directory already exists.
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_project_root() -> Path:
    """
    Finds and returns the project's root directory.

    This utility assumes that the project root is identifiable by the
    presence of a specific file or directory, such as '.git' or 'pyproject.toml'.
    This makes file paths relative to the project root more reliable than
    using relative paths like '../../'.

    Returns:
        Path: The absolute path to the project root directory.
    
    Raises:
        FileNotFoundError: If the project root marker cannot be found.
    """
    # Start from the current file's directory and traverse upwards
    current_path = Path(__file__).resolve()
    # A common marker for a project root is the .git directory or a setup.py file
    project_root_marker = ".git" 

    for parent in current_path.parents:
        if (parent / project_root_marker).exists():
            return parent
            
    raise FileNotFoundError(
        f"Could not find project root. "
        f"Searched for a directory containing '{project_root_marker}'."
    )

# --- Example Usage ---
#
# if __name__ == '__main__':
#     # Create a dummy directory to demonstrate ensure_directory_exists
#     temp_dir = Path("./temp_test_dir/subdir")
#     print(f"Ensuring directory exists: {temp_dir}")
#     ensure_directory_exists(temp_dir)
#     print(f"Directory exists: {temp_dir.exists()}")
#     
#     # Clean up the dummy directory
#     import shutil
#     shutil.rmtree("./temp_test_dir")
#     print("Cleaned up temporary directory.")
#
#     # Demonstrate get_project_root
#     try:
#         root = get_project_root()
#         print(f"Project root found at: {root}")
#     except FileNotFoundError as e:
#         print(e)