set -e

# --- The directory where this script exists ---
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR"

# --- setting ---
REQ_FILE="$BASE_DIR/requirements.txt"
ENTRY_POINT="./scripts/api/main.py"
EXE_NAME="ETLPipelineBuilderBackend"

# --- Unnecessary packages ---
SKIP_PACKAGES="python pip setuptools wheel"

# --- Clean ---
echo "Cleaning previous build..."
rm -rf build dist
rm -f "$EXE_NAME.spec"

# --- Reading requirements.txt ---
echo "Reading requirements.txt..."

packages=""
while IFS= read -r line; do
    [[ -z "$line" || "$line" =~ ^\s*# ]] && continue

    pkg=$(echo "$line" | sed 's/[<>=!].*//' | tr '-' '_' | tr '[:upper:]' '[:lower:]' | xargs)

    skip=false
    for s in $SKIP_PACKAGES; do
        [[ "$pkg" == "$s" ]] && skip=true && break
    done
    $skip && continue

    packages="$packages $pkg"
done < "$REQ_FILE"

# Remove duplicates & sort
packages=$(echo "$packages" | tr ' ' '\n' | sort -u | tr '\n' ' ')

# --- Building PyInstaller Commands ---
pyinstaller_args=(
    "--clean"
    "--onefile"
    "--console"
    "--name" "$EXE_NAME"
    "--add-data" "scripts:scripts"
    "--collect-all" "prefect"
    "--copy-metadata" "prefect"
    "--collect-all" "coolname"
    "--copy-metadata" "coolname"
    "--hidden-import" "sqlalchemy.dialects.sqlite"
    "--hidden-import" "sqlalchemy.dialects.sqlite.aiosqlite"
    "--hidden-import" "greenlet"
    "--hidden-import" "pkg_resources"
    "--hidden-import" "aiosqlite"
    "--hidden-import" "sqlite3"
    "--hidden-import" "_sqlite3"
    "--collect-all" "jinja2"
    "--collect-all" "jinja2_humanize_extension"
    "--hidden-import" "jinja2.environment"
    "--hidden-import" "jinja2.loaders"
    "--hidden-import" "jinja2.runtime"
    "--hidden-import" "jinja2.sandbox"
    "--hidden-import" "jinja2.ext"
    "--hidden-import" "jinja2.lexer"
    "--hidden-import" "jinja2.parser"
    "--hidden-import" "jinja2.compiler"
    "--hidden-import" "jinja2.nodes"
    "--hidden-import" "jinja2.optimizer"
    "--hidden-import" "jinja2.utils"
    "--hidden-import" "jinja2.exceptions"
    "--hidden-import" "jinja2.filters"
    "--hidden-import" "jinja2.tests"
)

for pkg in $packages; do
    pyinstaller_args+=("--collect-all" "$pkg")
done

pyinstaller_args+=("$ENTRY_POINT")

# --- Show Details ---
echo ""
echo "Executing PyInstaller with arguments:"
echo "pyinstaller ${pyinstaller_args[*]}"
echo ""

# --- Run PyInstaller ---
pyinstaller "${pyinstaller_args[@]}"

echo ""
echo "Build completed!"