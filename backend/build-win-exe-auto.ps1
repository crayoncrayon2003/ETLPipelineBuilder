$ErrorActionPreference = "Stop"

# --- The directory where this script exists ---
$BASE_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $BASE_DIR

# --- setting ---
$REQ_FILE = Join-Path $BASE_DIR "requirements.txt"
$ENTRY_POINT = "./scripts/api/main.py"
$EXE_NAME = "ETLPipelineBuilderBackend"

# --- Unnecessary packages ---
$SKIP_PACKAGES = @(
    "python",
    "pip",
    "setuptools",
    "wheel"
)

# --- Clean ---
Write-Host "Cleaning previous build..."
Remove-Item -Recurse -Force build, dist -ErrorAction SilentlyContinue
Remove-Item -Force "$EXE_NAME.spec" -ErrorAction SilentlyContinue

# --- Reading requirements.txt ---
Write-Host "Reading requirements.txt..."

$packages = Get-Content $REQ_FILE |
    Where-Object {
        $_ -and
        ($_ -notmatch "^\s*#")
    } |
    ForEach-Object {
        ($_ -split "[<>=!]")[0].Trim().ToLower().Replace("-", "_")
    } |
    Where-Object {
        $_ -notin $SKIP_PACKAGES
    } |
    Sort-Object -Unique

# --- Building PyInstaller Commands ---
$pyinstallerArgs = @(
    "--clean",
    "--onefile",
    "--console",
    "--name", $EXE_NAME,
    "--add-data", "scripts;scripts",
    "--collect-all", "prefect",
    "--copy-metadata", "prefect",
    "--collect-all", "coolname",
    "--copy-metadata", "coolname",
    "--hidden-import", "sqlalchemy.dialects.sqlite",
    "--hidden-import", "sqlalchemy.dialects.sqlite.aiosqlite",
    "--hidden-import", "greenlet",
    "--hidden-import", "pkg_resources",
    "--hidden-import", "aiosqlite",
    "--hidden-import", "sqlite3",
    "--hidden-import", "_sqlite3",
    "--collect-all", "jinja2",
    "--collect-all", "jinja2_humanize_extension",
    "--hidden-import", "jinja2.environment",
    "--hidden-import", "jinja2.loaders",
    "--hidden-import", "jinja2.runtime",
    "--hidden-import", "jinja2.sandbox",
    "--hidden-import", "jinja2.ext",
    "--hidden-import", "jinja2.lexer",
    "--hidden-import", "jinja2.parser",
    "--hidden-import", "jinja2.compiler",
    "--hidden-import", "jinja2.nodes",
    "--hidden-import", "jinja2.optimizer",
    "--hidden-import", "jinja2.utils",
    "--hidden-import", "jinja2.exceptions",
    "--hidden-import", "jinja2.filters",
    "--hidden-import", "jinja2.tests"
)

foreach ($pkg in $packages) {
    $pyinstallerArgs += "--collect-all"
    $pyinstallerArgs += $pkg
}

$pyinstallerArgs += $ENTRY_POINT

# --- Show Details ---
Write-Host ""
Write-Host "Executing PyInstaller with arguments:"
Write-Host "pyinstaller $($pyinstallerArgs -join ' ')"
Write-Host ""

# --- Run PyInstaller ---
pyinstaller @pyinstallerArgs

Write-Host ""
Write-Host "Build completed!"