import os
import subprocess
import sys

def run_tests(target_dirs, ignore_dirs):
    """
    Run pytest with coverage for selected directories and ignore specified directories.
    Branch coverage (--cov-branch) is enabled.
    Generates HTML report.
    """
    test_dir = os.path.abspath(os.path.dirname(__file__))        # backend/test
    backend_dir = os.path.abspath(os.path.join(test_dir, ".."))  # backend

    env = os.environ.copy()
    env["PYTHONPATH"] = backend_dir

    # Construct the pytest command
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        test_dir,
    ]

    # Add coverage target directories (branch coverage enabled)
    for d in target_dirs:
        cmd.append(f"--cov={d}")

    # Enable branch coverage
    cmd.append("--cov-branch")

    # Specify HTML coverage report
    cmd.append(f"--cov-report=html:{os.path.join(test_dir, 'coverage_html')}")
    cmd.append("--cov-report=term-missing")

    # Specify directories to ignore
    for d in ignore_dirs:
        cmd.append(f"--ignore={os.path.join(backend_dir, d)}")

    print("Running tests with coverage (branch coverage enabled)...")
    print("Command:", " ".join(cmd))

    # Run pytest in a subprocess
    try:
        subprocess.check_call(cmd, env=env)
    except subprocess.CalledProcessError as e:
        print(f"Tests failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    else:
        print("✅ All tests completed successfully!")
        print(f"HTML coverage report generated at: {os.path.join(test_dir, 'coverage_html', 'index.html')}")

if __name__ == "__main__":
    target_dirs = [
        "scripts"
    ]
    ignore_dirs = [
        "scripts/plugins"
    ]
    run_tests(target_dirs=target_dirs, ignore_dirs=ignore_dirs)
