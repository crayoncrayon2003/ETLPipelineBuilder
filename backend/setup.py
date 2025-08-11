from setuptools import setup, find_packages

setup(
    name="etl_framework_backend",
    version="1.0.0",
    author="Your Name",
    description="A custom, pluggable ETL framework backend.",

    # find_packages() は、'backend' ディレクトリ配下の
    # __init__.py を含むすべてのパッケージを自動的に見つけ出す
    packages=find_packages(where=".", include=["backend*"]),

    # これにより、'from core.pipeline...' のようなインポートが可能になる
    # 'backend' ディレクトリ自体をパッケージのルートとして扱う
    package_dir={"": "."},
)