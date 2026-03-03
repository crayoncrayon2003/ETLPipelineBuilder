"""
conftest.py - pytest用パス設定

配置場所: backend/test/conftest.py

【目的】
pytestがどのディレクトリから実行されても、`scripts/` ディレクトリを
sys.pathに追加することで以下のモジュールを正しく解決できるようにする。

    import api.xxx       -> scripts/api/xxx
    import core.xxx      -> scripts/core/xxx
    import utils.xxx     -> scripts/utils/xxx
    import plugins.xxx   -> scripts/plugins/xxx

"""

import sys
import os

# このファイルの場所: backend/test/conftest.py
# scripts/ の場所:    backend/scripts/
_test_dir = os.path.dirname(os.path.abspath(__file__))       # backend/test/
_backend_dir = os.path.dirname(_test_dir)                     # backend/
_scripts_dir = os.path.join(_backend_dir, "scripts")          # backend/scripts/

if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)