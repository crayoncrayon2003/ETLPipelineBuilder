import os
import sys
import json
import base64
import pytest
from unittest.mock import patch, Mock, MagicMock, mock_open, call
from botocore.exceptions import ClientError
from core.infrastructure.env_detector import is_running_on_aws as _is_running_on_aws

from core.infrastructure.secret_resolver import (
    SecretResolverError,
    SecretReadError,
    SecretWriteError,
    BaseSecretResolver,
    DotEnvSecretResolver,
    AWSSecretResolver,
    get_secret_resolver,
)


# ======================================================================
# Helpers
# ======================================================================

def _client_error(code: str, op: str = "op") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, op)


def _make_aws_resolver() -> AWSSecretResolver:
    """boto3 呼び出しをモックして AWSSecretResolver を生成する"""
    with patch("boto3.Session") as ms, patch("boto3.client"):
        ms.return_value = Mock(region_name="ap-northeast-1")
        return AWSSecretResolver()


def _make_dotenv_resolver() -> DotEnvSecretResolver:
    """dotenv 呼び出しをモックして DotEnvSecretResolver を生成する"""
    with patch("dotenv.find_dotenv", return_value=""), \
         patch("dotenv.load_dotenv"):
        return DotEnvSecretResolver()


def _lock_mock():
    """portalocker.Lock のモックとファイルオブジェクトを返す"""
    mock_file = MagicMock()
    mock_file.__enter__ = Mock(return_value=mock_file)
    mock_file.__exit__ = Mock(return_value=False)
    mock_lock = MagicMock(return_value=mock_file)
    return mock_lock, mock_file


def _resource_not_found(mock_sm):
    """secretsmanager の ResourceNotFoundException を設定する"""
    exc_cls = type(
        "ResourceNotFoundException",
        (ClientError,),
        {}
    )
    mock_sm.exceptions.ResourceNotFoundException = exc_cls
    mock_sm.get_secret_value.side_effect = exc_cls(
        {"Error": {"Code": "ResourceNotFoundException", "Message": ""}},
        "GetSecretValue",
    )


# ======================================================================
# 例外クラス
# ======================================================================
class TestExceptions:

    def test_secret_resolver_error_message(self):
        assert str(SecretResolverError("msg")) == "msg"

    def test_secret_read_error_is_resolver_error(self):
        assert isinstance(SecretReadError("r"), SecretResolverError)

    def test_secret_write_error_is_resolver_error(self):
        assert isinstance(SecretWriteError("w"), SecretResolverError)


# ======================================================================
# DotEnvSecretResolver.__init__
# ======================================================================
class TestDotEnvSecretResolverInit:

    def test_init_with_found_dotenv(self, tmp_path):
        """.env が見つかった場合: dotenv_path が設定され環境変数がロードされる"""
        dotenv_file = tmp_path / ".env"
        dotenv_file.write_text("TEST_KEY_FROM_FILE=controlled_value\n")
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            resolver = DotEnvSecretResolver()
            assert resolver.dotenv_path == str(dotenv_file.resolve())
            assert os.environ.get("TEST_KEY_FROM_FILE") == "controlled_value"
        finally:
            os.chdir(original_cwd)
            os.environ.pop("TEST_KEY_FROM_FILE", None)

    def test_init_without_found_dotenv(self, tmp_path):
        """.env がない場合: dotenv_path が cwd/.env にフォールバック"""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            resolver = DotEnvSecretResolver()
            assert resolver.dotenv_path == str(tmp_path / ".env")
            assert "SOME_TEST_VAR" not in os.environ
        finally:
            os.chdir(original_cwd)


# ======================================================================
# DotEnvSecretResolver._resolve_nested_key
# MCDC:
#   条件A: json.loads(value) が成功するか
#   条件B: isinstance(data, str) → 再parse
#   条件C: isinstance(data, dict)
#   条件D: data is None (キー不存在)
# ======================================================================
class TestDotEnvResolveNestedKey:

    @pytest.fixture
    def r(self):
        return _make_dotenv_resolver()

    def test_a_false_invalid_json_returns_none(self, r):
        """A=False: 不正JSON → None"""
        assert r._resolve_nested_key("not_json", "key") is None

    def test_single_key_success(self, r):
        """A=True, C=True, D=False: 単一キーで値を取得"""
        assert r._resolve_nested_key('{"k": "v"}', "k") == "v"

    def test_d_true_key_not_found_returns_none(self, r):
        """A=True, D=True: キーが存在しない → None"""
        assert r._resolve_nested_key('{"a": "x"}', "missing") is None

    def test_c_false_value_not_dict_returns_none(self, r):
        """A=True, C=False: 途中の値が dict でない(数値) → None"""
        assert r._resolve_nested_key('{"a": 42}', "a.b") is None

    def test_b_true_nested_string_reparsed(self, r):
        """A=True, B=True: 値がJSON文字列 → 再parse して深いキーを取得"""
        value = json.dumps({"a": json.dumps({"b": "deep"})})
        assert r._resolve_nested_key(value, "a.b") == "deep"

    def test_multi_level_nesting(self, r):
        """A=True, 多段ネスト a.b.c"""
        value = json.dumps({"a": {"b": {"c": "deep"}}})
        assert r._resolve_nested_key(value, "a.b.c") == "deep"

    def test_numeric_value_converted_to_str(self, r):
        """数値は str に変換して返す"""
        assert r._resolve_nested_key('{"n": 123}', "n") == "123"


# ======================================================================
# DotEnvSecretResolver.read
# MCDC:
#   条件E: env_match (正規表現マッチ)
#   条件F: value is None (環境変数未設定)
#   条件G: bool(json_key_path)
# ======================================================================
class TestDotEnvSecretResolverRead:

    @pytest.fixture
    def r(self):
        return _make_dotenv_resolver()

    def test_e_false_invalid_format_returns_none(self, r):
        """E=False: 不正フォーマット → None"""
        assert r.read("invalid://reference") is None

    def test_e_true_f_false_g_false_plain_value(self, r):
        """E=True, F=False, G=False: 環境変数をそのまま返す"""
        with patch.dict(os.environ, {"MY_VAR": "hello"}):
            assert r.read("env://MY_VAR") == "hello"

    def test_e_true_f_true_unset_var_returns_none(self, r):
        """E=True, F=True: 環境変数が未設定 → None"""
        assert r.read("env://NONEXISTENT_VAR_XYZ_12345") is None

    def test_e_true_f_false_g_true_single_json_key(self, r):
        """E=True, F=False, G=True: 単一 JSON キーを解決"""
        data = json.dumps({"user": "alice", "pass": "secret"})
        with patch.dict(os.environ, {"MY_SECRET": data}):
            assert r.read("env://MY_SECRET@user") == "alice"

    def test_e_true_f_false_g_true_nested_json_key(self, r):
        """E=True, F=False, G=True: 多段ネスト JSON キーを解決"""
        data = json.dumps({"db": {"host": "localhost"}})
        with patch.dict(os.environ, {"DB_SECRET": data}):
            assert r.read("env://DB_SECRET@db.host") == "localhost"

    def test_e_true_f_false_g_true_key_missing_returns_none(self, r):
        """E=True, F=False, G=True: JSON キーが存在しない → None"""
        with patch.dict(os.environ, {"MY_SECRET": '{"a": "1"}'}):
            assert r.read("env://MY_SECRET@missing_key") is None

    def test_e_true_f_false_g_true_invalid_json_returns_none(self, r):
        """E=True, F=False, G=True: 環境変数の値が不正 JSON → None"""
        with patch.dict(os.environ, {"MY_SECRET": "not_json"}):
            assert r.read("env://MY_SECRET@key") is None


# ======================================================================
# DotEnvSecretResolver.write
# MCDC:
#   条件H: env_match
#   条件I: os.path.exists(dotenv_path)
#   条件J: line が env_var_name= で始まる
#   条件K: current_value == secret_value
#   条件L: not updated
#   条件M: new_env_lines[-1] が \n で終わらない
# ======================================================================
class TestDotEnvSecretResolverWrite:

    @pytest.fixture
    def r(self):
        r = _make_dotenv_resolver()
        r.dotenv_path = "/fake/.env"
        return r

    def test_h_false_invalid_format_raises(self, r):
        """H=False: 不正フォーマット → SecretWriteError"""
        with pytest.raises(SecretWriteError):
            r.write("invalid://reference", "value")

    def test_i_false_file_not_exists_creates_new(self, r):
        """I=False: .env ファイルが存在しない → 新規作成"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=False), \
             patch("portalocker.Lock", mock_lock), \
             patch.dict(os.environ, {}, clear=False):
            r.write("env://NEW_VAR", "new_value")
        written = mock_file.writelines.call_args[0][0]
        assert any("NEW_VAR=new_value" in line for line in written)

    def test_i_true_l_true_new_key_appended(self, r):
        """I=True, J=False, L=True: 既存ファイルに新しいキーを末尾追記"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="EXISTING=val\n")):
            r.write("env://NEW_VAR", "new_value")
        written = mock_file.writelines.call_args[0][0]
        assert any("NEW_VAR=new_value" in line for line in written)
        assert any("EXISTING=val" in line for line in written)

    def test_j_true_k_false_existing_key_updated(self, r):
        """I=True, J=True, K=False, L=False: 既存キーの値を更新"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="MY_VAR=old\n")):
            r.write("env://MY_VAR", "new")
        written = mock_file.writelines.call_args[0][0]
        assert any("MY_VAR=new" in line for line in written)
        assert not any("MY_VAR=old" in line for line in written)

    def test_j_true_k_true_same_value_skips_write(self, r):
        """I=True, J=True, K=True: 同値 → early return で書き込みなし"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="MY_VAR=same\n")):
            r.write("env://MY_VAR", "same")
        mock_file.writelines.assert_not_called()

    def test_m_true_adds_newline_before_append(self, r):
        """I=True, L=True, M=True: 末尾に改行のない既存ファイルへの追記で改行を挿入"""
        mock_lock, mock_file = _lock_mock()
        # 末尾が \n で終わっていない内容
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="EXISTING=val")):
            r.write("env://NEW_VAR", "new_value")
        written = mock_file.writelines.call_args[0][0]
        assert "\n" in written
        assert any("NEW_VAR=new_value" in line for line in written)

    def test_write_updates_os_environ(self, r):
        """write 後に os.environ にも反映される"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="")), \
             patch.dict(os.environ, {}, clear=False):
            r.write("env://SYNC_VAR", "synced")
            assert os.environ.get("SYNC_VAR") == "synced"
        os.environ.pop("SYNC_VAR", None)

    def test_io_error_raises_secret_write_error(self, r):
        """IOError → SecretWriteError"""
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data="")), \
             patch("portalocker.Lock", side_effect=IOError("disk full")):
            with pytest.raises(SecretWriteError, match="Failed to write to .env file"):
                r.write("env://MY_VAR", "value")

    def test_value_with_equals_sign(self, r):
        """値に '=' が含まれる場合も正しく書き込まれる (split('=', 1) の確認)"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="")):
            r.write("env://MY_VAR", "pass=word")
        written = mock_file.writelines.call_args[0][0]
        assert any("MY_VAR=pass=word" in line for line in written)


# ======================================================================
# AWSSecretResolver.__init__
# ======================================================================
class TestAWSSecretResolverInit:

    @patch("boto3.client")
    @patch("boto3.Session")
    def test_init_success(self, mock_session, mock_client):
        mock_session.return_value = Mock(region_name="ap-northeast-1")
        r = AWSSecretResolver()
        assert r.secretsmanager_client is not None
        assert r.ssm_client is not None
        assert r.kms_client is not None

    @patch("boto3.Session", side_effect=Exception("no credentials"))
    def test_init_failure_sets_clients_none(self, _):
        """Exception 発生時: 全クライアントが None になる"""
        r = AWSSecretResolver()
        assert r.secretsmanager_client is None
        assert r.ssm_client is None
        assert r.kms_client is None

    @patch("boto3.client")
    @patch("boto3.Session")
    def test_init_uses_default_region_when_none(self, mock_session, mock_client):
        """region_name が None の場合 ap-northeast-1 をデフォルト使用"""
        mock_session.return_value = Mock(region_name=None)
        AWSSecretResolver()
        for c in mock_client.call_args_list:
            assert c.kwargs.get("region_name") == "ap-northeast-1"


# ======================================================================
# AWSSecretResolver._resolve_nested_key
# (DotEnv と同実装だが独立クラス)
# MCDC: 条件A/B/C/D
# ======================================================================
class TestAWSResolveNestedKey:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_a_false_invalid_json_returns_none(self, r):
        """A=False: 不正JSON → None"""
        assert r._resolve_nested_key("not_json", "key") is None

    def test_single_key(self, r):
        """A=True, C=True, D=False: 単一キー"""
        assert r._resolve_nested_key('{"k": "v"}', "k") == "v"

    def test_d_true_key_not_found_returns_none(self, r):
        """A=True, D=True: キー不存在 → None"""
        assert r._resolve_nested_key('{"a": "x"}', "missing") is None

    def test_c_false_value_not_dict_returns_none(self, r):
        """A=True, C=False: 値が dict でない → None"""
        assert r._resolve_nested_key('{"a": 42}', "a.b") is None

    def test_b_true_nested_string_reparsed(self, r):
        """A=True, B=True: str → 再parse"""
        value = json.dumps({"a": json.dumps({"b": "deep"})})
        assert r._resolve_nested_key(value, "a.b") == "deep"

    def test_multi_level_nesting(self, r):
        """多段ネスト a.b.c"""
        value = json.dumps({"a": {"b": {"c": "deep"}}})
        assert r._resolve_nested_key(value, "a.b.c") == "deep"


# ======================================================================
# AWSSecretResolver._set_nested_key
# MCDC:
#   単一キー / 2段 / 3段 / 既存値更新 / 他キー保持 / 中間が非dict
# ======================================================================
class TestSetNestedKey:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_single_key(self, r):
        assert r._set_nested_key({}, "k", "v") == {"k": "v"}

    def test_two_level(self, r):
        assert r._set_nested_key({}, "a.b", "v") == {"a": {"b": "v"}}

    def test_three_level(self, r):
        assert r._set_nested_key({}, "a.b.c", "v") == {"a": {"b": {"c": "v"}}}

    def test_updates_existing_nested_value(self, r):
        data = {"a": {"b": "old"}}
        assert r._set_nested_key(data, "a.b", "new") == {"a": {"b": "new"}}

    def test_preserves_other_keys(self, r):
        data = {"x": "keep"}
        result = r._set_nested_key(data, "a.b", "v")
        assert result["x"] == "keep"
        assert result["a"]["b"] == "v"

    def test_overwrites_non_dict_intermediate(self, r):
        """中間キーが dict でない場合は dict で上書き"""
        data = {"a": "string"}
        assert r._set_nested_key(data, "a.b", "v") == {"a": {"b": "v"}}


# ======================================================================
# AWSSecretResolver.read (prefix ルーティング)
# MCDC:
#   条件N: prefix (secretsmanager / parameterstore / kms_decrypt / 不明)
# ======================================================================
class TestAWSSecretResolverRead:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_unsupported_prefix_returns_none(self, r):
        """N=不明: 未知プレフィックス → None"""
        assert r.read("unsupported://ref") is None

    def test_routes_to_secretsmanager(self, r):
        """N=secretsmanager: _read_from_secretsmanager() に委譲"""
        r._read_from_secretsmanager = Mock(return_value="val")
        result = r.read("aws_secretsmanager://my-secret")
        r._read_from_secretsmanager.assert_called_once_with("aws_secretsmanager://my-secret")
        assert result == "val"

    def test_routes_to_parameterstore(self, r):
        """N=parameterstore: _read_from_parameterstore() に委譲"""
        r._read_from_parameterstore = Mock(return_value="val")
        result = r.read("aws_parameterstore://my-param")
        r._read_from_parameterstore.assert_called_once()
        assert result == "val"

    def test_routes_to_kms_decrypt(self, r):
        """N=kms_decrypt: _decrypt_with_kms() に委譲"""
        r._decrypt_with_kms = Mock(return_value="val")
        result = r.read("aws_kms_decrypt://cipher")
        r._decrypt_with_kms.assert_called_once()
        assert result == "val"


# ======================================================================
# AWSSecretResolver._read_from_secretsmanager
# MCDC:
#   条件O: sm_match (正規表現マッチ)
#   条件P: secret_string が truthy
#   条件Q: json_key_path が truthy
# ======================================================================
class TestReadFromSecretsManager:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_o_false_invalid_reference_returns_none(self, r):
        """O=False: 不正参照 (aws_secretsmanager:// 後が空) → None"""
        assert r._read_from_secretsmanager("aws_secretsmanager://") is None

    def test_p_false_empty_secret_string_returns_none(self, r):
        """O=True, P=False: SecretString が空文字 → None"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": ""}
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret") is None

    def test_o_true_p_true_q_false_plain_secret(self, r):
        """O=True, P=True, Q=False: JSONキーなし → 全体を返す"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {
            "SecretString": "plain_value"
        }
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret") == "plain_value"

    def test_o_true_p_true_q_true_json_key_single(self, r):
        """O=True, P=True, Q=True: 単一 JSON キー"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"username": "alice"})
        }
        result = r._read_from_secretsmanager("aws_secretsmanager://my-secret@username")
        assert result == "alice"

    def test_o_true_p_true_q_true_json_key_nested(self, r):
        """O=True, P=True, Q=True: 多段ネスト JSON キー (a.b)"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"db": {"host": "localhost"}})
        }
        result = r._read_from_secretsmanager("aws_secretsmanager://my-secret@db.host")
        assert result == "localhost"

    def test_client_error_returns_none(self, r):
        """O=True: ClientError → None"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.side_effect = _client_error("AccessDenied")
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret") is None


# ======================================================================
# AWSSecretResolver._write_to_secretsmanager
# MCDC:
#   条件R: sm_match
#   条件S: ResourceNotFoundException (新規 vs 更新)
#   条件T: json_key あり
#   条件U: current_secret が有効 JSON
# ======================================================================
class TestWriteToSecretsManager:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_r_false_invalid_reference_raises(self, r):
        """R=False: 不正参照 → SecretWriteError"""
        with pytest.raises(SecretWriteError):
            r._write_to_secretsmanager("aws_secretsmanager://", "value")

    def test_s_true_t_false_create_without_json_key(self, r):
        """R=True, S=True, T=False: 新規作成 (JSONキーなし) → そのまま create"""
        r.secretsmanager_client = Mock()
        _resource_not_found(r.secretsmanager_client)
        r._write_to_secretsmanager("aws_secretsmanager://new-secret", "plain_value")
        r.secretsmanager_client.create_secret.assert_called_once_with(
            Name="new-secret", SecretString="plain_value"
        )

    def test_s_true_t_true_create_with_json_key_nested(self, r):
        """R=True, S=True, T=True: 新規作成 (JSONキーあり) → ネスト構造で create"""
        r.secretsmanager_client = Mock()
        _resource_not_found(r.secretsmanager_client)
        r._write_to_secretsmanager("aws_secretsmanager://new-secret@a.b", "val")
        call_args = r.secretsmanager_client.create_secret.call_args
        payload = json.loads(call_args.kwargs["SecretString"])
        assert payload == {"a": {"b": "val"}}

    def test_s_false_t_false_update_without_json_key(self, r):
        """R=True, S=False, T=False: 既存更新 (JSONキーなし) → 値をそのまま put"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": "old"}
        r._write_to_secretsmanager("aws_secretsmanager://my-secret", "new_value")
        r.secretsmanager_client.put_secret_value.assert_called_once_with(
            SecretId="my-secret", SecretString="new_value"
        )

    def test_s_false_t_true_update_with_json_key_nested(self, r):
        """R=True, S=False, T=True: 既存JSON に json_key をネスト構造でマージ
        data['a']['b'] = 'val' というネスト構造で書き込まれる"""
        r.secretsmanager_client = Mock()
        existing = json.dumps({"other": "keep"})
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": existing}
        r._write_to_secretsmanager("aws_secretsmanager://my-secret@a.b", "new_val")
        call_args = r.secretsmanager_client.put_secret_value.call_args
        payload = json.loads(call_args.kwargs["SecretString"])
        assert payload["a"]["b"] == "new_val"
        assert payload["other"] == "keep"

    def test_u_false_invalid_existing_json_starts_fresh(self, r):
        """R=True, S=False, T=True, U=False: 既存が不正JSON → data={} から開始"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": "not_json"}
        r._write_to_secretsmanager("aws_secretsmanager://my-secret@key", "val")
        call_args = r.secretsmanager_client.put_secret_value.call_args
        payload = json.loads(call_args.kwargs["SecretString"])
        assert payload == {"key": "val"}

    def test_exception_raises_secret_write_error(self, r):
        """R=True: 予期しない例外 → SecretWriteError"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.side_effect = Exception("network error")
        with pytest.raises(SecretWriteError):
            r._write_to_secretsmanager("aws_secretsmanager://my-secret", "val")

    def test_write_read_roundtrip_nested_key(self, r):
        """write(a.b) したものを read(a.b) で取得できる"""
        stored: dict = {}

        def fake_get(**kw):
            return {"SecretString": stored.get("val", "{}")}

        def fake_put(**kw):
            stored["val"] = kw["SecretString"]

        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value = Mock(side_effect=lambda **kw: fake_get(**kw))
        r.secretsmanager_client.put_secret_value = Mock(side_effect=lambda **kw: fake_put(**kw))

        r._write_to_secretsmanager("aws_secretsmanager://s@a.b", "hello")
        result = r._read_from_secretsmanager("aws_secretsmanager://s@a.b")
        assert result == "hello"


# ======================================================================
# AWSSecretResolver.write (prefix ルーティング)
# MCDC:
#   条件: prefix (secretsmanager / parameterstore / kms_encrypt / 不明)
# ======================================================================
class TestAWSSecretResolverWrite:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_unsupported_prefix_raises_secret_write_error(self, r):
        """不明プレフィックス → SecretWriteError
        logger.warning + return None のみで失敗を検知できなかった"""
        with pytest.raises(SecretWriteError, match="unsupported reference format"):
            r.write("unsupported://ref", "value")

    def test_routes_to_secretsmanager(self, r):
        r._write_to_secretsmanager = Mock()
        r.write("aws_secretsmanager://my-secret", "val")
        r._write_to_secretsmanager.assert_called_once()

    def test_routes_to_parameterstore(self, r):
        r._write_to_parameterstore = Mock()
        r.write("aws_parameterstore://my-param", "val")
        r._write_to_parameterstore.assert_called_once()

    def test_routes_to_kms_encrypt(self, r):
        r._encrypt_with_kms = Mock()
        r.write("aws_kms_encrypt://key-id", "val")
        r._encrypt_with_kms.assert_called_once()

    def test_kms_encrypt_passes_encryption_context(self, r):
        """encryption_context kwarg が _encrypt_with_kms に渡される"""
        r._encrypt_with_kms = Mock()
        ctx = {"purpose": "test"}
        r.write("aws_kms_encrypt://key-id", "val", encryption_context=ctx)
        args, kwargs = r._encrypt_with_kms.call_args
        passed_ctx = kwargs.get("encryption_context") or (args[2] if len(args) > 2 else None)
        assert passed_ctx == ctx


# ======================================================================
# AWSSecretResolver._read_from_parameterstore
# MCDC:
#   条件V: ps_match
#   条件W: json_key_path
# ======================================================================
class TestReadFromParameterStore:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_v_false_invalid_reference_returns_none(self, r):
        """V=False: aws_parameterstore:// 後が空 → None"""
        assert r._read_from_parameterstore("aws_parameterstore://") is None

    def test_v_true_w_false_plain_value(self, r):
        """V=True, W=False: JSONキーなし → 値そのまま"""
        r.ssm_client = Mock()
        r.ssm_client.get_parameter.return_value = {"Parameter": {"Value": "plain_value"}}
        assert r._read_from_parameterstore("aws_parameterstore:///my/param") == "plain_value"

    def test_v_true_w_true_json_key(self, r):
        """V=True, W=True: JSONキーあり → 値を解析してキーを返す"""
        r.ssm_client = Mock()
        r.ssm_client.get_parameter.return_value = {
            "Parameter": {"Value": json.dumps({"host": "db.example.com"})}
        }
        result = r._read_from_parameterstore("aws_parameterstore:///my/param@host")
        assert result == "db.example.com"

    def test_client_error_returns_none(self, r):
        """V=True: ClientError → None"""
        r.ssm_client = Mock()
        r.ssm_client.get_parameter.side_effect = _client_error("ParameterNotFound")
        assert r._read_from_parameterstore("aws_parameterstore:///my/param") is None


# ======================================================================
# AWSSecretResolver._write_to_parameterstore
# ======================================================================
class TestWriteToParameterStore:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_invalid_reference_raises(self, r):
        """不正参照 → SecretWriteError"""
        with pytest.raises(SecretWriteError):
            r._write_to_parameterstore("aws_parameterstore://", "value")

    def test_success(self, r):
        """正常系: put_parameter が正しい引数で呼ばれる"""
        r.ssm_client = Mock()
        r._write_to_parameterstore("aws_parameterstore:///my/param", "val")
        r.ssm_client.put_parameter.assert_called_once_with(
            Name="/my/param", Value="val", Type="SecureString", Overwrite=True
        )

    def test_exception_raises_secret_write_error(self, r):
        """Exception → SecretWriteError"""
        r.ssm_client = Mock()
        r.ssm_client.put_parameter.side_effect = Exception("error")
        with pytest.raises(SecretWriteError):
            r._write_to_parameterstore("aws_parameterstore:///my/param", "val")


# ======================================================================
# AWSSecretResolver._decrypt_with_kms
# MCDC:
#   kms_match / json_key_path / base64デコードエラー
# ======================================================================
class TestDecryptWithKms:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_invalid_reference_returns_none(self, r):
        """不正参照 (空) → None"""
        assert r._decrypt_with_kms("aws_kms_decrypt://") is None

    def test_plain_decrypt(self, r):
        """JSONキーなし: 復号結果をそのまま返す"""
        r.kms_client = Mock()
        ciphertext = base64.b64encode(b"encrypted_data").decode()
        r.kms_client.decrypt.return_value = {"Plaintext": b"secret_text"}
        result = r._decrypt_with_kms(f"aws_kms_decrypt://{ciphertext}")
        assert result == "secret_text"

    def test_decrypt_with_json_key(self, r):
        """JSONキーあり: 復号後に JSON 解析してキーを返す"""
        r.kms_client = Mock()
        payload = json.dumps({"token": "abc123"}).encode()
        ciphertext = base64.b64encode(b"encrypted").decode()
        r.kms_client.decrypt.return_value = {"Plaintext": payload}
        result = r._decrypt_with_kms(f"aws_kms_decrypt://{ciphertext}@token")
        assert result == "abc123"

    def test_invalid_base64_returns_none(self, r):
        """base64 デコードエラー → None"""
        r.kms_client = Mock()
        r.kms_client.decrypt.side_effect = Exception("decode error")
        assert r._decrypt_with_kms("aws_kms_decrypt://!!!invalid!!!") is None

    def test_kms_exception_returns_none(self, r):
        """KMS 例外 → None"""
        r.kms_client = Mock()
        r.kms_client.decrypt.side_effect = Exception("KMS error")
        ciphertext = base64.b64encode(b"data").decode()
        assert r._decrypt_with_kms(f"aws_kms_decrypt://{ciphertext}") is None


# ======================================================================
# AWSSecretResolver._encrypt_with_kms
# ======================================================================
class TestEncryptWithKms:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_invalid_reference_raises(self, r):
        """不正参照 → SecretWriteError"""
        with pytest.raises(SecretWriteError, match="Invalid KMS encrypt reference"):
            r._encrypt_with_kms("aws_kms_encrypt://", "plaintext")

    def test_success_returns_base64(self, r):
        """正常系: base64 エンコードされた暗号文を返す"""
        r.kms_client = Mock()
        r.kms_client.encrypt.return_value = {"CiphertextBlob": b"encrypted_bytes"}
        result = r._encrypt_with_kms("aws_kms_encrypt://my-key-id", "secret")
        assert result == base64.b64encode(b"encrypted_bytes").decode()

    def test_with_encryption_context(self, r):
        """encryption_context が KMS に渡される"""
        r.kms_client = Mock()
        r.kms_client.encrypt.return_value = {"CiphertextBlob": b"data"}
        ctx = {"purpose": "test"}
        r._encrypt_with_kms("aws_kms_encrypt://my-key-id", "secret", ctx)
        assert r.kms_client.encrypt.call_args.kwargs["EncryptionContext"] == ctx

    def test_exception_raises_secret_write_error(self, r):
        """Exception → SecretWriteError"""
        r.kms_client = Mock()
        r.kms_client.encrypt.side_effect = Exception("KMS error")
        with pytest.raises(SecretWriteError):
            r._encrypt_with_kms("aws_kms_encrypt://my-key-id", "secret")


# ======================================================================
# get_secret_resolver (ファクトリ)
# patch先: secret_resolver モジュール内の is_running_on_aws
# ======================================================================
class TestGetSecretResolver:
    """
    get_secret_resolver() のテスト。

    【設計上の制約と解決策】
    secret_resolver.py 末尾の `secret_resolver = get_secret_resolver()` が
    モジュールロード時に実行されるため、テスト時に is_running_on_aws を
    patch しても遅い (シングルトンが先に生成済み)。
    また core/infrastructure/__init__.py で
    `from .secret_resolver import secret_resolver` がエクスポートされると
    `core.infrastructure.secret_resolver` がモジュールではなくインスタンスを指し
    patch/patch.object いずれも AttributeError になる。

    解決策: mock に頼らず環境変数を直接操作する。
    is_running_on_aws() は GLUE_VERSION 等の環境変数のみで判定するため、
    環境変数を制御することで確実に True/False を切り替えられる。
    @lru_cache のリセットは autouse fixture で行う。
    """

    _AWS_ENV_VARS = ("AWS_LAMBDA_FUNCTION_NAME", "GLUE_VERSION", "AWS_EXECUTION_ENV")

    @pytest.fixture(autouse=True)
    def reset_aws_cache(self):
        """各テスト前後に lru_cache と環境変数をリセット"""
        saved = {v: os.environ.get(v) for v in self._AWS_ENV_VARS}
        for v in self._AWS_ENV_VARS:
            os.environ.pop(v, None)
        _is_running_on_aws.cache_clear()
        yield
        for v in self._AWS_ENV_VARS:
            os.environ.pop(v, None)
        for v, val in saved.items():
            if val is not None:
                os.environ[v] = val
        _is_running_on_aws.cache_clear()

    def test_returns_aws_resolver_on_aws(self):
        """GLUE_VERSION を設定 → is_running_on_aws()=True → AWSSecretResolver を返す"""
        os.environ["GLUE_VERSION"] = "4.0"
        with patch("boto3.Session") as mock_session, patch("boto3.client"):
            mock_session.return_value = Mock(region_name="ap-northeast-1")
            result = get_secret_resolver()
        assert isinstance(result, AWSSecretResolver)

    def test_returns_dotenv_resolver_on_local(self):
        """AWS環境変数なし → is_running_on_aws()=False → DotEnvSecretResolver を返す"""
        # autouse fixture で全 AWS 環境変数クリア済み
        result = get_secret_resolver()
        assert isinstance(result, DotEnvSecretResolver)


# ======================================================================
# Integration
# ======================================================================
class TestIntegration:

    def test_dotenv_write_then_read(self):
        """write → read の往復テスト"""
        r = _make_dotenv_resolver()
        r.dotenv_path = "/fake/.env"
        mock_lock, mock_file = _lock_mock()

        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="")), \
             patch.dict(os.environ, {"WRITE_READ_KEY": "test_value_new"}):
            r.write("env://WRITE_READ_KEY", "test_value_new")
            result = r.read("env://WRITE_READ_KEY")

        assert result == "test_value_new"
        os.environ.pop("WRITE_READ_KEY", None)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])