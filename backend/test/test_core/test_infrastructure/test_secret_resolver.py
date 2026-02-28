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
    with patch("boto3.Session") as ms, patch("boto3.client"):
        ms.return_value = Mock(region_name="ap-northeast-1")
        return AWSSecretResolver()


def _make_dotenv_resolver() -> DotEnvSecretResolver:
    with patch("dotenv.find_dotenv", return_value=""), \
         patch("dotenv.load_dotenv"):
        return DotEnvSecretResolver()


def _lock_mock():
    mock_file = MagicMock()
    mock_file.__enter__ = Mock(return_value=mock_file)
    mock_file.__exit__ = Mock(return_value=False)
    mock_lock = MagicMock(return_value=mock_file)
    return mock_lock, mock_file


def _resource_not_found(mock_sm):
    exc_cls = type("ResourceNotFoundException", (ClientError,), {})
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
# MCDC: 条件A/B/C/D
# ======================================================================
class TestDotEnvResolveNestedKey:

    @pytest.fixture
    def r(self):
        return _make_dotenv_resolver()

    def test_a_false_invalid_json_returns_none(self, r):
        """A=False: 不正JSON → None"""
        assert r._resolve_nested_key("not_json", "key") is None

    def test_single_key_success(self, r):
        """A=True, C=True, D=False: 単一キー"""
        assert r._resolve_nested_key('{"k": "v"}', "k") == "v"

    def test_d_true_key_not_found_returns_none(self, r):
        """A=True, D=True: キー不存在 → None"""
        assert r._resolve_nested_key('{"a": "x"}', "missing") is None

    def test_c_false_value_not_dict_returns_none(self, r):
        """A=True, C=False: 途中値がdict非 → None"""
        assert r._resolve_nested_key('{"a": 42}', "a.b") is None

    def test_b_true_nested_string_reparsed(self, r):
        """A=True, B=True: 値がJSON文字列 → 再parse"""
        value = json.dumps({"a": json.dumps({"b": "deep"})})
        assert r._resolve_nested_key(value, "a.b") == "deep"

    def test_multi_level_nesting(self, r):
        """多段ネスト a.b.c"""
        value = json.dumps({"a": {"b": {"c": "deep"}}})
        assert r._resolve_nested_key(value, "a.b.c") == "deep"

    def test_numeric_value_converted_to_str(self, r):
        """数値はstrに変換"""
        assert r._resolve_nested_key('{"n": 123}', "n") == "123"


# ======================================================================
# DotEnvSecretResolver.read
# MCDC: 条件E/F/G
# ======================================================================
class TestDotEnvSecretResolverRead:

    @pytest.fixture
    def r(self):
        return _make_dotenv_resolver()

    def test_e_false_invalid_format_returns_none(self, r):
        """E=False: 不正フォーマット → None"""
        assert r.read("invalid://reference") is None

    def test_e_true_f_false_g_false_plain_value(self, r):
        """E=True, F=False, G=False: plain値をそのまま返す"""
        with patch.dict(os.environ, {"MY_VAR": "hello"}):
            assert r.read("env://MY_VAR") == "hello"

    def test_e_true_f_true_unset_var_returns_none(self, r):
        """E=True, F=True: 未設定 → None"""
        assert r.read("env://NONEXISTENT_VAR_XYZ_12345") is None

    def test_e_true_f_false_g_true_single_json_key(self, r):
        """E=True, F=False, G=True: 単一JSONキー"""
        data = json.dumps({"user": "alice"})
        with patch.dict(os.environ, {"MY_SECRET": data}):
            assert r.read("env://MY_SECRET@user") == "alice"

    def test_e_true_f_false_g_true_nested_json_key(self, r):
        """E=True, F=False, G=True: 多段ネストJSONキー"""
        data = json.dumps({"db": {"host": "localhost"}})
        with patch.dict(os.environ, {"DB_SECRET": data}):
            assert r.read("env://DB_SECRET@db.host") == "localhost"

    def test_e_true_f_false_g_true_key_missing_returns_none(self, r):
        """E=True, F=False, G=True: JSONキー不存在 → None"""
        with patch.dict(os.environ, {"MY_SECRET": '{"a": "1"}'}):
            assert r.read("env://MY_SECRET@missing_key") is None

    def test_e_true_f_false_g_true_invalid_json_returns_none(self, r):
        """E=True, F=False, G=True: 値が不正JSON → None"""
        with patch.dict(os.environ, {"MY_SECRET": "not_json"}):
            assert r.read("env://MY_SECRET@key") is None


# ======================================================================
# DotEnvSecretResolver.write
# MCDC: 条件H/I/J/K/L/M
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
        """I=False: .envファイル不存在 → 新規作成"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=False), \
             patch("portalocker.Lock", mock_lock), \
             patch.dict(os.environ, {}, clear=False):
            r.write("env://NEW_VAR", "new_value")
        written = mock_file.writelines.call_args[0][0]
        assert any("NEW_VAR=new_value" in line for line in written)

    def test_i_true_l_true_new_key_appended(self, r):
        """I=True, J=False, L=True: 新キーを末尾追記"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="EXISTING=val\n")):
            r.write("env://NEW_VAR", "new_value")
        written = mock_file.writelines.call_args[0][0]
        assert any("NEW_VAR=new_value" in line for line in written)
        assert any("EXISTING=val" in line for line in written)

    def test_j_true_k_false_existing_key_updated(self, r):
        """I=True, J=True, K=False, L=False: 既存キーを更新"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="MY_VAR=old\n")):
            r.write("env://MY_VAR", "new")
        written = mock_file.writelines.call_args[0][0]
        assert any("MY_VAR=new" in line for line in written)
        assert not any("MY_VAR=old" in line for line in written)

    def test_j_true_k_true_same_value_skips_write(self, r):
        """I=True, J=True, K=True: 同値 → early returnで書き込みなし"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="MY_VAR=same\n")):
            r.write("env://MY_VAR", "same")
        mock_file.writelines.assert_not_called()

    def test_m_true_adds_newline_before_append(self, r):
        """I=True, L=True, M=True: 末尾改行なしファイルへの追記で改行挿入"""
        mock_lock, mock_file = _lock_mock()
        with patch("os.path.exists", return_value=True), \
             patch("portalocker.Lock", mock_lock), \
             patch("builtins.open", mock_open(read_data="EXISTING=val")):
            r.write("env://NEW_VAR", "new_value")
        written = mock_file.writelines.call_args[0][0]
        assert "\n" in written
        assert any("NEW_VAR=new_value" in line for line in written)

    def test_write_updates_os_environ(self, r):
        """write後にos.environにも反映される"""
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
        """値に'='が含まれる場合も正しく書き込まれる"""
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
        """Exception発生時: 全クライアントがNoneになる"""
        r = AWSSecretResolver()
        assert r.secretsmanager_client is None
        assert r.ssm_client is None
        assert r.kms_client is None

    @patch("boto3.client")
    @patch("boto3.Session")
    def test_init_uses_default_region_when_none(self, mock_session, mock_client):
        """region_nameがNoneの場合 ap-northeast-1 をデフォルト使用"""
        mock_session.return_value = Mock(region_name=None)
        AWSSecretResolver()
        for c in mock_client.call_args_list:
            assert c.kwargs.get("region_name") == "ap-northeast-1"


# ======================================================================
# AWSSecretResolver._resolve_nested_key
# MCDC: 条件A/B/C/D
# ======================================================================
class TestAWSResolveNestedKey:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_a_false_invalid_json_returns_none(self, r):
        assert r._resolve_nested_key("not_json", "key") is None

    def test_single_key(self, r):
        assert r._resolve_nested_key('{"k": "v"}', "k") == "v"

    def test_d_true_key_not_found_returns_none(self, r):
        assert r._resolve_nested_key('{"a": "x"}', "missing") is None

    def test_c_false_value_not_dict_returns_none(self, r):
        assert r._resolve_nested_key('{"a": 42}', "a.b") is None

    def test_b_true_nested_string_reparsed(self, r):
        value = json.dumps({"a": json.dumps({"b": "deep"})})
        assert r._resolve_nested_key(value, "a.b") == "deep"

    def test_multi_level_nesting(self, r):
        value = json.dumps({"a": {"b": {"c": "deep"}}})
        assert r._resolve_nested_key(value, "a.b.c") == "deep"


# ======================================================================
# AWSSecretResolver._set_nested_key (修正3)
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
        assert r._set_nested_key({"a": {"b": "old"}}, "a.b", "new") == {"a": {"b": "new"}}

    def test_preserves_other_keys(self, r):
        result = r._set_nested_key({"x": "keep"}, "a.b", "v")
        assert result["x"] == "keep"
        assert result["a"]["b"] == "v"

    def test_overwrites_non_dict_intermediate(self, r):
        """中間キーがdict非の場合はdictで上書き"""
        assert r._set_nested_key({"a": "string"}, "a.b", "v") == {"a": {"b": "v"}}


# ======================================================================
# AWSSecretResolver.read (prefix ルーティング)
# MCDC: 条件N
# ======================================================================
class TestAWSSecretResolverRead:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_unsupported_prefix_returns_none(self, r):
        assert r.read("unsupported://ref") is None

    def test_routes_to_secretsmanager(self, r):
        r._read_from_secretsmanager = Mock(return_value="val")
        assert r.read("aws_secretsmanager://my-secret") == "val"
        r._read_from_secretsmanager.assert_called_once_with("aws_secretsmanager://my-secret")

    def test_routes_to_parameterstore(self, r):
        r._read_from_parameterstore = Mock(return_value="val")
        assert r.read("aws_parameterstore://my-param") == "val"
        r._read_from_parameterstore.assert_called_once()

    def test_routes_to_kms_decrypt(self, r):
        r._decrypt_with_kms = Mock(return_value="val")
        assert r.read("aws_kms_decrypt://cipher") == "val"
        r._decrypt_with_kms.assert_called_once()


# ======================================================================
# AWSSecretResolver._read_from_secretsmanager
# MCDC: 条件O/P/Q
# ======================================================================
class TestReadFromSecretsManager:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_o_false_invalid_reference_returns_none(self, r):
        """O=False: aws_secretsmanager:// 後が空 → None"""
        assert r._read_from_secretsmanager("aws_secretsmanager://") is None

    def test_p_false_empty_secret_string_returns_none(self, r):
        """O=True, P=False: SecretStringが空 → None"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": ""}
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret") is None

    def test_o_true_p_true_q_false_plain_secret(self, r):
        """O=True, P=True, Q=False: JSONキーなし → 全体を返す"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": "plain_value"}
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret") == "plain_value"

    def test_o_true_p_true_q_true_json_key_single(self, r):
        """O=True, P=True, Q=True: 単一JSONキー"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"username": "alice"})
        }
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret@username") == "alice"

    def test_o_true_p_true_q_true_json_key_nested(self, r):
        """O=True, P=True, Q=True: 多段ネストJSONキー"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"db": {"host": "localhost"}})
        }
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret@db.host") == "localhost"

    def test_client_error_returns_none(self, r):
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.side_effect = _client_error("AccessDenied")
        assert r._read_from_secretsmanager("aws_secretsmanager://my-secret") is None


# ======================================================================
# AWSSecretResolver._write_to_secretsmanager
# MCDC: 条件R/S/T/U
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
        """R=True, S=True, T=False: 新規作成 (JSONキーなし)"""
        r.secretsmanager_client = Mock()
        _resource_not_found(r.secretsmanager_client)
        r._write_to_secretsmanager("aws_secretsmanager://new-secret", "plain_value")
        r.secretsmanager_client.create_secret.assert_called_once_with(
            Name="new-secret", SecretString="plain_value"
        )

    def test_s_true_t_true_create_with_nested_json_key(self, r):
        """R=True, S=True, T=True: 新規作成 (JSONキーあり) → ネスト構造で create (修正3)"""
        r.secretsmanager_client = Mock()
        _resource_not_found(r.secretsmanager_client)
        r._write_to_secretsmanager("aws_secretsmanager://new-secret@a.b", "val")
        payload = json.loads(r.secretsmanager_client.create_secret.call_args.kwargs["SecretString"])
        assert payload == {"a": {"b": "val"}}

    def test_s_false_t_false_update_without_json_key(self, r):
        """R=True, S=False, T=False: 既存更新 (JSONキーなし)"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": "old"}
        r._write_to_secretsmanager("aws_secretsmanager://my-secret", "new_value")
        r.secretsmanager_client.put_secret_value.assert_called_once_with(
            SecretId="my-secret", SecretString="new_value"
        )

    def test_s_false_t_true_update_with_nested_json_key(self, r):
        """R=True, S=False, T=True: 既存JSONにネスト構造でマージ (修正3)
        修正前: data['a.b'] = 'val' というフラットなキーで書き込まれた
        修正後: data['a']['b'] = 'val' というネスト構造で書き込まれる"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"other": "keep"})
        }
        r._write_to_secretsmanager("aws_secretsmanager://my-secret@a.b", "new_val")
        payload = json.loads(r.secretsmanager_client.put_secret_value.call_args.kwargs["SecretString"])
        assert payload["a"]["b"] == "new_val"
        assert payload["other"] == "keep"

    def test_u_false_invalid_existing_json_starts_fresh(self, r):
        """R=True, S=False, T=True, U=False: 既存が不正JSON → data={}から開始"""
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.return_value = {"SecretString": "not_json"}
        r._write_to_secretsmanager("aws_secretsmanager://my-secret@key", "val")
        payload = json.loads(r.secretsmanager_client.put_secret_value.call_args.kwargs["SecretString"])
        assert payload == {"key": "val"}

    def test_exception_raises_secret_write_error(self, r):
        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value.side_effect = Exception("network error")
        with pytest.raises(SecretWriteError):
            r._write_to_secretsmanager("aws_secretsmanager://my-secret", "val")

    def test_write_read_roundtrip_nested_key(self, r):
        """修正3の往復確認: write(a.b)したものをread(a.b)で取得できる"""
        stored: dict = {}

        def fake_get(**kw):
            return {"SecretString": stored.get("val", "{}")}

        def fake_put(**kw):
            stored["val"] = kw["SecretString"]

        r.secretsmanager_client = Mock()
        r.secretsmanager_client.get_secret_value = Mock(side_effect=lambda **kw: fake_get(**kw))
        r.secretsmanager_client.put_secret_value = Mock(side_effect=lambda **kw: fake_put(**kw))

        r._write_to_secretsmanager("aws_secretsmanager://s@a.b", "hello")
        assert r._read_from_secretsmanager("aws_secretsmanager://s@a.b") == "hello"


# ======================================================================
# AWSSecretResolver.write (prefix ルーティング + 修正2)
# ======================================================================
class TestAWSSecretResolverWrite:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_unsupported_prefix_raises_secret_write_error(self, r):
        """修正2: 不明プレフィックス → SecretWriteError
        修正前は logger.warning + return None のみで失敗を検知できなかった"""
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
        r._encrypt_with_kms = Mock()
        ctx = {"purpose": "test"}
        r.write("aws_kms_encrypt://key-id", "val", encryption_context=ctx)
        args, kwargs = r._encrypt_with_kms.call_args
        passed_ctx = kwargs.get("encryption_context") or (args[2] if len(args) > 2 else None)
        assert passed_ctx == ctx


# ======================================================================
# AWSSecretResolver._read_from_parameterstore
# MCDC: 条件V/W
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
        """V=True, W=True: JSONキーあり"""
        r.ssm_client = Mock()
        r.ssm_client.get_parameter.return_value = {
            "Parameter": {"Value": json.dumps({"host": "db.example.com"})}
        }
        assert r._read_from_parameterstore("aws_parameterstore:///my/param@host") == "db.example.com"

    def test_client_error_returns_none(self, r):
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
        with pytest.raises(SecretWriteError):
            r._write_to_parameterstore("aws_parameterstore://", "value")

    def test_success(self, r):
        r.ssm_client = Mock()
        r._write_to_parameterstore("aws_parameterstore:///my/param", "val")
        r.ssm_client.put_parameter.assert_called_once_with(
            Name="/my/param", Value="val", Type="SecureString", Overwrite=True
        )

    def test_exception_raises_secret_write_error(self, r):
        r.ssm_client = Mock()
        r.ssm_client.put_parameter.side_effect = Exception("error")
        with pytest.raises(SecretWriteError):
            r._write_to_parameterstore("aws_parameterstore:///my/param", "val")


# ======================================================================
# AWSSecretResolver._decrypt_with_kms
# ======================================================================
class TestDecryptWithKms:

    @pytest.fixture
    def r(self):
        return _make_aws_resolver()

    def test_invalid_reference_returns_none(self, r):
        assert r._decrypt_with_kms("aws_kms_decrypt://") is None

    def test_plain_decrypt(self, r):
        r.kms_client = Mock()
        ciphertext = base64.b64encode(b"encrypted_data").decode()
        r.kms_client.decrypt.return_value = {"Plaintext": b"secret_text"}
        assert r._decrypt_with_kms(f"aws_kms_decrypt://{ciphertext}") == "secret_text"

    def test_decrypt_with_json_key(self, r):
        r.kms_client = Mock()
        payload = json.dumps({"token": "abc123"}).encode()
        ciphertext = base64.b64encode(b"encrypted").decode()
        r.kms_client.decrypt.return_value = {"Plaintext": payload}
        assert r._decrypt_with_kms(f"aws_kms_decrypt://{ciphertext}@token") == "abc123"

    def test_invalid_base64_returns_none(self, r):
        r.kms_client = Mock()
        r.kms_client.decrypt.side_effect = Exception("decode error")
        assert r._decrypt_with_kms("aws_kms_decrypt://!!!invalid!!!") is None

    def test_kms_exception_returns_none(self, r):
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
        with pytest.raises(SecretWriteError, match="Invalid KMS encrypt reference"):
            r._encrypt_with_kms("aws_kms_encrypt://", "plaintext")

    def test_success_returns_base64(self, r):
        r.kms_client = Mock()
        r.kms_client.encrypt.return_value = {"CiphertextBlob": b"encrypted_bytes"}
        result = r._encrypt_with_kms("aws_kms_encrypt://my-key-id", "secret")
        assert result == base64.b64encode(b"encrypted_bytes").decode()

    def test_with_encryption_context(self, r):
        r.kms_client = Mock()
        r.kms_client.encrypt.return_value = {"CiphertextBlob": b"data"}
        ctx = {"purpose": "test"}
        r._encrypt_with_kms("aws_kms_encrypt://my-key-id", "secret", ctx)
        assert r.kms_client.encrypt.call_args.kwargs["EncryptionContext"] == ctx

    def test_exception_raises_secret_write_error(self, r):
        r.kms_client = Mock()
        r.kms_client.encrypt.side_effect = Exception("KMS error")
        with pytest.raises(SecretWriteError):
            r._encrypt_with_kms("aws_kms_encrypt://my-key-id", "secret")


# ======================================================================
# get_secret_resolver (ファクトリ)
#
# 【patch不可の理由】
# core/infrastructure/__init__.py で
#   from .secret_resolver import secret_resolver
# がエクスポートされているため、
#   "core.infrastructure.secret_resolver" はモジュールではなくインスタンスを指す。
# @patch / patch.object(_sr_module, ...) いずれも AttributeError になる。
#
# 【解決策】
# mock に頼らず環境変数を直接操作する。
# is_running_on_aws() は環境変数のみで判定するため確実に制御できる。
# @lru_cache のリセットは autouse fixture で行う。
# ======================================================================
class TestGetSecretResolver:

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