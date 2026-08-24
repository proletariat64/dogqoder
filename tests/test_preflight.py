import json
import os
from collections.abc import Mapping
from io import StringIO
from pathlib import Path

import pytest
from qoder_agent_sdk import (
    AccessTokenAuthOptions,
    QoderAgentOptions,
    ServiceAccountAuthOptions,
)

from qworker.cli import run
from qworker.config import ConfigError, load_config
from qworker.domain import AuditContract
from qworker.preflight import (
    AuthSelection,
    DoctorResult,
    PreflightDiagnostic,
    RuntimeInfo,
    RuntimePreflight,
)
from qworker.qoder_sdk import (
    CommandResult,
    QoderPreflightBackend,
    build_configured_auditor_options,
)
from qworker.rpc import RPCClientError, RPCServer, call
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor, SupervisorError
from tests.fakes import FakeQoderTransport


def test_project_policy_can_only_narrow_user_policy(tmp_path: Path) -> None:
    user_config = tmp_path / "user.toml"
    user_config.write_text(
        """
[policy]
proactive_auditor = true
proactive_coder = true
coder_permission_mode = "acceptEdits"
coder_denied_tools = ["Bash"]
auditor_web_access = true
""".strip(),
        encoding="utf-8",
    )
    project = tmp_path / "project"
    project.mkdir()
    (project / ".qworker.toml").write_text(
        """
[policy]
proactive_auditor = false
proactive_coder = false
coder_permission_mode = "dontAsk"
coder_denied_tools = ["Write", "Bash"]
auditor_web_access = false
""".strip(),
        encoding="utf-8",
    )

    config = load_config(project, user_path=user_config)

    assert config.policy.proactive_auditor is False
    assert config.policy.proactive_coder is False
    assert config.policy.coder_permission_mode == "dontAsk"
    assert config.policy.coder_denied_tools == ("Bash", "Write")
    assert config.policy.auditor_web_access is False


def test_project_policy_expansion_requires_explicit_user_permission(
    tmp_path: Path,
) -> None:
    user_config = tmp_path / "user.toml"
    user_config.write_text("[policy]\nproactive_coder = false\n", encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    (project / ".qworker.toml").write_text(
        "[policy]\nproactive_coder = true\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_config(project, user_path=user_config)

    assert caught.value.code == "invalid_request"
    assert caught.value.message == (
        "Project configuration expands policy field 'proactive_coder'; "
        "user configuration must set project.allow_expansion = true."
    )


@pytest.mark.parametrize(
    ("user_policy", "project_policy", "field"),
    [
        (
            'coder_permission_mode = "dontAsk"',
            'coder_permission_mode = "acceptEdits"',
            "coder_permission_mode",
        ),
        (
            'coder_denied_tools = ["Bash"]',
            "coder_denied_tools = []",
            "coder_denied_tools",
        ),
    ],
)
def test_project_permission_expansion_is_rejected(
    tmp_path: Path,
    user_policy: str,
    project_policy: str,
    field: str,
) -> None:
    user_config = tmp_path / "user.toml"
    user_config.write_text(f"[policy]\n{user_policy}\n", encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    (project / ".qworker.toml").write_text(
        f"[policy]\n{project_policy}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_config(project, user_path=user_config)

    assert field in caught.value.message


def test_user_can_explicitly_allow_project_policy_expansion(tmp_path: Path) -> None:
    user_config = tmp_path / "user.toml"
    user_config.write_text(
        """
[project]
allow_expansion = true
[policy]
proactive_coder = false
coder_permission_mode = "dontAsk"
""".strip(),
        encoding="utf-8",
    )
    project = tmp_path / "project"
    project.mkdir()
    (project / ".qworker.toml").write_text(
        """
[policy]
proactive_coder = true
coder_permission_mode = "acceptEdits"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(project, user_path=user_config)

    assert config.policy.proactive_coder is True
    assert config.policy.coder_permission_mode == "acceptEdits"


def test_expansion_permission_does_not_allow_project_runtime_or_auth(
    tmp_path: Path,
) -> None:
    user_config = tmp_path / "user.toml"
    user_config.write_text(
        "[project]\nallow_expansion = true\n",
        encoding="utf-8",
    )
    project = tmp_path / "project"
    project.mkdir()
    (project / ".qworker.toml").write_text(
        "[auth]\nreuse_qodercli = true\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_config(project, user_path=user_config)

    assert caught.value.code == "invalid_request"
    assert caught.value.message == "Project configuration may only contain [policy]."


class FakePreflightBackend:
    def __init__(self) -> None:
        self.auth: list[AuthSelection] = []
        self.runtime_requests: list[Path | None] = []

    def sdk_version(self) -> str:
        return "1.0.13"

    async def resolve_runtime(self, explicit_path: Path | None) -> RuntimeInfo:
        self.runtime_requests.append(explicit_path)
        if explicit_path is None:
            return RuntimeInfo(
                recorded_path="bundled",
                executable=Path("/sdk/qodercli"),
                version="1.1.23",
            )
        return RuntimeInfo(
            recorded_path=str(explicit_path),
            executable=explicit_path,
            version="1.2.7",
        )

    async def initialize(
        self,
        cwd: Path,
        runtime: RuntimeInfo,
        auth: AuthSelection,
    ) -> tuple[str, ...]:
        del cwd, runtime
        self.auth.append(auth)
        return ("modelPolicy", "steering")

    async def local_login_status(self, runtime: RuntimeInfo) -> bool:
        del runtime
        return False


@pytest.mark.parametrize(
    ("environment", "user_toml", "expected_source", "expected_env"),
    [
        (
            {
                "QODER_PERSONAL_ACCESS_TOKEN": "pat-secret",
                "QODER_SERVICE_ACCOUNT_KEY": "service-secret",
            },
            """
[auth]
service_account_env = "QODER_SERVICE_ACCOUNT_KEY"
reuse_qodercli = true
""",
            "personal_access_token",
            "QODER_PERSONAL_ACCESS_TOKEN",
        ),
        (
            {"CUSTOM_SERVICE_ACCOUNT": "service-secret"},
            """
[auth]
service_account_env = "CUSTOM_SERVICE_ACCOUNT"
reuse_qodercli = true
""",
            "service_account",
            "CUSTOM_SERVICE_ACCOUNT",
        ),
        (
            {},
            """
[auth]
reuse_qodercli = true
""",
            "qodercli",
            None,
        ),
    ],
)
async def test_authentication_selection_has_fixed_priority(
    tmp_path: Path,
    environment: Mapping[str, str],
    user_toml: str,
    expected_source: str,
    expected_env: str | None,
) -> None:
    user_config = tmp_path / "user.toml"
    user_config.write_text(user_toml, encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    backend = FakePreflightBackend()

    result = await RuntimePreflight(
        backend,
        environ=environment,
        user_path=user_config,
    ).run(project)

    assert result.ok is True
    assert result.auth_source == expected_source
    assert backend.auth == [AuthSelection(expected_source, expected_env)]
    assert result.capabilities == ("modelPolicy",)


@pytest.mark.parametrize(
    ("user_toml", "expected_path", "expected_version"),
    [
        ("", "bundled", "1.1.23"),
        (
            (
                '[runtime]\npath = "/opt/qoder/qodercli"\n'
                '[auth]\nreuse_qodercli = true\n'
            ),
            "/opt/qoder/qodercli",
            "1.2.7",
        ),
    ],
)
async def test_runtime_selection_records_bundled_or_explicit_version(
    tmp_path: Path,
    user_toml: str,
    expected_path: str,
    expected_version: str,
) -> None:
    user_config = tmp_path / "user.toml"
    config_text = user_toml or "[auth]\nreuse_qodercli = true\n"
    user_config.write_text(config_text, encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    backend = FakePreflightBackend()

    result = await RuntimePreflight(
        backend,
        environ={},
        user_path=user_config,
    ).run(project)

    assert result.sdk_version == "1.0.13"
    assert result.runtime_path == expected_path
    assert result.runtime_version == expected_version


class FakeControlClient:
    def __init__(
        self,
        options: QoderAgentOptions,
        *,
        server_info: Mapping[str, object] | None = None,
        connect_error: Exception | None = None,
    ) -> None:
        self.options = options
        self.server_info = server_info
        self.connect_error = connect_error
        self.calls: list[str] = []

    async def connect(self, prompt: None = None) -> None:
        assert prompt is None
        self.calls.append("connect")
        if self.connect_error is not None:
            raise self.connect_error

    async def get_server_info(self) -> Mapping[str, object] | None:
        self.calls.append("server_info")
        return self.server_info

    async def disconnect(self) -> None:
        self.calls.append("disconnect")


async def test_sdk_backend_uses_public_control_connection_and_safe_capabilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "pat-do-not-report"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", secret)
    clients: list[FakeControlClient] = []

    def client_factory(options: QoderAgentOptions) -> object:
        client = FakeControlClient(
            options,
            server_info={
                "capabilities": {
                    "modelPolicy": "pull",
                    secret: "must-not-be-reported",
                },
                "account": {"access_token": secret},
            },
        )
        clients.append(client)
        return client

    async def command_runner(command: tuple[str, ...]) -> CommandResult:
        assert command[-1] == "-v"
        return CommandResult(returncode=0, stdout="1.2.7\n")

    runtime_file = tmp_path / "qodercli"
    runtime_file.write_text("runtime", encoding="utf-8")
    runtime_file.chmod(0o700)
    backend = QoderPreflightBackend(
        client_factory=client_factory,
        command_runner=command_runner,
    )
    runtime = await backend.resolve_runtime(runtime_file)

    capabilities = await backend.initialize(
        tmp_path,
        runtime,
        AuthSelection("personal_access_token", "QODER_PERSONAL_ACCESS_TOKEN"),
    )

    assert backend.sdk_version() == "1.0.13"
    assert runtime.recorded_path == str(runtime_file.resolve())
    assert runtime.version == "1.2.7"
    assert capabilities == ("modelPolicy",)
    assert clients[0].calls == ["connect", "server_info", "disconnect"]
    assert clients[0].options.cli_path == runtime_file.resolve()
    assert isinstance(clients[0].options.auth, AccessTokenAuthOptions)
    assert secret not in repr(capabilities)


async def test_local_login_initialize_timeout_is_redacted_and_actionable(
    tmp_path: Path,
) -> None:
    secret = "service-secret-do-not-report"
    commands: list[tuple[str, ...]] = []
    clients: list[FakeControlClient] = []

    def client_factory(options: QoderAgentOptions) -> object:
        client = FakeControlClient(
            options,
            connect_error=RuntimeError(
                f"Control request timeout: initialize {secret}"
            ),
        )
        clients.append(client)
        return client

    async def command_runner(command: tuple[str, ...]) -> CommandResult:
        commands.append(command)
        if command[-1] == "-v":
            return CommandResult(returncode=0, stdout="1.1.23")
        assert command[-1] == "status"
        return CommandResult(returncode=0, stdout=f"logged in {secret}")

    user_config = tmp_path / "user.toml"
    user_config.write_text(
        "[auth]\nreuse_qodercli = true\n",
        encoding="utf-8",
    )
    backend = QoderPreflightBackend(
        client_factory=client_factory,
        command_runner=command_runner,
    )
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "QODER_PERSONAL_ACCESS_TOKEN",
            "QODER_SERVICE_ACCOUNT_KEY",
            "QODERCN_PERSONAL_ACCESS_TOKEN",
            "QODERCN_SERVICE_ACCOUNT_KEY",
            "QODERCLI_PATH",
        }
    }

    result = await RuntimePreflight(
        backend,
        environ=environment,
        user_path=user_config,
    ).run(tmp_path)

    assert result.ok is False
    assert result.error is not None
    assert result.error.code == "initialize_timeout"
    assert result.warnings == ("qodercli_auth_reuse_failed",)
    assert commands[-1][-1] == "status"
    assert clients[0].calls == ["connect", "disconnect"]
    assert secret not in json.dumps(result.to_json())


async def test_local_login_capabilities_are_allowlisted_without_credentials(
    tmp_path: Path,
) -> None:
    local_credential = "fake-local-login-credential"
    clients: list[FakeControlClient] = []

    def client_factory(options: QoderAgentOptions) -> object:
        client = FakeControlClient(
            options,
            server_info={
                "capabilities": {
                    "modelPolicy": "pull",
                    local_credential: "runtime-private",
                    "futureArbitraryField": local_credential,
                }
            },
        )
        clients.append(client)
        return client

    async def command_runner(command: tuple[str, ...]) -> CommandResult:
        assert command[-1] == "-v"
        return CommandResult(returncode=0, stdout="1.1.23")

    user_config = tmp_path / "user.toml"
    user_config.write_text(
        "[auth]\nreuse_qodercli = true\n",
        encoding="utf-8",
    )
    result = await RuntimePreflight(
        QoderPreflightBackend(
            client_factory=client_factory,
            command_runner=command_runner,
        ),
        environ={},
        user_path=user_config,
    ).run(tmp_path)

    assert result.ok is True
    assert result.capabilities == ("modelPolicy",)
    assert clients[0].calls == ["connect", "server_info", "disconnect"]
    assert local_credential not in json.dumps(result.to_json())


async def test_doctor_cli_renders_injected_preflight_result() -> None:
    stdout = StringIO()
    expected = DoctorResult(
        ok=True,
        sdk_version="1.0.13",
        runtime_path="bundled",
        runtime_version="1.1.23",
        auth_source="personal_access_token",
        capabilities=("modelPolicy",),
    )
    inspected: list[Path] = []

    async def doctor_runner(cwd: Path) -> DoctorResult:
        inspected.append(cwd)
        return expected

    exit_code = await run(
        ["doctor", "--json"],
        stdin=StringIO(),
        stdout=stdout,
        doctor_runner=doctor_runner,
    )

    assert exit_code == 0
    assert json.loads(stdout.getvalue()) == expected.to_json()
    assert inspected == [Path.cwd()]


async def test_doctor_cli_returns_failure_and_actionable_warning() -> None:
    stdout = StringIO()
    expected = DoctorResult(
        ok=False,
        sdk_version="1.0.13",
        runtime_path="bundled",
        runtime_version="1.1.23",
        auth_source="qodercli",
        warnings=("qodercli_auth_reuse_failed",),
        error=PreflightDiagnostic(
            "initialize_timeout",
            "Qoder control initialization timed out.",
        ),
    )

    async def doctor_runner(cwd: Path) -> DoctorResult:
        del cwd
        return expected

    exit_code = await run(
        ["doctor", "--json"],
        stdin=StringIO(),
        stdout=stdout,
        doctor_runner=doctor_runner,
    )

    assert exit_code == 1
    assert json.loads(stdout.getvalue()) == expected.to_json()


async def test_spawn_preflight_records_runtime_metadata(tmp_path: Path) -> None:
    store = WorkerStore(tmp_path / "state")
    transport = FakeQoderTransport.successful_audit(model="Qwen3.8-Max")
    inspected: list[Path] = []

    async def successful_preflight(cwd: Path) -> DoctorResult:
        inspected.append(cwd)
        return DoctorResult(
            ok=True,
            sdk_version="1.0.13",
            runtime_path="/opt/qoder/qodercli",
            runtime_version="1.2.7",
            auth_source="service_account",
            capabilities=("modelPolicy",),
        )

    supervisor = Supervisor(
        store,
        lambda _: transport,
        sdk_version="stale-version",
        preflight=successful_preflight,
    )

    accepted = await supervisor.spawn(
        AuditContract(objective="preflight before audit", cwd=tmp_path)
    )
    worker_id = accepted["worker_id"]
    assert isinstance(worker_id, str)
    worker = await store.get_worker(worker_id)

    assert worker is not None
    assert worker.sdk_version == "1.0.13"
    assert worker.runtime_path == "/opt/qoder/qodercli"
    assert worker.runtime_version == "1.2.7"
    assert inspected == [tmp_path]
    await supervisor.close()


async def test_spawn_rejects_failed_preflight_before_persistence(
    tmp_path: Path,
) -> None:
    store = WorkerStore(tmp_path / "state")

    async def failed_preflight(cwd: Path) -> DoctorResult:
        del cwd
        return DoctorResult(
            ok=False,
            sdk_version="1.0.13",
            runtime_path="bundled",
            runtime_version="1.1.23",
            auth_source="qodercli",
            warnings=("qodercli_auth_reuse_failed",),
            error=PreflightDiagnostic(
                "initialize_timeout",
                "Control request timeout: initialize fake-local-login-credential",
            ),
        )

    supervisor = Supervisor(
        store,
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        preflight=failed_preflight,
    )

    with pytest.raises(SupervisorError) as caught:
        await supervisor.spawn(
            AuditContract(objective="must not persist", cwd=tmp_path)
        )

    assert caught.value.code == "initialize_timeout"
    assert caught.value.message == "Qoder control initialization timed out."
    assert caught.value.warnings == ("qodercli_auth_reuse_failed",)
    assert store.database_path.exists() is False
    await supervisor.close()


async def test_spawn_rpc_preserves_preflight_warning(tmp_path: Path) -> None:
    async def failed_preflight(cwd: Path) -> DoctorResult:
        del cwd
        return DoctorResult(
            ok=False,
            sdk_version="1.0.13",
            runtime_path="bundled",
            runtime_version="1.1.23",
            auth_source="qodercli",
            warnings=("qodercli_auth_reuse_failed",),
            error=PreflightDiagnostic(
                "initialize_timeout",
                "Qoder control initialization timed out.",
            ),
        )

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        preflight=failed_preflight,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    with pytest.raises(RPCClientError) as caught:
        await call(
            socket_path,
            "spawn",
            {
                "role": "auditor",
                "cwd": str(tmp_path),
                "objective": "warning must reach RPC client",
            },
        )

    assert caught.value.code == "initialize_timeout"
    assert caught.value.warnings == ("qodercli_auth_reuse_failed",)
    await server.close()
    await supervisor.close()


async def test_spawn_cli_renders_preflight_warning(tmp_path: Path) -> None:
    async def failed_preflight(cwd: Path) -> DoctorResult:
        del cwd
        return DoctorResult(
            ok=False,
            sdk_version="1.0.13",
            runtime_path="bundled",
            runtime_version="1.1.23",
            auth_source="qodercli",
            warnings=("qodercli_auth_reuse_failed",),
            error=PreflightDiagnostic(
                "initialize_timeout",
                "Qoder control initialization timed out.",
            ),
        )

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        preflight=failed_preflight,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    stdout = StringIO()

    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "spawn",
            "--role",
            "auditor",
            "--cwd",
            str(tmp_path),
            "--no-start-supervisor",
            "--json",
        ],
        stdin=StringIO("warning must reach CLI"),
        stdout=stdout,
    )

    assert exit_code == 1
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {
            "code": "initialize_timeout",
            "message": "Qoder control initialization timed out.",
            "warnings": ["qodercli_auth_reuse_failed"],
        },
    }
    await server.close()
    await supervisor.close()


def test_worker_transport_options_reuse_preflight_config_and_auth_order(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "external-qodercli"
    runtime.write_text("runtime", encoding="utf-8")
    runtime.chmod(0o700)
    user_config = tmp_path / "user.toml"
    user_config.write_text(
        f"""
[runtime]
path = "{runtime}"
[auth]
service_account_env = "CUSTOM_QODER_SERVICE_KEY"
reuse_qodercli = true
""".strip(),
        encoding="utf-8",
    )

    options = build_configured_auditor_options(
        tmp_path,
        user_path=user_config,
        environ={"CUSTOM_QODER_SERVICE_KEY": "service-secret"},
    )

    assert options.cli_path == runtime.resolve()
    assert isinstance(options.auth, ServiceAccountAuthOptions)
