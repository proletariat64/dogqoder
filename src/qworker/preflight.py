"""SDK-independent Qoder runtime and authentication preflight."""

import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from qworker.config import ConfigError, EffectiveConfig, load_config
from qworker.store import JsonValue

type AuthSource = Literal[
    "personal_access_token",
    "service_account",
    "qodercli",
]

_PERSONAL_ACCESS_TOKEN_ENV = "QODER_PERSONAL_ACCESS_TOKEN"
_KNOWN_CREDENTIAL_ENV_VARS = (
    _PERSONAL_ACCESS_TOKEN_ENV,
    "QODER_SERVICE_ACCOUNT_KEY",
    "QODERCN_PERSONAL_ACCESS_TOKEN",
    "QODERCN_SERVICE_ACCOUNT_KEY",
)
_MAX_DIAGNOSTIC_CHARS = 512


@dataclass(frozen=True, slots=True)
class AuthSelection:
    """Credential-free description of the selected SDK auth helper."""

    source: AuthSource
    env_var: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeInfo:
    """Resolved runtime metadata and internal executable path."""

    recorded_path: str
    executable: Path
    version: str


@dataclass(frozen=True, slots=True)
class PreflightDiagnostic:
    """Stable, display-safe preflight failure."""

    code: str
    message: str


@dataclass(frozen=True, slots=True)
class DoctorResult:
    """Finite, credential-free result shared by doctor and spawn."""

    ok: bool
    sdk_version: str | None = None
    runtime_path: str | None = None
    runtime_version: str | None = None
    auth_source: AuthSource | None = None
    capabilities: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    error: PreflightDiagnostic | None = None

    def to_json(self) -> dict[str, JsonValue]:
        """Render the stable JSON CLI shape without internal paths or auth data."""

        result: dict[str, JsonValue] = {
            "ok": self.ok,
            "sdk_version": self.sdk_version,
            "runtime": (
                {
                    "path": self.runtime_path,
                    "version": self.runtime_version,
                }
                if self.runtime_path is not None
                else None
            ),
            "auth": (
                {"source": self.auth_source}
                if self.auth_source is not None
                else None
            ),
            "capabilities": list(self.capabilities),
            "warnings": list(self.warnings),
        }
        if self.error is not None:
            result["error"] = {
                "code": self.error.code,
                "message": self.error.message,
            }
        return result


class PreflightFailure(RuntimeError):
    """Backend failure with a stable code, redacted by the orchestrator."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


class PreflightBackend(Protocol):
    """Injected system boundary used by deterministic preflight tests."""

    def sdk_version(self) -> str: ...

    async def resolve_runtime(self, explicit_path: Path | None) -> RuntimeInfo: ...

    async def initialize(
        self,
        cwd: Path,
        runtime: RuntimeInfo,
        auth: AuthSelection,
    ) -> tuple[str, ...]: ...

    async def local_login_status(self, runtime: RuntimeInfo) -> bool: ...


type PreflightRunner = Callable[[Path], Awaitable[DoctorResult]]


class RuntimePreflight:
    """Load effective config and prove one SDK control connection works."""

    def __init__(
        self,
        backend: PreflightBackend,
        *,
        environ: Mapping[str, str] | None = None,
        user_path: Path | None = None,
    ) -> None:
        self._backend = backend
        self._environ = os.environ if environ is None else environ
        self._user_path = user_path

    async def run(self, cwd: Path) -> DoctorResult:
        """Run ordered SDK, runtime, auth, and control capability checks."""

        try:
            sdk_version = self._backend.sdk_version()
        except Exception:  # noqa: BLE001 -- one safe adapter boundary
            return _failed("sdk_protocol_error", "Unable to import Qoder SDK.")

        try:
            config = load_config(cwd, user_path=self._user_path)
        except ConfigError as error:
            return _failed(error.code, error.message, sdk_version=sdk_version)

        credential_values = _credential_values(config, self._environ)
        try:
            runtime = await self._backend.resolve_runtime(config.runtime_path)
        except PreflightFailure as error:
            return _failed(
                error.code,
                _safe_message(error.message, credential_values),
                sdk_version=sdk_version,
            )
        except Exception:  # noqa: BLE001 -- backend details are not display-safe
            return _failed(
                "sdk_protocol_error",
                "Unable to resolve Qoder runtime.",
                sdk_version=sdk_version,
            )

        try:
            auth = select_auth(config, self._environ)
        except PreflightFailure as error:
            return _failed(
                error.code,
                _safe_message(error.message, credential_values),
                sdk_version=sdk_version,
                runtime=runtime,
            )

        try:
            capabilities = await self._backend.initialize(cwd, runtime, auth)
        except PreflightFailure as error:
            warnings: tuple[str, ...] = ()
            if auth.source == "qodercli" and error.code == "initialize_timeout":
                try:
                    status_ok = await self._backend.local_login_status(runtime)
                except Exception:  # noqa: BLE001 -- status output is never exposed
                    status_ok = False
                if status_ok:
                    warnings = ("qodercli_auth_reuse_failed",)
            return _failed(
                error.code,
                _safe_message(error.message, credential_values),
                sdk_version=sdk_version,
                runtime=runtime,
                auth=auth,
                warnings=warnings,
            )
        except Exception:  # noqa: BLE001 -- backend details are not display-safe
            return _failed(
                "sdk_protocol_error",
                "Qoder control initialization failed.",
                sdk_version=sdk_version,
                runtime=runtime,
                auth=auth,
            )

        return DoctorResult(
            ok=True,
            sdk_version=sdk_version,
            runtime_path=runtime.recorded_path,
            runtime_version=runtime.version,
            auth_source=auth.source,
            capabilities=tuple(sorted(set(capabilities))),
        )


def select_auth(
    config: EffectiveConfig,
    environ: Mapping[str, str],
) -> AuthSelection:
    """Apply fixed PAT, configured service-account, then local-login order."""

    if environ.get(_PERSONAL_ACCESS_TOKEN_ENV):
        return AuthSelection("personal_access_token", _PERSONAL_ACCESS_TOKEN_ENV)
    if config.service_account_env is not None:
        return AuthSelection("service_account", config.service_account_env)
    if config.reuse_qodercli_auth:
        return AuthSelection("qodercli")
    raise PreflightFailure(
        "auth_required",
        "Set QODER_PERSONAL_ACCESS_TOKEN, configure a service-account environment "
        "variable, or enable qodercli auth reuse.",
    )


def _failed(
    code: str,
    message: str,
    *,
    sdk_version: str | None = None,
    runtime: RuntimeInfo | None = None,
    auth: AuthSelection | None = None,
    warnings: tuple[str, ...] = (),
) -> DoctorResult:
    return DoctorResult(
        ok=False,
        sdk_version=sdk_version,
        runtime_path=runtime.recorded_path if runtime is not None else None,
        runtime_version=runtime.version if runtime is not None else None,
        auth_source=auth.source if auth is not None else None,
        warnings=warnings,
        error=PreflightDiagnostic(code, _bounded(message)),
    )


def _credential_values(
    config: EffectiveConfig,
    environ: Mapping[str, str],
) -> tuple[str, ...]:
    names = set(_KNOWN_CREDENTIAL_ENV_VARS)
    if config.service_account_env is not None:
        names.add(config.service_account_env)
    return tuple(value for name in names if (value := environ.get(name)))


def _safe_message(message: str, credential_values: tuple[str, ...]) -> str:
    safe = message
    for value in credential_values:
        safe = safe.replace(value, "[REDACTED]")
    return _bounded(safe)


def _bounded(message: str) -> str:
    if len(message) <= _MAX_DIAGNOSTIC_CHARS:
        return message
    return message[: _MAX_DIAGNOSTIC_CHARS - 3] + "..."
