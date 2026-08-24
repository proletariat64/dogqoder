"""Closed qworker user and project configuration."""

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, cast

type PermissionMode = Literal["dontAsk", "default", "acceptEdits"]

_MAX_CONFIG_BYTES = 1024 * 1024
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_PERMISSION_RANK: dict[PermissionMode, int] = {
    "dontAsk": 0,
    "default": 1,
    "acceptEdits": 2,
}
_USER_TABLES = frozenset(("project", "runtime", "auth", "policy"))
_POLICY_FIELDS = frozenset(
    (
        "proactive_auditor",
        "proactive_coder",
        "coder_permission_mode",
        "coder_denied_tools",
        "auditor_web_access",
    )
)


class ConfigError(ValueError):
    """Stable, display-safe configuration failure."""

    def __init__(self, message: str) -> None:
        self.code = "invalid_request"
        self.message = message
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class PolicyConfig:
    """Proactive and permission policy that a project may narrow."""

    proactive_auditor: bool = True
    proactive_coder: bool = False
    coder_permission_mode: PermissionMode = "acceptEdits"
    coder_denied_tools: tuple[str, ...] = ()
    auditor_web_access: bool = True


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    """Validated user configuration plus one project's policy overlay."""

    runtime_path: Path | None = None
    service_account_env: str | None = None
    reuse_qodercli_auth: bool = False
    allow_project_expansion: bool = False
    policy: PolicyConfig = PolicyConfig()


def default_user_config_path() -> Path:
    """Return the normative per-user configuration path."""

    return Path.home() / ".config" / "qworker" / "config.toml"


def load_config(
    cwd: Path,
    *,
    user_path: Path | None = None,
) -> EffectiveConfig:
    """Load user settings and apply the project's policy-only overlay."""

    selected_user_path = default_user_config_path() if user_path is None else user_path
    user_data = _read_toml(selected_user_path, label="User")
    config = _parse_user_config(user_data)
    project_data = _read_toml(cwd / ".qworker.toml", label="Project")
    if not project_data:
        return config
    if set(project_data) - {"policy"}:
        raise ConfigError("Project configuration may only contain [policy].")
    project_policy = _table(project_data, "policy", label="Project")
    if set(project_policy) - _POLICY_FIELDS:
        raise ConfigError("Project [policy] contains an unknown field.")
    return replace(config, policy=_apply_project_policy(config, project_policy))


def _read_toml(path: Path, *, label: str) -> dict[str, object]:
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return {}
    except OSError:
        raise ConfigError(f"Unable to read {label.lower()} configuration.") from None
    if size > _MAX_CONFIG_BYTES:
        raise ConfigError(f"{label} configuration exceeds the size limit.")
    try:
        with path.open("rb") as stream:
            decoded = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigError(f"{label} configuration is invalid TOML.") from None
    return cast(dict[str, object], decoded)


def _parse_user_config(data: Mapping[str, object]) -> EffectiveConfig:
    if set(data) - _USER_TABLES:
        raise ConfigError("User configuration contains an unknown table.")
    project = _table(data, "project", label="User")
    runtime = _table(data, "runtime", label="User")
    auth = _table(data, "auth", label="User")
    policy = _table(data, "policy", label="User")
    if set(project) - {"allow_expansion"}:
        raise ConfigError("User [project] contains an unknown field.")
    if set(runtime) - {"path"}:
        raise ConfigError("User [runtime] contains an unknown field.")
    if set(auth) - {"service_account_env", "reuse_qodercli"}:
        raise ConfigError("User [auth] contains an unknown field.")
    if set(policy) - _POLICY_FIELDS:
        raise ConfigError("User [policy] contains an unknown field.")

    runtime_path: Path | None = None
    if "path" in runtime:
        raw_runtime_path = _string(runtime["path"], "runtime.path")
        runtime_path = Path(raw_runtime_path).expanduser()
        if not runtime_path.is_absolute():
            raise ConfigError("runtime.path must be absolute.")

    service_account_env: str | None = None
    if "service_account_env" in auth:
        service_account_env = _string(
            auth["service_account_env"], "auth.service_account_env"
        )
        if _ENVIRONMENT_NAME.fullmatch(service_account_env) is None:
            raise ConfigError("auth.service_account_env must be an environment name.")

    return EffectiveConfig(
        runtime_path=runtime_path,
        service_account_env=service_account_env,
        reuse_qodercli_auth=_optional_boolean(
            auth, "reuse_qodercli", default=False, qualified="auth.reuse_qodercli"
        ),
        allow_project_expansion=_optional_boolean(
            project,
            "allow_expansion",
            default=False,
            qualified="project.allow_expansion",
        ),
        policy=_apply_policy(PolicyConfig(), policy),
    )


def _apply_project_policy(
    config: EffectiveConfig,
    values: Mapping[str, object],
) -> PolicyConfig:
    current = config.policy
    candidate = _apply_policy(current, values)
    if config.allow_project_expansion:
        return candidate
    changed = _expanded_policy_field(current, candidate)
    if changed is not None:
        raise ConfigError(
            f"Project configuration expands policy field '{changed}'; "
            "user configuration must set project.allow_expansion = true."
        )
    return candidate


def _apply_policy(
    base: PolicyConfig,
    values: Mapping[str, object],
) -> PolicyConfig:
    permission_mode = base.coder_permission_mode
    if "coder_permission_mode" in values:
        raw_mode = _string(
            values["coder_permission_mode"], "policy.coder_permission_mode"
        )
        if raw_mode not in _PERMISSION_RANK:
            raise ConfigError(
                "policy.coder_permission_mode must be dontAsk, default, or acceptEdits."
            )
        permission_mode = raw_mode

    denied_tools = base.coder_denied_tools
    if "coder_denied_tools" in values:
        denied_tools = _tool_names(values["coder_denied_tools"])

    return PolicyConfig(
        proactive_auditor=_optional_boolean(
            values,
            "proactive_auditor",
            default=base.proactive_auditor,
            qualified="policy.proactive_auditor",
        ),
        proactive_coder=_optional_boolean(
            values,
            "proactive_coder",
            default=base.proactive_coder,
            qualified="policy.proactive_coder",
        ),
        coder_permission_mode=permission_mode,
        coder_denied_tools=denied_tools,
        auditor_web_access=_optional_boolean(
            values,
            "auditor_web_access",
            default=base.auditor_web_access,
            qualified="policy.auditor_web_access",
        ),
    )


def _expanded_policy_field(
    current: PolicyConfig,
    candidate: PolicyConfig,
) -> str | None:
    for field in ("proactive_auditor", "proactive_coder", "auditor_web_access"):
        if not getattr(current, field) and getattr(candidate, field):
            return field
    if (
        _PERMISSION_RANK[candidate.coder_permission_mode]
        > _PERMISSION_RANK[current.coder_permission_mode]
    ):
        return "coder_permission_mode"
    if not set(current.coder_denied_tools).issubset(candidate.coder_denied_tools):
        return "coder_denied_tools"
    return None


def _table(
    data: Mapping[str, object],
    name: str,
    *,
    label: str,
) -> Mapping[str, object]:
    value = data.get(name, {})
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise ConfigError(f"{label} [{name}] must be a table.")
    return cast(dict[str, object], value)


def _optional_boolean(
    values: Mapping[str, object],
    field: str,
    *,
    default: bool,
    qualified: str,
) -> bool:
    if field not in values:
        return default
    value = values[field]
    if not isinstance(value, bool):
        raise ConfigError(f"{qualified} must be a boolean.")
    return value


def _string(value: object, qualified: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{qualified} must be a non-empty string.")
    return value


def _tool_names(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item and len(item) <= 128 for item in value
    ):
        raise ConfigError(
            "policy.coder_denied_tools must be an array of non-empty tool names."
        )
    return tuple(sorted(set(cast(list[str], value))))
