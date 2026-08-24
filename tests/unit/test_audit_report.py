from qworker.audit_report import AuditReportCapture


def _valid_report() -> dict[str, object]:
    return {
        "outcome": "completed",
        "summary": "safe",
        "files": ["README.md"],
        "validation": ["read-only inspection"],
        "risks": [],
        "verdict": "approved",
        "confirmed": ["workspace unchanged"],
        "findings": [
            {
                "severity": "low",
                "evidence": "README.md inspected",
                "affected_requirement_or_location": "README.md",
            }
        ],
        "required_changes": [],
    }


def test_capture_rejects_extra_report_fields() -> None:
    capture = AuditReportCapture()
    report = _valid_report()
    report["unexpected"] = "must fail closed"

    accepted = capture.record(report)

    assert accepted is False
    assert capture.report_text is None
