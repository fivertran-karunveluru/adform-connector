#!/usr/bin/env python3
"""
Unit tests for Adform API Connector (schema, config parsing, validation).
"""

import sys

from connector import (
    API_BASE_URL,
    SyncType,
    AdformConfig,
    parse_configuration,
    validate_configuration,
    get_time_range,
    schema,
)


def test_parse_and_validate_config() -> bool:
    raw = {
        "client_id": "abc",
        "client_secret": "xyz",
        "refresh_token": "r123",
        "adform_base_url": API_BASE_URL,
        "initial_sync_days": "30",
        "page_size": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_campaigns_sync": "true",
        "enable_lineitems_sync": "true",
        "enable_creatives_sync": "true",
        "enable_debug_logging": "false",
        "use_mock": "true",
    }

    cfg = parse_configuration(raw)
    assert isinstance(cfg, AdformConfig)
    validate_configuration(cfg)
    return True


def test_time_range() -> bool:
    cfg = parse_configuration({"initial_sync_days": "7"})
    r = get_time_range(SyncType.INITIAL, cfg)
    assert "start" in r and "end" in r
    return True


def test_schema() -> bool:
    s = schema({})
    assert isinstance(s, list) and len(s) > 0
    tables = [t["table"] for t in s]
    for expected in ["campaigns", "line_items", "creatives", "sync_metadata"]:
        assert expected in tables
    return True


def main() -> int:
    tests = [
        ("parse_and_validate_config", test_parse_and_validate_config()),
        ("time_range", test_time_range()),
        ("schema", test_schema()),
    ]
    passed = sum(1 for _, ok in tests if ok)
    total = len(tests)
    print(f"Adform Connector Tests: {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
