from unittest.mock import MagicMock, patch
import os
os.environ['GITHUB_TOKEN'] = 'token'
os.environ['GITHUB_REPO'] = 'my/repo'

import dagster as dg
import pytest

from airglow.dagster_airglow.assets.upload_chunked_archive import (
    _validate_file_chunks,
    resolve_config,
    ChunkedArchiveConfig,
)


def _mock_context():
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


# ---------------------------------------------------------------------------
# _validate_file_chunks
# ---------------------------------------------------------------------------

def test_validate_file_chunks_keeps_matching():
    ctx = _mock_context()
    chunks = [
        "raw/fpi05_uao_20260407.tar.gz000000",
        "raw/fpi05_uao_20260407.tar.gz000001",
    ]
    result = _validate_file_chunks(chunks, "fpi05", "uao", "20260407", ctx)
    assert result == chunks
    ctx.log.warning.assert_not_called()


def test_validate_file_chunks_drops_wrong_instrument():
    ctx = _mock_context()
    mixed = [
        "raw/fpi05_uao_20260407.tar.gz000000",
        "raw/fpi13_uao_20260407.tar.gz000000",  # wrong instrument
    ]
    result = _validate_file_chunks(mixed, "fpi05", "uao", "20260407", ctx)
    assert result == ["raw/fpi05_uao_20260407.tar.gz000000"]
    ctx.log.warning.assert_called_once()


def test_validate_file_chunks_raises_when_none_remain():
    ctx = _mock_context()
    chunks = ["raw/fpi13_uao_20260407.tar.gz000000"]
    with pytest.raises(Exception, match="No valid file chunks remain"):
        _validate_file_chunks(chunks, "fpi05", "uao", "20260407", ctx)


# ---------------------------------------------------------------------------
# resolve_config — file_chunks branch
# ---------------------------------------------------------------------------

def test_resolve_config_uses_provided_chunks():
    """When file_chunks is set, resolve_config must not call S3 and must
    return only the valid (instrument-matching) chunks."""
    config = ChunkedArchiveConfig(
        site="uao",
        observation_date="20260407",
        instrument_name="minime05",
        file_chunks=[
            "raw/fpi05_uao_20260407.tar.gz000000",
            "raw/fpi05_uao_20260407.tar.gz000001",
        ],
    )
    s3_client = MagicMock()
    ctx = _mock_context()

    file_chunks, cloud_files, log_file = resolve_config(config, s3_client, "my-bucket", ctx)

    s3_client.get_paginator.assert_not_called()
    assert file_chunks == [
        "raw/fpi05_uao_20260407.tar.gz000000",
        "raw/fpi05_uao_20260407.tar.gz000001",
    ]


def test_resolve_config_filters_mixed_chunks_from_sensor():
    """Even if the sensor (or a manual config) supplies both instruments'
    chunks, resolve_config must filter down to the correct instrument."""
    config = ChunkedArchiveConfig(
        site="uao",
        observation_date="20260407",
        instrument_name="minime05",
        file_chunks=[
            "raw/fpi05_uao_20260407.tar.gz000000",
            "raw/fpi13_uao_20260407.tar.gz000000",  # wrong instrument
        ],
    )
    s3_client = MagicMock()
    ctx = _mock_context()

    file_chunks, _, _ = resolve_config(config, s3_client, "my-bucket", ctx)

    assert file_chunks == ["raw/fpi05_uao_20260407.tar.gz000000"]
    ctx.log.warning.assert_called()  # should warn about the dropped chunk


def test_resolve_config_autodiscovers_when_chunks_none():
    """When file_chunks is None, S3 is queried with an instrument-scoped prefix."""
    config = ChunkedArchiveConfig(
        site="uao",
        observation_date="20260407",
        instrument_name="minime05",
        file_chunks=None,
    )
    s3_client = MagicMock()
    paginator = MagicMock()
    s3_client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "raw/fpi05_uao_20260407.tar.gz000000"},
            {"Key": "raw/fpi05_uao_20260407.tar.gz000001"},
        ]}
    ]
    ctx = _mock_context()

    file_chunks, _, _ = resolve_config(config, s3_client, "my-bucket", ctx)

    # Must have used the instrument-scoped prefix
    paginator.paginate.assert_called_once_with(
        Bucket="my-bucket",
        Prefix="raw/fpi05_uao_20260407.tar.gz",
    )
    assert file_chunks == [
        "raw/fpi05_uao_20260407.tar.gz000000",
        "raw/fpi05_uao_20260407.tar.gz000001",
    ]
