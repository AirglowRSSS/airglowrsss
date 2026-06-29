from unittest.mock import MagicMock
import os
os.environ['GITHUB_TOKEN'] = 'token'
os.environ['GITHUB_REPO'] = 'my/repo'

from airglow.dagster_airglow.sensors import (cloud_cover_files_for_site,
                                             group_files_by_date,
                                             instrument_upload_sensor)
import dagster as dg


def _make_log_response(num_parts: int, total_size: int = 12345678) -> dict:
    """Returns a mock S3 get_object response with num_parts on line 2."""
    content = "\n".join([
        "fpiXX_site_20260101.tar.gz000000",
        str(num_parts),
        str(total_size),
        "2026-01-01T00:00:00",
        "100.0",
        "first chunk filename",
        "number of parts",
        "size of tar.gz bytes",
        "creation timestamp",
        "GB disk free",
    ])
    body = MagicMock()
    body.read.return_value = content.encode('utf-8')
    return {"Body": body}


def test_group_files_by_date():
    # Test data
    files = [
        "raw/Cloud_ABC_20230101.txt",
        "raw/Cloud_DEF_20230101.txt",
        "raw/Cloud_XYZ_20230101.txt",
        "raw/Cloud_ABC_20230201.txt",
        "raw/Cloud_DEF_20230201.txt",
        "raw/Cloud_XYZ_20230201.txt",
        "raw/fpi05_ABC_20250409.tar.gz000006"
    ]

    # Expected output
    expected_output = {
        20230101: [
            "raw/Cloud_ABC_20230101.txt",
            "raw/Cloud_DEF_20230101.txt",
            "raw/Cloud_XYZ_20230101.txt"
        ],
        20230201: [
            "raw/Cloud_ABC_20230201.txt",
            "raw/Cloud_DEF_20230201.txt",
            "raw/Cloud_XYZ_20230201.txt"
        ],
        20250409: [
            "raw/fpi05_ABC_20250409.tar.gz000006"
        ]
    }

    # Call the function
    result = group_files_by_date(files)

    # Assert the result
    assert result == expected_output


def test_cloud_cover_files_for_site():
    # Test data
    files = [
        "raw/Cloud_ABC_20230101.txt",
        "raw/Cloud_DEF_20230101.txt",
        "raw/Cloud_XYZ_20230101.txt",
        "raw/Cloud_ABC_20230201.txt",
        "raw/Cloud_DEF_20230201.txt",
        "raw/Cloud_XYZ_20230201.txt",
        "raw/fpi05_ABC_20250409.tar.gz000006"
    ]
    site = "ABC"

    # Expected output
    expected_output = [
        "raw/Cloud_ABC_20230101.txt",
        "raw/Cloud_ABC_20230201.txt"
    ]

    # Call the function
    result = cloud_cover_files_for_site(site, files)

    # Assert the result
    assert result == expected_output


def test_instrument_upload_sensor():
    mock_s3 = MagicMock()
    paginator = MagicMock()
    files = ["raw/fpi12_ABC_20250409.tar.gz000000",
             "raw/fpi12_ABC_20250409.tar.gz000001",
             "raw/fpi12_ABC_20250409.tar.gz000002",
             "raw/fpi12_ABC_20250409.txt",
             "raw/Cloud_ABC_20250409.txt",
             "raw/Cloud_ABC_20250410.txt",
             # Incomplete upload
             "raw/fpi05_ABC_20250410.tar.gz000000",
             "raw/fpi05_ABC_20250410.tar.gz000002",
             # Just the log file
             "raw/fpi05_ABC_20250409.txt"
             ]
    paginator.paginate.return_value = [
        {"Contents": [{"Key": file} for file in files]}
    ]

    mock_s3.get_client.return_value.get_paginator.return_value = paginator
    # fpi12 log says 3 parts — matches the 3 chunks above
    mock_s3.get_client.return_value.get_object.return_value = _make_log_response(num_parts=3)

    context = dg.build_sensor_context(
        resources={"s3": mock_s3}
    )
    runs = list(instrument_upload_sensor(context))

    assert len(runs) == 1
    run09 = runs[0]
    assert run09.run_key == 'sort-20250409-fpi12'
    cfg = run09.run_config["ops"]['unzip_chunked_archive']["config"]
    assert cfg['observation_date'] == '20250409'
    assert cfg['site'] == 'ABC'
    assert cfg['instrument_name'] == 'minime12'
    # None fields are omitted by Dagster serialization — auto-discovered at run time
    assert cfg.get('file_chunks') is None
    assert cfg.get('cloud_files') is None
    assert cfg.get('instrument_log_file') is None


def test_instrument_upload_sensor_two_instruments_same_site():
    """Two instruments (fpi05, fpi13) at the same site must produce two
    separate RunRequests with correct identifiers. file_chunks and cloud_files
    are intentionally None — resolve_config discovers them at run time."""
    mock_s3 = MagicMock()
    paginator = MagicMock()
    files = [
        "raw/fpi05_uao_20260407.tar.gz000000",
        "raw/fpi05_uao_20260407.tar.gz000001",
        "raw/fpi13_uao_20260407.tar.gz000000",
        "raw/fpi13_uao_20260407.tar.gz000001",
        "raw/fpi05_uao_20260407.txt",
        "raw/fpi13_uao_20260407.txt",
        "raw/Cloud_uao_20260407.txt",
    ]
    paginator.paginate.return_value = [
        {"Contents": [{"Key": file} for file in files]}
    ]
    mock_s3.get_client.return_value.get_paginator.return_value = paginator
    # both fpi05 and fpi13 log files say 2 parts — matches the 2 chunks each above
    mock_s3.get_client.return_value.get_object.return_value = _make_log_response(num_parts=2)

    context = dg.build_sensor_context(resources={"s3": mock_s3})
    runs = list(instrument_upload_sensor(context))

    assert len(runs) == 2, f"Expected 2 RunRequests, got {len(runs)}"

    runs_by_key = {r.run_key: r for r in runs}
    assert 'sort-20260407-fpi05' in runs_by_key, f"Missing fpi05 run, keys: {list(runs_by_key)}"
    assert 'sort-20260407-fpi13' in runs_by_key, f"Missing fpi13 run, keys: {list(runs_by_key)}"

    fpi05_config = runs_by_key['sort-20260407-fpi05'].run_config['ops']['unzip_chunked_archive']['config']
    assert fpi05_config['instrument_name'] == 'minime05'
    assert fpi05_config['site'] == 'uao'
    assert fpi05_config['observation_date'] == '20260407'
    assert fpi05_config.get('file_chunks') is None   # discovered at run time
    assert fpi05_config.get('cloud_files') is None   # discovered at run time

    fpi13_config = runs_by_key['sort-20260407-fpi13'].run_config['ops']['unzip_chunked_archive']['config']
    assert fpi13_config['instrument_name'] == 'minime13'
    assert fpi13_config['site'] == 'uao'
    assert fpi13_config['observation_date'] == '20260407'
    assert fpi13_config.get('file_chunks') is None
    assert fpi13_config.get('cloud_files') is None


def test_sensor_does_not_trigger_when_log_only_no_chunks():
    """An instrument that uploads only its log file (no tar.gz) must not
    trigger a run — it is not a complete upload."""
    mock_s3 = MagicMock()
    paginator = MagicMock()
    files = [
        # fpi13 has chunks but no log yet
        "raw/fpi13_uao_20260628.tar.gz000000",
        "raw/fpi13_uao_20260628.tar.gz000001",
        # fpi05 has a log but no chunks
        "raw/fpi05_uao_20260628.txt",
        "raw/Cloud_uao_20260628.txt",
    ]
    paginator.paginate.return_value = [
        {"Contents": [{"Key": file} for file in files]}
    ]
    mock_s3.get_client.return_value.get_paginator.return_value = paginator

    context = dg.build_sensor_context(resources={"s3": mock_s3})
    runs = list(instrument_upload_sensor(context))

    assert len(runs) == 0, (
        f"Expected no runs (neither instrument is complete), got {len(runs)}: "
        f"{[r.run_key for r in runs]}"
    )


def test_sensor_does_not_trigger_when_parts_incomplete():
    """Log file says 5 parts are expected but only 3 are present — must not trigger."""
    mock_s3 = MagicMock()
    paginator = MagicMock()
    files = [
        "raw/fpi05_uao_20260628.tar.gz000000",
        "raw/fpi05_uao_20260628.tar.gz000001",
        "raw/fpi05_uao_20260628.tar.gz000002",
        "raw/fpi05_uao_20260628.txt",
        "raw/Cloud_uao_20260628.txt",
    ]
    paginator.paginate.return_value = [
        {"Contents": [{"Key": file} for file in files]}
    ]
    mock_s3.get_client.return_value.get_paginator.return_value = paginator
    # log says 5 parts expected, but only 3 chunks are present above
    mock_s3.get_client.return_value.get_object.return_value = _make_log_response(num_parts=5)

    context = dg.build_sensor_context(resources={"s3": mock_s3})
    runs = list(instrument_upload_sensor(context))

    assert len(runs) == 0, (
        f"Expected no runs (only 3/5 parts present), got {len(runs)}: "
        f"{[r.run_key for r in runs]}"
    )
