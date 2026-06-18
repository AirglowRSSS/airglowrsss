from unittest.mock import MagicMock
import os
os.environ['GITHUB_TOKEN'] = 'token'
os.environ['GITHUB_REPO'] = 'my/repo'

from airglow.dagster_airglow.sensors import (cloud_cover_files_for_site,
                                             group_files_by_date,
                                             instrument_upload_sensor)
import dagster as dg


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

    context = dg.build_sensor_context(
        resources={"s3": mock_s3}
    )
    runs = list(instrument_upload_sensor(context))

    assert len(runs) == 1
    run09 = runs[0]
    assert run09.run_key == 'sort-20250409-fpi12'
    assert run09.run_config["ops"]['unzip_chunked_archive']["config"] == {
        'observation_date': '20250409', 'site': 'ABC',
        'file_chunks': ['raw/fpi12_ABC_20250409.tar.gz000000',
                        'raw/fpi12_ABC_20250409.tar.gz000001',
                        'raw/fpi12_ABC_20250409.tar.gz000002'],
        'cloud_files': ['raw/Cloud_ABC_20250409.txt', 'raw/Cloud_ABC_20250410.txt'],
        'instrument_name': 'minime12',
        'instrument_log_file': 'raw/fpi12_ABC_20250409.txt'
    }


def test_instrument_upload_sensor_two_instruments_same_site():
    """Two instruments (fpi05, fpi13) at the same site must produce two
    separate RunRequests, each scoped to its own chunks only."""
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

    context = dg.build_sensor_context(resources={"s3": mock_s3})
    runs = list(instrument_upload_sensor(context))

    assert len(runs) == 2, f"Expected 2 RunRequests, got {len(runs)}"

    runs_by_key = {r.run_key: r for r in runs}
    assert 'sort-20260407-fpi05' in runs_by_key, f"Missing fpi05 run, keys: {list(runs_by_key)}"
    assert 'sort-20260407-fpi13' in runs_by_key, f"Missing fpi13 run, keys: {list(runs_by_key)}"

    fpi05_config = runs_by_key['sort-20260407-fpi05'].run_config['ops']['unzip_chunked_archive']['config']
    assert fpi05_config['instrument_name'] == 'minime05'
    assert fpi05_config['site'] == 'uao'
    assert fpi05_config['file_chunks'] == [
        'raw/fpi05_uao_20260407.tar.gz000000',
        'raw/fpi05_uao_20260407.tar.gz000001',
    ], f"fpi05 got wrong chunks: {fpi05_config['file_chunks']}"

    fpi13_config = runs_by_key['sort-20260407-fpi13'].run_config['ops']['unzip_chunked_archive']['config']
    assert fpi13_config['instrument_name'] == 'minime13'
    assert fpi13_config['site'] == 'uao'
    assert fpi13_config['file_chunks'] == [
        'raw/fpi13_uao_20260407.tar.gz000000',
        'raw/fpi13_uao_20260407.tar.gz000001',
    ], f"fpi13 got wrong chunks: {fpi13_config['file_chunks']}"
