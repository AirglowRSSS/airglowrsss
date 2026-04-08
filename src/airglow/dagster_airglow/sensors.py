import re
from pathlib import Path

import dagster as dg
from botocore.exceptions import ClientError
from dagster import EnvVar, RunConfig
from dagster_ncsa import S3ResourceNCSA

from airglow.dagster_airglow.assets.upload_chunked_archive import ChunkedArchiveConfig


def list_files(bucket: str, prefix: str, s3_client) -> list[str]:
    if not prefix.endswith("/"):
        prefix += "/"

    try:
        # List all objects in the directory
        paginator = s3_client.get_paginator("list_objects_v2")
        objects = []

        # Use pagination to handle large directories
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects.append(obj["Key"])
        return objects

    except ClientError as e:
        raise e


def group_files_by_date(file_list):
    """
    Using a list of files from the S3 bucket, this function groups the files by date.
    """
    date_files_dict = {}

    for file in file_list:
        # Extract the date part from the filename
        # The date appears in format YYYYMMDD
        filename = Path(file).name

        # Look for the date pattern in the filename
        parts = filename.split('_')
        if len(parts) >= 3:
            # The date should be in the last part before .txt or .tar.gz
            date_part = int(parts[2][:8])  # Extract YYYYMMDD

            # Add to dictionary or append to existing entry if there
            if date_part in date_files_dict:
                date_files_dict[date_part].append(file)
            else:
                date_files_dict[date_part] = [file]

    return date_files_dict


def cloud_cover_files_for_site(site: str, files: list[str]) -> list[str]:
    """
    Filters the list of files to get only the cloud cover files for the specified site.
    """
    return [file for file in files if f"Cloud_{site}" in file and file.endswith(".txt")]


@dg.sensor(
    job_name="analysis_job",
    minimum_interval_seconds=1 * 60 * 60,  # 1 hour
)
def instrument_upload_sensor(context, s3: S3ResourceNCSA):
    objects = list_files(EnvVar('DEST_BUCKET').get_value(), "raw", s3.get_client())
    files = group_files_by_date(objects)

    for data_date in sorted(files.keys()):
        sensor_files = files[data_date]
        sensor_date = data_date

        if not sensor_files or len(sensor_files) <= 1:
            continue

        context.log.info(f"Found {len(sensor_files)} files on {sensor_date}")

        # Key by (site_code, stub) so instruments at the same site stay separate.
        # e.g. ('uao', 'fpi05') and ('uao', 'fpi13') are distinct keys.
        tar_gz_files = {}   # dict[(site, stub), list[str]]
        complete_sites = {}  # dict[(site, stub), str]

        for file in sensor_files:
            filename = file.split('/')[-1]
            parts = filename.split('_')
            if len(parts) < 2:
                continue

            stub = parts[0]       # e.g. 'fpi05' or 'fpi13'
            site_code = parts[1]  # e.g. 'uao'
            key = (site_code, stub)

            if filename.startswith("fpi") and filename.endswith(".txt"):
                # Log file — signals that all chunks for this instrument are uploaded
                complete_sites[key] = file
                continue

            if "tar.gz" in file:
                if key in tar_gz_files:
                    tar_gz_files[key].append(file)
                else:
                    tar_gz_files[key] = [file]

        context.log.info(f"Found files for {list(tar_gz_files.keys())}")

        for (site, stub) in tar_gz_files.keys():
            key = (site, stub)
            if key not in complete_sites:
                context.log.info(
                    f"Incomplete upload for {stub} at {site} on {sensor_date} "
                    f"— will pick up next time"
                )
                continue

            match = re.search(r'fpi(\d{2})', stub)
            if match:
                instrument_name = f"minime{match.group(1)}"
            else:
                context.log.warning(
                    f"Could not determine instrument name from stub '{stub}' "
                    f"for {tar_gz_files[key][0]}"
                )
                instrument_name = "minime??"

            run_config = RunConfig({
                "unzip_chunked_archive": ChunkedArchiveConfig(
                    site=site,
                    observation_date=str(sensor_date),
                    cloud_files=cloud_cover_files_for_site(site, objects),
                    file_chunks=sorted(tar_gz_files[key]),
                    instrument_name=instrument_name,
                    instrument_log_file=complete_sites[key],
                )
            })

            yield dg.RunRequest(
                run_key=f"sort-{sensor_date}-{site}-{stub}",  # unique per instrument
                run_config=run_config,
                tags={
                    "site": site,
                    "instrument_name": instrument_name,
                    "observation_date": str(sensor_date),
                },
            )
