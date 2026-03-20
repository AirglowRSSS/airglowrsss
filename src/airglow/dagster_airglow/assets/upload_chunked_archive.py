import os
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
from typing import Optional

import dagster as dg
from botocore.exceptions import ClientError
from dagster import EnvVar
from dagster_aws.s3 import S3FakeSession
from dagster_ncsa import S3ResourceNCSA
from airglow.dagster_airglow.assets.analysis_asset import AnalysisConfig
from airglow.dagster_airglow.assets.delete_raw import DeleteRawConfig


class ChunkedArchiveConfig(dg.Config):
    observation_date: str = "20250328"
    site: str = "uao"
    instrument_name: str = "minime05"
    file_chunks: Optional[list[str]] = None
    cloud_files: Optional[list[str]] = None
    instrument_log_file: Optional[str] = None


def _fpi_stub(instrument_name: str) -> str:
    """Derives the fpi file prefix from an instrument name (e.g. minime05 -> fpi05)."""
    return instrument_name.replace("minime", "fpi")


def _list_s3_prefix(s3_client, bucket: str, prefix: str) -> list[str]:
    """Returns all S3 keys matching the given prefix, across pages."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def resolve_config(
    config: ChunkedArchiveConfig,
    s3_client,
    bucket: str,
    context: dg.AssetExecutionContext,
) -> tuple[list[str], list[str], str]:
    """
    Resolves file_chunks, cloud_files, and instrument_log_file from the config.
    If any of these are None, they are auto-generated from instrument_name,
    site, and observation_date via S3 listing or naming convention.
    Returns (file_chunks, cloud_files, instrument_log_file).
    """
    stub = _fpi_stub(config.instrument_name)

    # --- file_chunks ---
    if config.file_chunks is not None:
        file_chunks = config.file_chunks
    else:
        prefix = f"raw/{stub}_{config.site}_{config.observation_date}.tar.gz"
        context.log.info(f"file_chunks not provided; listing S3 with prefix: {prefix}")
        file_chunks = sorted(_list_s3_prefix(s3_client, bucket, prefix))
        if not file_chunks:
            raise Exception(
                f"No file chunks found in bucket '{bucket}' for prefix '{prefix}'. "
                "Verify instrument_name, site, and observation_date are correct."
            )
        context.log.info(f"Auto-discovered {len(file_chunks)} chunk(s): {file_chunks}")

    # --- cloud_files ---
    if config.cloud_files is not None:
        cloud_files = config.cloud_files
    else:
        cloud_files = [f"raw/Cloud_{config.site}_{config.observation_date}.txt"]
        context.log.info(f"cloud_files not provided; defaulting to: {cloud_files}")

    # --- instrument_log_file ---
    if config.instrument_log_file is not None:
        instrument_log_file = config.instrument_log_file
    else:
        instrument_log_file = f"raw/{stub}_{config.site}_{config.observation_date}.txt"
        context.log.info(f"instrument_log_file not provided; defaulting to: {instrument_log_file}")

    return file_chunks, cloud_files, instrument_log_file


def upload_chunked_archive(data_path: str,
                           config: ChunkedArchiveConfig,
                           file_chunks: list[str],
                           context: dg.AssetExecutionContext,
                           s3: S3FakeSession) -> int:
    """
    Uploads a chunked archive to the destination bucket. This accepts a list
    of tar.gz chunks, downloads them to a temp directory, combines them into a
    single tar.gz file, and unzips them to the destination bucket.
    """
    with TemporaryDirectory() as tmp_dir:
        chunk_dir = Path(tmp_dir) / "chunks"
        chunk_dir.mkdir(parents=True, exist_ok=True)

        downloaded_chunks = []
        for chunk in file_chunks:
            downloaded = chunk_dir / Path(chunk).name
            context.log.info(f"Downloading {chunk}")
            s3.download_file(
                Bucket=EnvVar("DEST_BUCKET").get_value(),
                Key=chunk,
                Filename=str(downloaded),
            )
            downloaded_chunks.append(downloaded)

        # Sort the downloaded chunks in order to merge correctly
        downloaded_chunks.sort()

        # Merge the chunks into a single tar.gz file
        combined = Path(tmp_dir) / f"{config.site}_{config.observation_date}.tar.gz"

        with open(combined, 'wb') as outfile:
            for file_path in downloaded_chunks:
                with open(file_path, 'rb') as infile:
                    outfile.write(infile.read())

        # Unzip the combined file
        out_dir = Path(tmp_dir) / "output"
        with tarfile.open(combined, "r:gz") as tar:
            tar.extractall(path=out_dir)

        # Upload the unzipped files to the destination bucket
        uploaded_files = 0
        for root, dirs, files in os.walk(out_dir):
            for file in files:
                context.log.info(f"upload {file}")
                s3.upload_file(
                    Filename=os.path.join(root, file),
                    Bucket=EnvVar("DEST_BUCKET").get_value(),
                    Key=f"{data_path}{file}",
                )
            uploaded_files = len(files)
    return uploaded_files


@dg.asset
def unzip_chunked_archive(
        context: dg.AssetExecutionContext,
        config: ChunkedArchiveConfig,
        s3: S3ResourceNCSA
) -> dg.Output:
    """Unzips a chunked archive"""
    bucket = EnvVar("DEST_BUCKET").get_value()
    s3_client = s3.get_client()

    # Resolve any auto-generated fields before proceeding
    file_chunks, cloud_files, instrument_log_file = resolve_config(
        config, s3_client, bucket, context
    )

    year = config.observation_date[0:4]
    data_path = f"fpi/{config.instrument_name}/{config.site}/{year}/{config.observation_date}/"
    uploaded_files = upload_chunked_archive(data_path, config, file_chunks, context, s3_client)

    # Copy the cloud cover files to the archive directory in the bucket
    cloud_cover_path = f"cloudsensor/{config.site}/{year}"
    for cloud_cover_file in cloud_files:

        # Check if the cloud cover file exists before copying
        try:
            s3_client.head_object(
                Bucket=bucket,
                Key=cloud_cover_file
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                context.log.warning(f"Cloud cover file not found, skipping: {cloud_cover_file}")
                continue
            raise  # re-raise unexpected errors

        s3_client.copy_object(
            Bucket=bucket,
            CopySource={
                "Bucket": bucket,
                "Key": cloud_cover_file
            },
            Key=f"{cloud_cover_path}/{Path(cloud_cover_file).name}"
        )

    analysis_config = AnalysisConfig(
        site=config.site,
        observation_date=config.observation_date,
    )

    delete_raw_config = DeleteRawConfig(
        site=config.site,
        observation_date=config.observation_date,
        raw_files=file_chunks,
        cloud_cover_files=cloud_files,
        instrument_log_file=instrument_log_file,
    )

    return dg.MaterializeResult(
        metadata={
            "analysis_config": dg.MetadataValue.json(analysis_config.__dict__),
            "delete_raw_config": dg.MetadataValue.json(delete_raw_config.__dict__),
            "observation_date": config.observation_date,
            "site": config.site,
            "dataset_files": uploaded_files,
            "cloud_cover_files": len(cloud_files),
        }
    )


# Define asset job
unzip_archive_job = dg.define_asset_job(
    "unzip_archive_job",
    selection=[unzip_chunked_archive]
)
