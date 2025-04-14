import os
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory

import dagster as dg
from dagster import EnvVar
from dagster_aws.s3 import S3FakeSession
from dagster_ncsa import S3ResourceNCSA


class ChunkedArchiveConfig(dg.Config):
    observation_date: str = "20250328"
    site: str = "uao"
    file_chunks: list[str] = ["raw/fpi05_uao_20250328.tar.gz000000",
                              "raw/fpi05_uao_20250328.tar.gz000001",
                              "raw/fpi05_uao_20250328.tar.gz000002",
                              "raw/fpi05_uao_20250328.tar.gz000003",
                              "raw/fpi05_uao_20250328.tar.gz000004",
                              "raw/fpi05_uao_20250328.tar.gz000005",
                              "raw/fpi05_uao_20250328.tar.gz000006",
                              "raw/fpi05_uao_20250328.tar.gz000007",
                              "raw/fpi05_uao_20250328.tar.gz000008"]
    cloud_files: list[str] = ["raw/Cloud_uao_20250328.txt"]
    instrument_name: str = "minime05"


def upload_chunked_archive(data_path: str,
                           config: ChunkedArchiveConfig,
                           context: dg.AssetExecutionContext,
                           s3: S3FakeSession):
    """
    Uploads a chunked archive to the destination bucket. This accepts a list
    of tar.gz chunks, downloads them to a temp directory, combines them into a
    single tar.gz file, and unzips them to the destination bucket.
    """
    with TemporaryDirectory() as tmp_dir:
        chunk_dir = tmp_dir / Path("chunks")
        chunk_dir.mkdir(parents=True, exist_ok=True)

        downloaded_chunks = []
        for chunk in config.file_chunks:
            downloaded = chunk_dir / Path(chunk).name
            context.log.info(f"Downloading {chunk}")
            s3.download_file(
                Bucket=EnvVar("DEST_BUCKET").get_value(),
                Key=chunk,
                Filename=downloaded,
            )
            downloaded_chunks.append(downloaded)

        # Sort the downloaded chunks in order merge the correctly
        downloaded_chunks.sort()

        # Merge the chunks into a single tar.gz file
        combined = Path(tmp_dir) / f"{config.site}_{config.observation_date}.tar.gz"

        with open(combined, 'wb') as outfile:
            for file_path in downloaded_chunks:
                with open(file_path, 'rb') as infile:
                    outfile.write(infile.read())

        # Unzip the combined file
        out_dir = Path(tmp_dir) / Path("output")
        with tarfile.open(combined, "r:gz") as tar:
            tar.extractall(path=out_dir)

        # Upload the unzipped files to the destination bucket
        for root, dirs, files in os.walk(out_dir):
            for file in files:
                context.log.info(f"upload {file}")
                s3.upload_file(
                    Filename=os.path.join(root, file),
                    Bucket=EnvVar("DEST_BUCKET").get_value(),
                    Key=f"{data_path}{file}",
                )


@dg.asset
def unzip_chunked_archive(
        context: dg.AssetExecutionContext,
        config: ChunkedArchiveConfig,
        s3: S3ResourceNCSA
) -> dg.Output[dict[str, list[str]]]:
    """Unzips a chunked archive"""
    year = config.observation_date[0:4]
    data_path = f"fpi/{config.instrument_name}/{config.site}/{year}/{config.observation_date}/"
    s3_client = s3.get_client()
    upload_chunked_archive(data_path, config, context, s3_client)

    # Copy the cloud cover files to the archive directory in the bucket
    for cloud_cover_file in config.cloud_files:
        s3_client.copy_object(
            Bucket=EnvVar("DEST_BUCKET").get_value(),
            CopySource={
                "Bucket": EnvVar("DEST_BUCKET").get_value(),
                "Key": cloud_cover_file
            },
            Key=f"cloudsensor/{config.site}/{year}/{Path(cloud_cover_file).name}"
        )

    return dg.Output(
        value={
            "raw_files": config.file_chunks,
            "cloud_data": config.cloud_files
        },
        metadata={
            "observation_date": config.observation_date,
            "site": config.site
        }
    )


# Define asset job
unzip_archive_job = dg.define_asset_job(
    "unzip_archive_job",
    selection=[unzip_chunked_archive]
)
