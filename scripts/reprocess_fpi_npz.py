#!/usr/bin/env python3
# reprocess_fpi_npz.py
#
# Regenerate FPI summary plots (and push them to the website) from an
# already-processed .npz results file, without re-running the raw laser/sky
# image analysis.
#
# This is the tool to reach for when FPIprocess.process_instr previously
# succeeded in producing a .npz file but something downstream of that (plot
# generation, website upload, etc.) failed. It downloads the existing .npz
# and cloud-sensor data from S3, calls
# FPIprocess.process_instr(..., use_npz=True) -- which skips FPI.ParameterFit
# and instead loads the saved FPI_Results, reloads the cloud-sensor data, and
# regenerates the summary plots -- then re-uploads the results.
#
# Mirrors the S3/MySQL wiring and GitHub issue reporting used by
# airglow.dagster_airglow.assets.analysis_asset.reanalyze_data, but as a
# standalone CLI script that can be run directly against S3 rather than
# through a Dagster run.
#
# Usage:
#   python scripts/reprocess_fpi_npz.py \
#       --site blo --years 2026 --doys 185 \
#       --instruments minime12 --tags XG,XR --reference zenith

import argparse
import io
import logging
import os
import sys
import tempfile
import traceback
from datetime import datetime, timedelta

from dotenv import load_dotenv

from dagster import EnvVar
from dagster_mysql import MySQLResource
from dagster_ncsa import S3ResourceNCSA

from airglow import FPIprocess, fpiinfo
from airglow.exceptions import (
    BadDopplerReferenceError,
    BadLaserError,
    InstrumentProcessingError,
    LaserProcessingError,
)
from airglow.GitHubIssueHandler import IssueType, SiteIssueManager
from airglow.github_config import github_config

# Maps exception types to GitHub issue subject-line prefixes. Walked via
# type(e).__mro__ so subclasses fall back to their nearest ancestor.
ERROR_SUBJECTS = {
    BadLaserError: "!!! Laser Processing Error",
    LaserProcessingError: "!!! Other Laser Error",
    InstrumentProcessingError: "!!! Instrument Error",
    BadDopplerReferenceError: "!!! Doppler Reference Error",
    Exception: "!!! Processing error",
}


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


def parse_int_list(spec: str) -> list[int]:
    """Parses '185', '180-190', or '180,182,185-187' into a sorted list of ints."""
    values = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-")
            values.update(range(int(start), int(end) + 1))
        else:
            values.add(int(part))
    return sorted(values)


def _list_s3_prefix(s3_client, bucket: str, prefix: str) -> list[str]:
    """Returns all S3 keys matching the given prefix, across pages."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _download_preserving_path(s3_client, bucket, keys, target_dir, logger):
    """Downloads S3 keys into target_dir, preserving the key's own path."""
    created = []
    for key in keys:
        local_path = os.path.join(target_dir, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        logger.info(f"Downloading s3://{bucket}/{key} -> {local_path}")
        s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path)
        created.append(local_path)
    return created


def _download_flat(s3_client, bucket, keys, target_dir, logger):
    """Downloads S3 keys into target_dir using only the key's basename."""
    created = []
    for key in keys:
        local_path = os.path.join(target_dir, os.path.basename(key))
        logger.info(f"Downloading s3://{bucket}/{key} -> {local_path}")
        s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path)
        created.append(local_path)
    return created


def download_cloud_sensor_data(s3, bucket, site, year, instr_temp, logger):
    """Downloads Boltwood cloud-sensor .txt files for (site, year)."""
    cloud_cover_path = f"cloudsensor/{site}/{year}"
    files = s3.list_files(bucket, cloud_cover_path, ".txt")
    return _download_preserving_path(s3.get_client(), bucket, files, instr_temp, logger)


def download_existing_npz_and_log(s3, bucket, results_path, year, instrsitedate, results_stub, logger):
    """Downloads the previously-saved .npz/.log for this instrument/site/date/tag."""
    prefix = f"{results_path}/{year}/{instrsitedate}"
    keys = _list_s3_prefix(s3.get_client(), bucket, prefix)
    if not any(k.endswith(".npz") for k in keys):
        raise FileNotFoundError(
            f"No existing .npz found at s3://{bucket}/{prefix}.npz -- "
            "use_npz reprocessing requires a previously-processed npz file."
        )
    return _download_flat(s3.get_client(), bucket, keys, results_stub, logger)


def upload_directory(s3, bucket, local_dir, object_prefix, logger):
    """Uploads every file under local_dir to s3://{bucket}/{object_prefix}/."""
    if not os.path.exists(local_dir):
        return []
    if object_prefix and not object_prefix.endswith("/"):
        object_prefix += "/"
    s3_client = s3.get_client()
    uploaded = []
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = object_prefix + relative_path
            logger.info(f"Uploading {local_path} -> s3://{bucket}/{s3_key}")
            s3_client.upload_file(local_path, bucket, s3_key)
            uploaded.append(local_path)
    return uploaded


def get_instrument_info(site, year, doy, instr_name, sky_line_tag):
    nominal_dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
    site_name = fpiinfo.get_site_of(instr_name, nominal_dt)
    datestr = nominal_dt.strftime("%Y%m%d")
    instrsitedate = f"{instr_name}_{site_name}_{datestr}"
    if sky_line_tag != "X":
        instrsitedate += f"_{sky_line_tag.lower()}"
    return site_name, datestr, instrsitedate


def reprocess_one(
    s3, bucket, mysql, results_path, summary_path, madrigal_path,
    site, year, doy, instr_name, tag, reference,
    enable_windfield_estimate, send_to_website, logger,
):
    site_name, datestr, instrsitedate = get_instrument_info(site, year, doy, instr_name, tag)
    logger.info(f"Reprocessing {instrsitedate} from existing npz (tag={tag}, reference={reference})")

    with tempfile.TemporaryDirectory() as instr_temp:
        bw_dir = os.path.join(instr_temp, "cloudsensor/")
        x300_dir = os.path.join(instr_temp, "templogs/x300/")
        results_stub = os.path.join(instr_temp, "results/")
        madrigal_stub = os.path.join(instr_temp, "madrigal/")
        temp_plots_stub = os.path.join(instr_temp, "temporary_plots/")
        for d in (bw_dir, x300_dir, results_stub, madrigal_stub, temp_plots_stub):
            os.makedirs(d, exist_ok=True)

        # NOTE: no raw fpi/ laser or sky images are downloaded here -- use_npz=True
        # skips FPI.ParameterFit entirely and loads FPI_Results from the npz below.
        download_cloud_sensor_data(s3, bucket, site, year, instr_temp, logger)
        download_existing_npz_and_log(s3, bucket, results_path, year, instrsitedate, results_stub, logger)

        warning = FPIprocess.process_instr(
            instr_name, year, doy,
            mysql=mysql,
            reference=reference,
            use_npz=True,
            enable_windfield_estimate=enable_windfield_estimate,
            send_to_website=send_to_website,
            sky_line_tag=tag,
            bw_dir=bw_dir,
            x300_dir=x300_dir,
            results_stub=results_stub,
            madrigal_stub=madrigal_stub,
            temp_plots_stub=temp_plots_stub,
        )

        upload_directory(s3, bucket, results_stub, f"{results_path}/{year}", logger)
        upload_directory(s3, bucket, temp_plots_stub, f"{summary_path}/{year}", logger)
        upload_directory(s3, bucket, madrigal_stub, f"{madrigal_path}/{year}", logger)

    return warning


def report_error(issue_manager, site, instr_name, year, doy, tag, error, logger):
    subject_prefix = ERROR_SUBJECTS[Exception]
    for exc_type in type(error).__mro__:  # walk up the inheritance chain
        if exc_type in ERROR_SUBJECTS:
            subject_prefix = ERROR_SUBJECTS[exc_type]
            break

    warning_messages = ""
    if getattr(error, "warning_log", None):
        warning_messages = f"\n\nWarnings before error:\n{error.warning_log}"

    tb_stream = io.StringIO()
    traceback.print_exc(file=tb_stream)
    tb_string = tb_stream.getvalue()

    logger.error(f"{subject_prefix} '{instr_name}',{year},{doy},{tag} @ {site}")
    logger.error(tb_string)

    if issue_manager is None:
        return

    issue_manager.handle_processing_issue(
        site_id=site,
        message=str(error) + warning_messages,
        error_type=error.__class__.__name__,
        category=IssueType.ERROR,
        additional_context={
            "Instrument": instr_name,
            "Year": year,
            "Day of Year": doy,
            "Tag": tag,
            "Processing Stage": type(error).__name__,
            "Traceback": tb_string,
        },
    )


def report_warning(issue_manager, site, instr_name, year, doy, tag, warning, logger):
    subject = f"!!! Manually inspect ('{instr_name}',{year},{doy}) @ {site}"
    logger.warning(subject)

    if issue_manager is None:
        return

    issue_manager.handle_processing_issue(
        site_id=site,
        message=warning,
        category=IssueType.WARNING,
        additional_context={
            "Instrument": instr_name,
            "Year": year,
            "Day of Year": doy,
            "Tag": tag,
        },
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate FPI summary plots from previously-saved .npz results "
            "(FPIprocess.process_instr(..., use_npz=True)) without re-running "
            "the raw laser/sky image analysis."
        )
    )
    parser.add_argument("-s", "--site", required=True, help="Site code, e.g. 'uao' or 'blo'")
    parser.add_argument("-y", "--years", required=True, help="Year, or '2025-2026' / '2025,2026'")
    parser.add_argument("-d", "--doys", required=True, help="Day(s) of year, e.g. '185' or '180-190' or '180,182,185-187'")
    parser.add_argument("-i", "--instruments", required=True, help="Comma-separated instrument names, e.g. 'minime12,minime05'")
    parser.add_argument("-t", "--tags", default="XG,XR", help="Comma-separated sky_line_tag values (default: XG,XR)")
    parser.add_argument("-r", "--reference", default="zenith", choices=["zenith", "laser"], help="Doppler reference (default: zenith)")
    parser.add_argument("--enable-windfield-estimate", action="store_true", help="Pass enable_windfield_estimate=True to process_instr")
    parser.add_argument("--no-send-to-website", action="store_true", help="Do not update the website database")
    parser.add_argument("-e", "--env", default=".env", help="Path to .env file (default: .env)")
    parser.add_argument("--no-github-issues", action="store_true", help="Do not create/update GitHub issues on error/warning")
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging()

    load_dotenv(args.env)

    bucket = EnvVar("DEST_BUCKET").get_value()
    results_path = EnvVar("RESULTS_PATH").get_value()
    summary_path = EnvVar("SUMMARY_IMAGES_PATH").get_value()
    madrigal_path = EnvVar("MADRIGAL_PATH").get_value()

    s3 = S3ResourceNCSA(
        endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    mysql = MySQLResource(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )

    issue_manager = None
    if not args.no_github_issues:
        try:
            issue_manager = SiteIssueManager(
                token=github_config.github_token,
                repo_name=github_config.github_repo,
            )
        except Exception as e:
            logger.error(f"Failed to initialize GitHub issue manager, continuing without it: {e}")

    site = args.site
    years = parse_int_list(args.years)
    doys = parse_int_list(args.doys)
    instruments = [i.strip() for i in args.instruments.split(",") if i.strip()]
    tags = [t.strip() for t in args.tags.split(",") if t.strip()]
    send_to_website = not args.no_send_to_website

    all_success = True
    failed = []

    for year in years:
        for doy in doys:
            for instr_name in instruments:
                for tag in tags:
                    logger.info(f"--- {instr_name} {year} {doy} {tag} @ {site} ---")
                    try:
                        warning = reprocess_one(
                            s3, bucket, mysql, results_path, summary_path, madrigal_path,
                            site, year, doy, instr_name, tag, args.reference,
                            args.enable_windfield_estimate, send_to_website, logger,
                        )
                        if warning:
                            report_warning(issue_manager, site, instr_name, year, doy, tag, warning, logger)
                    except Exception as e:
                        report_error(issue_manager, site, instr_name, year, doy, tag, e, logger)
                        all_success = False
                        failed.append((instr_name, year, doy, tag))

    if all_success:
        logger.info("All reprocessing runs succeeded")
    else:
        logger.error("Failed reprocessing runs:")
        for instr_name, year, doy, tag in failed:
            logger.error(f"  {instr_name} {year} {doy} {tag}")
        sys.exit(1)


if __name__ == "__main__":
    main()
