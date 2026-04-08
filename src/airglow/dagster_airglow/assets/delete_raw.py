import datetime

import dagster as dg
from dagster_ncsa import S3ResourceNCSA

from airglow import fpiinfo


def _fpi_stub(instrument_name: str) -> str:
    """Derives the fpi file prefix from an instrument name (e.g. minime05 -> fpi05)."""
    return instrument_name.replace("minime", "fpi")


class DeleteRawConfig(dg.Config):
    site: str
    observation_date: str
    instrument_name: str
    raw_files: list[str]
    cloud_cover_files: list[str]
    instrument_log_file: str


@dg.asset(
    deps=["unzip_chunked_archive"],
)
def delete_raw(
        context: dg.AssetExecutionContext,
        s3: S3ResourceNCSA):
    upstream_metadata = context.instance.get_latest_materialization_event(
        dg.AssetKey("unzip_chunked_archive")).asset_materialization.metadata
    config = DeleteRawConfig(**upstream_metadata['delete_raw_config'].data)
    context.log.info(f"Delete Raw config: {config}")

    context.log.info(
        f"Deleting raw files for instrument={config.instrument_name} "
        f"site={config.site} date={config.observation_date}"
    )
    s3_client = s3.get_client()
    bucket = dg.EnvVar("DEST_BUCKET").get_value()

    expected_stub = _fpi_stub(config.instrument_name)
    expected_prefix = f"{expected_stub}_{config.site}_{config.observation_date}"

    # ------------------------------------------------------------------
    # Delete raw archive chunks — only those matching this instrument
    # ------------------------------------------------------------------
    for raw_file in config.raw_files:
        filename = raw_file.split("/")[-1]
        if not filename.startswith(expected_prefix):
            context.log.warning(
                f"Skipping raw file that does not match expected pattern "
                f"'{expected_prefix}': {raw_file}"
            )
            continue
        context.log.info(f"Deleting raw file: {raw_file}")
        s3_client.delete_object(Bucket=bucket, Key=raw_file)

    # ------------------------------------------------------------------
    # Delete cloud cover files — only when this is the sole instrument
    # at the site on this date, since the cloud file is shared.
    # ------------------------------------------------------------------
    nominal_dt = datetime.datetime.strptime(config.observation_date, "%Y%m%d")
    instruments_at_site = fpiinfo.get_instr_at(config.site, nominal_dt)

    if len(instruments_at_site) > 1:
        context.log.warning(
            f"Multiple instruments at site '{config.site}' on "
            f"{config.observation_date}: {instruments_at_site}. "
            f"Skipping deletion of shared cloud cover file(s) "
            f"{config.cloud_cover_files} — clean these up manually once "
            f"all instrument pipelines for this date have completed."
        )
    else:
        for cloud_cover_file in config.cloud_cover_files:
            context.log.info(f"Deleting cloud cover file: {cloud_cover_file}")
            s3_client.delete_object(Bucket=bucket, Key=cloud_cover_file)

    # ------------------------------------------------------------------
    # Delete instrument log file — validate it matches this instrument
    # ------------------------------------------------------------------
    log_filename = config.instrument_log_file.split("/")[-1]
    if log_filename.startswith(expected_prefix):
        context.log.info(f"Deleting log file: {config.instrument_log_file}")
        s3_client.delete_object(Bucket=bucket, Key=config.instrument_log_file)
    else:
        context.log.warning(
            f"Skipping log file that does not match expected pattern "
            f"'{expected_prefix}': {config.instrument_log_file}"
        )
