"""
FPIDataHandler.py — Load FPI result (.npz) files into a tidy xarray Dataset.

Extracts and generalises the per-file loading logic that was previously
embedded in FPIDisplayNew.DataSummary so that the resulting Dataset can be
passed directly to DataSummary (or any other downstream consumer).

Dataset layout
--------------
A single flat ``time`` dimension is used (one element per sky observation,
sorted chronologically).  Direction is stored as a non-dimension coordinate
so that consumers can vectorise over all observations at once rather than
iterating per-direction — this simplifies the keogram binning in DataSummary
and allows natural boolean masking like ``ds.where(ds.direction == 'North')``.

    dims:    time
    coords:  time (datetime64), direction (str, same length as time)
    vars:    LOSwind, sigma_LOSwind, T, sigma_T, skyI, sigma_skyI,
             ze, ref_Dop, cloud_mean
    attrs:   site, emission, date_range
"""

import datetime
import logging
import os
import tempfile
from typing import List, Optional, Tuple, Union

import numpy as np
import xarray as xr

import airglow.FPI as FPI
import airglow.fpiinfo as fpiinfo

logger = logging.getLogger(__name__)

_ALL_DIRECTIONS = ['North', 'South', 'East', 'West', 'Zenith']
_EMISSION_MAP   = {'green': 'xg', 'red': 'xr'}


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _strip_tz(dt):
    """Return tz-naive copy of dt, preserving local wall-clock time."""
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def _norm_date(d):
    """Accept datetime.date, datetime.datetime, or ISO string → datetime.date."""
    if isinstance(d, str):
        return datetime.date.fromisoformat(d)
    if isinstance(d, datetime.datetime):
        return d.date()
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def load_fpi_data(
    site: str,
    date,
    emission: str,
    *,
    directions: Optional[List[str]] = None,
    reference: str = 'zenith',
    instrument: Optional[str] = None,
    cloud_storage=None,
    local_dir: Optional[str] = None,
    verbose: bool = False,
) -> xr.Dataset:
    """
    Load FPI result files for a site/date/emission into a tidy xr.Dataset.

    Parameters
    ----------
    site : str
        Site abbreviation, e.g. ``'ann'``, ``'uao'``.
    date : datetime.date | datetime.datetime | str | tuple
        Single night date **or** a ``(start, end)`` tuple for a date range.
        Strings must be ISO-format (``'YYYY-MM-DD'``).  A single date loads
        only that night; a tuple loads every night from start through end
        (inclusive).
    emission : str
        ``'green'`` (557.7 nm, maps to ``xg``) or
        ``'red'`` (630.0 nm, maps to ``xr``).
    directions : list of str, optional
        Which look directions to include.  Default: all five
        ``['North', 'South', 'East', 'West', 'Zenith']``.
    reference : str
        Doppler reference method passed to ``FPI.DopplerReference``.
        ``'zenith'`` (default) assumes zero vertical wind;
        ``'laser'`` trusts the laser calibration lamp.
    instrument : str, optional
        Override the instrument name instead of looking it up via
        ``fpiinfo.get_instr_at``.  Required when two instruments were
        operating at the same site on the same night.
    cloud_storage : CloudStorage, optional
        If provided, files not found locally are downloaded from cloud
        storage using the key ``results/{year}/{instr}_{site}_{date}_{em}.npz``.
    local_dir : str, optional
        Directory to search for local ``.npz`` files before attempting a
        cloud download.  Files are matched by basename (flat) or by the
        ``{year}/`` subdirectory layout.
    verbose : bool
        If ``True``, log a message for each night where the file is not
        found or skipped.

    Returns
    -------
    xr.Dataset
        Tidy dataset with dimension ``time`` (one row per sky observation,
        sorted chronologically) and non-dimension coordinate ``direction``.
        Times are stored as local wall-clock datetimes (timezone stripped)
        so that ``_lt_hour()`` in FPIDisplayNew works without conversion.

        Variables: ``LOSwind``, ``sigma_LOSwind``, ``T``, ``sigma_T``,
        ``skyI``, ``sigma_skyI``, ``ze``, ``ref_Dop``, ``cloud_mean``.
        ``ref_Dop`` is computed via ``FPI.DopplerReference`` at load time.
        ``cloud_mean`` is NaN where no cloud sensor was available.

        Dataset attributes: ``site``, ``emission`` (tag, e.g. ``'xg'``),
        ``date_range``.

    Raises
    ------
    ValueError
        If *emission* is not ``'green'`` or ``'red'``.
    ValueError
        If more than one instrument is found at *site* on a given night and
        *instrument* was not specified.
    ValueError
        If no data at all was found across the requested date range.
    """
    if directions is None:
        directions = list(_ALL_DIRECTIONS)

    emission_tag = _EMISSION_MAP.get(emission.lower())
    if emission_tag is None:
        raise ValueError(
            f"emission must be 'green' or 'red', got '{emission}'."
        )

    # ── Date range ─────────────────────────────────────────────────────────
    if isinstance(date, tuple):
        start_date = _norm_date(date[0])
        end_date   = _norm_date(date[1])
    else:
        start_date = end_date = _norm_date(date)

    nights = []
    d = start_date
    while d <= end_date:
        nights.append(d)
        d += datetime.timedelta(days=1)

    # ── Per-observation accumulators ───────────────────────────────────────
    times_list      = []
    dir_list        = []
    los_list        = []
    sigma_los_list  = []
    T_list          = []
    sigma_T_list    = []
    skyI_list       = []
    sigma_skyI_list = []
    ze_list         = []
    ref_dop_list    = []
    cloud_list      = []

    for night in nights:
        # Use noon on the calendar date for fpiinfo lookups (avoids
        # midnight ambiguity with the observing-night convention).
        dt_noon = datetime.datetime(night.year, night.month, night.day, 12, 0, 0)

        # ── Instrument lookup ──────────────────────────────────────────────
        if instrument is not None:
            instr = instrument
        else:
            instrs = fpiinfo.get_instr_at(site, dt_noon)
            if len(instrs) == 0:
                if verbose:
                    logger.info(
                        "No instrument found at %s on %s; skipping.", site, night
                    )
                continue
            if len(instrs) > 1:
                raise ValueError(
                    f"Multiple instruments at '{site}' on {night}: {instrs}. "
                    "Specify instrument= to select one."
                )
            instr = instrs[0]

        yyyymmdd  = night.strftime('%Y%m%d')
        year_str  = night.strftime('%Y')
        basename  = f"{instr}_{site}_{yyyymmdd}_{emission_tag}.npz"
        cloud_key = f"results/{year_str}/{basename}"

        # ── Locate file: local first, then cloud download ──────────────────
        local_path = None
        temp_file  = None

        if local_dir is not None:
            # Try flat layout (all files in one directory)
            flat = os.path.join(local_dir, basename)
            # Try year-subdirectory layout
            deep = os.path.join(local_dir, year_str, basename)
            if os.path.exists(flat):
                local_path = flat
            elif os.path.exists(deep):
                local_path = deep

        if local_path is None and cloud_storage is not None:
            tmp = tempfile.NamedTemporaryFile(suffix='.npz', delete=False)
            tmp.close()
            temp_file = tmp.name
            if cloud_storage.download_file(cloud_key, temp_file):
                local_path = temp_file
            else:
                if verbose:
                    logger.info(
                        "Could not download %s; skipping.", cloud_key
                    )
                try:
                    os.unlink(temp_file)
                except OSError:
                    pass
                temp_file = None
                continue

        if local_path is None:
            if verbose:
                logger.info(
                    "File not found for %s at %s (%s); skipping.",
                    night, site, cloud_key,
                )
            continue

        # ── Load and extract data ──────────────────────────────────────────
        try:
            npz         = np.load(local_path, allow_pickle=True, encoding='latin1')
            FPI_Results = npz['FPI_Results'].reshape(-1)[0]
            npz.close()
        except Exception as exc:
            logger.warning("Failed to load %s: %s", local_path, exc)
            continue
        finally:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                except OSError:
                    pass

        sky_times = FPI_Results['sky_times']

        # Doppler reference — computed once per file for all directions
        try:
            ref_dop, _ = FPI.DopplerReference(FPI_Results, reference=reference)
        except Exception as exc:
            logger.warning(
                "DopplerReference failed for %s on %s: %s; skipping.",
                site, night, exc,
            )
            continue

        has_clouds = (
            'Clouds' in FPI_Results
            and FPI_Results['Clouds'] is not None
        )

        # ── Per-direction accumulation ─────────────────────────────────────
        for direction in directions:
            idx = np.array(FPI.all_indices(direction, FPI_Results['direction']))
            if len(idx) == 0:
                continue

            n = len(idx)
            times_list.extend(
                [_strip_tz(sky_times[i]) for i in idx]
            )
            dir_list.extend([direction] * n)
            los_list.extend(FPI_Results['LOSwind'][idx])
            sigma_los_list.extend(FPI_Results['sigma_LOSwind'][idx])
            T_list.extend(FPI_Results['T'][idx])
            sigma_T_list.extend(FPI_Results['sigma_T'][idx])
            skyI_list.extend(FPI_Results['skyI'][idx])
            sigma_skyI_list.extend(FPI_Results['sigma_skyI'][idx])
            ze_list.extend(FPI_Results['ze'][idx])
            ref_dop_list.extend(ref_dop[idx])

            if has_clouds:
                cloud_list.extend(FPI_Results['Clouds']['mean'][idx])
            else:
                cloud_list.extend([np.nan] * n)

    if not times_list:
        raise ValueError(
            f"No data found for site='{site}', date={date}, "
            f"emission='{emission}'."
        )

    # ── Sort by time and build Dataset ─────────────────────────────────────
    times_arr = np.array(
        [np.datetime64(t, 'ns') for t in times_list]
    )
    order = np.argsort(times_arr, kind='stable')

    ds = xr.Dataset(
        {
            'LOSwind':       ('time', np.array(los_list,         dtype=float)[order]),
            'sigma_LOSwind': ('time', np.array(sigma_los_list,   dtype=float)[order]),
            'T':             ('time', np.array(T_list,           dtype=float)[order]),
            'sigma_T':       ('time', np.array(sigma_T_list,     dtype=float)[order]),
            'skyI':          ('time', np.array(skyI_list,        dtype=float)[order]),
            'sigma_skyI':    ('time', np.array(sigma_skyI_list,  dtype=float)[order]),
            'ze':            ('time', np.array(ze_list,          dtype=float)[order]),
            'ref_Dop':       ('time', np.array(ref_dop_list,     dtype=float)[order]),
            'cloud_mean':    ('time', np.array(cloud_list,       dtype=float)[order]),
        },
        coords={
            'time':      times_arr[order],
            'direction': ('time', np.array(dir_list)[order]),
        },
        attrs={
            'site':       site,
            'emission':   emission_tag,
            'date_range': f"{start_date} – {end_date}",
        },
    )

    return ds


# ─────────────────────────────────────────────────────────────────────────────
# Demo / smoke-test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    import matplotlib.pyplot as plt
    import airglow.FPIDisplayNew as FPIDisplayNew

    # ── Configuration — edit these to match your environment ──────────────
    SITE       = 'ann'
    EMISSION   = 'red'
    LOCAL_DIR  = '/rdata/airglow/fpi/results'
    SINGLE_NIGHT = datetime.date(2023, 3, 15)
    RANGE_START  = datetime.date(2023, 3,  1)
    RANGE_END    = datetime.date(2023, 3, 31)

    # ── Example 1: single night ───────────────────────────────────────────
    print(f"Loading single night: {SINGLE_NIGHT}")
    try:
        ds1 = load_fpi_data(SITE, SINGLE_NIGHT, EMISSION,
                            local_dir=LOCAL_DIR, verbose=True)
        print(ds1)
        print(f"  Observations: {ds1.sizes['time']}")
        print(f"  Directions:   {np.unique(ds1.coords['direction'].values).tolist()}")
    except ValueError as e:
        print(f"  No data: {e}")

    print()

    # ── Example 2: date range ─────────────────────────────────────────────
    print(f"Loading date range: {RANGE_START} – {RANGE_END}")
    try:
        ds_range = load_fpi_data(SITE, (RANGE_START, RANGE_END), EMISSION,
                                 local_dir=LOCAL_DIR, verbose=True)
        print(ds_range)
        print(f"  Observations: {ds_range.sizes['time']}")
    except ValueError as e:
        print(f"  No data: {e}")
        sys.exit(0)

    print()

    # ── Example 3: pass Dataset to FPIDisplayNew.DataSummary ─────────────
    print("Passing Dataset to DataSummary (publication mode, zonal + meridional)…")
    results = FPIDisplayNew.DataSummary(
        ds_range,
        variables=['U', 'V'],
        mode='publication',
    )
    for var, (fig, ax) in results.items():
        fname = f'fpidatahandler_test_{SITE}_{var}.png'
        fig.savefig(fname, dpi=150)
        print(f"  saved {fname}")
    plt.close('all')

    print("Done.")
