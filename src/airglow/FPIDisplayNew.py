"""
FPIDisplayNew.py — Refactored two-dimensional FPI data summary display.

Replaces the DataSummary function in FPIDisplay.py with an xarray-based
implementation that supports flexible time/date binning and publication-
or presentation-quality output modes.

Author: refactored from Jonathan J. Makela's original FPIDisplay.py
"""

import math
import os
import warnings
import numpy as np
import numpy.ma as ma
import pandas as pd
import xarray as xr
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
import datetime

import airglow.FPI as FPI
import airglow.fpiinfo as fpiinfo


# ─────────────────────────────────────────────────────────────────────────────
# Module-level display constants
# ─────────────────────────────────────────────────────────────────────────────

#: Colorblind-safe direction colours (Wong palette).
#: North/South are a warm/cool pair; East/West are a green/purple pair;
#: Zenith is neutral.  Shared by DataSummary overlays and MonthlySummaries.
DIRECTION_COLORS = {
    'North':  '#D55E00',   # vermillion  – warm
    'South':  '#56B4E9',   # sky blue    – cool
    'East':   '#009E73',   # bluish green
    'West':   '#CC79A7',   # reddish purple
    'Zenith': '#999999',   # neutral gray
}

#: Alpha for shaded uncertainty bands in MonthlySummaries.
#: Reduce if overlapping direction bands obscure each other.
_SHADE_ALPHA = 0.25


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_pandas_timestamps(times):
    """
    Accept matplotlib date numbers (floats), Python datetimes, or a
    pandas DatetimeIndex and return a normalized pandas DatetimeIndex
    (time component stripped; date only).
    """
    if isinstance(times, pd.DatetimeIndex):
        return times.normalize()
    arr = np.asarray(times)
    if arr.dtype.kind == 'f':          # matplotlib date numbers
        dts = mdates.num2date(arr)
        return pd.DatetimeIndex(dts).normalize()
    return pd.DatetimeIndex(times).normalize()


def _lt_hour(dt):
    """
    Convert a (possibly tz-aware) datetime to decimal local-time hour
    using the 17–32 h convention:
      - 17–24  →  evening (PM of the calendar date)
      - 24–32  →  early morning of the next calendar date

    This matches the behaviour of FPI.dt2h with no tz argument.
    """
    h = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    if h <= 12:
        h += 24.0
    return h


def _night_date(dt):
    """
    Return the calendar *date* of the observing night (the evening side).
    Observations recorded before local noon belong to the previous
    evening's night.
    """
    if dt.hour <= 12:
        return (dt - datetime.timedelta(days=1)).date()
    return dt.date()


def _mode_rcparams(mode):
    """
    Return (rc_dict, cmap_diverging, cmap_sequential) style settings for
    the requested output mode.

    Parameters
    ----------
    mode : {'publication', 'presentation'}

    Returns
    -------
    rc : dict
        Keyword arguments suitable for ``matplotlib.rc_context``.
    cmap_div : str
        Colormap name for diverging data (winds).
    cmap_seq : str
        Colormap name for sequential data (temperature, intensity).
    """
    if mode == 'presentation':
        rc = {
            'font.size': 14,
            'axes.titlesize': 16,
            'axes.labelsize': 14,
            'xtick.labelsize': 12,
            'ytick.labelsize': 12,
            'axes.linewidth': 1.5,
            'lines.linewidth': 2.0,
            'font.family': 'sans-serif',
        }
        cmap_div = 'bwr'
        cmap_seq = 'inferno'
    else:                              # 'publication'
        rc = {
            'font.size': 9,
            'axes.titlesize': 10,
            'axes.labelsize': 9,
            'xtick.labelsize': 8,
            'ytick.labelsize': 8,
            'axes.linewidth': 0.8,
            'lines.linewidth': 1.0,
            'font.family': 'sans-serif',
        }
        cmap_div = 'RdBu_r'
        cmap_seq = 'viridis'
    return rc, cmap_div, cmap_seq


def _accumulate_from_dataset(ds, time_edges, date_to_di,
                             n_tbins, n_nights, variables, cloudy_temperature):
    """
    Bin a tidy FPI Dataset (from FPIDataHandler.load_fpi_data) into the same
    weighted-sum accumulator grids that the file-loop path produces.

    Parameters
    ----------
    ds : xr.Dataset
        Must have dimension ``time``, non-dimension coordinate ``direction``,
        and variables ``LOSwind``, ``sigma_LOSwind``, ``T``, ``sigma_T``,
        ``skyI``, ``sigma_skyI``, ``ze``, ``ref_Dop``, ``cloud_mean``.
    time_edges : np.ndarray
        Bin edges in decimal local-time hours (same as outer DataSummary).
    date_to_di : dict
        Mapping from datetime.date → column index (di).
    n_tbins, n_nights : int
        Accumulator grid dimensions.
    variables : list of str
        Subset of ``['T','U','V','W','I']`` to compute.
    cloudy_temperature : float
        Cloud-sensor threshold [°C].  Obs above this value are excluded.

    Returns
    -------
    dict mapping variable name → 2-D np.ndarray (n_tbins × n_nights)
        Weighted means; NaN where no valid data.
    """
    zeros = lambda: np.zeros((n_tbins, n_nights))
    sum_wT, sum_w_T = zeros(), zeros()
    sum_wU, sum_w_U = zeros(), zeros()
    sum_wV, sum_w_V = zeros(), zeros()
    sum_wW, sum_w_W = zeros(), zeros()
    sum_wI, sum_w_I = zeros(), zeros()

    # Extract numpy arrays once — avoids repeated xarray overhead
    obs_times  = pd.DatetimeIndex(ds.coords['time'].values)
    dirs       = ds.coords['direction'].values
    LOSwind    = ds['LOSwind'].values
    ref_Dop    = ds['ref_Dop'].values
    sigma_los  = ds['sigma_LOSwind'].values
    T_arr      = ds['T'].values
    eT_arr     = ds['sigma_T'].values
    skyI_arr   = ds['skyI'].values
    eI_arr     = ds['sigma_skyI'].values
    ze_arr     = ds['ze'].values
    cloud_arr  = ds['cloud_mean'].values   # NaN where no sensor

    corr_wind  = LOSwind - ref_Dop         # Doppler-corrected LOS wind

    # night_of_obs[i] = the observing-night calendar date for observation i
    night_of_obs = np.array([_night_date(t) for t in obs_times])

    for night_date, di in date_to_di.items():
        night_mask = (night_of_obs == night_date)
        if not night_mask.any():
            continue

        n_idx        = np.where(night_mask)[0]
        night_times  = obs_times[n_idx]
        night_dirs   = dirs[n_idx]
        night_corr   = corr_wind[n_idx]
        night_slos   = sigma_los[n_idx]
        night_T      = T_arr[n_idx]
        night_eT     = eT_arr[n_idx]
        night_skyI   = skyI_arr[n_idx]
        night_eI     = eI_arr[n_idx]
        night_ze     = ze_arr[n_idx]
        night_clouds = cloud_arr[n_idx]

        # ── Interpolate zenith vertical wind to all sky times ──────────────
        # Required to project horizontal LOS winds to true horizontal.
        zen_mask = (night_dirs == 'Zenith')
        if zen_mask.sum() <= 1:
            continue   # can't build a reliable reference; skip night

        t0     = night_times[0]
        t_secs = np.array([(t - t0).total_seconds() for t in night_times])
        t_zen  = t_secs[zen_mask]
        w_zen  = night_corr[zen_mask]
        sw_zen = night_slos[zen_mask]

        good_w = np.abs(w_zen) < 200.0
        if good_w.sum() <= 1:
            good_w = np.ones(len(w_zen), dtype=bool)
        if good_w.sum() <= 1:
            continue

        w_interp  = np.interp(t_secs, t_zen[good_w], w_zen[good_w],
                              left=0.0, right=0.0)
        sw_interp = np.interp(t_secs, t_zen[good_w], sw_zen[good_w],
                              left=0.0, right=0.0)

        # Check whether cloud data are usable for this night
        has_clouds_night = not np.all(np.isnan(night_clouds))

        # ── Per-direction accumulation ─────────────────────────────────────
        for direction in np.unique(night_dirs):
            d_mask = (night_dirs == direction)

            # Cloud filter: exclude observations above cloudy_temperature
            if has_clouds_night:
                cloud_ok   = night_clouds[d_mask] < cloudy_temperature
                d_idx_ok   = np.where(d_mask)[0][cloud_ok]
                d_mask_new = np.zeros(len(night_dirs), dtype=bool)
                d_mask_new[d_idx_ok] = True
                d_mask = d_mask_new

            if not d_mask.any():
                continue

            st = night_times[d_mask]
            lt = np.array([_lt_hour(x) for x in st])
            ti = np.digitize(lt, time_edges) - 1

            # ── Zenith: T, I, vertical wind W ─────────────────────────────
            if direction == 'Zenith' and any(
                v in variables for v in ('T', 'I', 'W')
            ):
                T_d     = night_T[d_mask]
                eT_d    = night_eT[d_mask]
                I_d     = night_skyI[d_mask]
                eI_d    = night_eI[d_mask]
                dop_W   = night_corr[d_mask]
                edop_W  = np.sqrt(
                    night_slos[d_mask]**2 + sw_interp[d_mask]**2
                )

                q = (eT_d > 0) & (eT_d < 100)
                if 'T' in variables:
                    _add_weighted(sum_wT, sum_w_T, ti[q], di,
                                  T_d[q], 1.0 / eT_d[q]**2)
                if 'I' in variables:
                    q_I = q & (eI_d > 0)
                    _add_weighted(sum_wI, sum_w_I, ti[q_I], di,
                                  I_d[q_I], 1.0 / eI_d[q_I]**2)
                if 'W' in variables:
                    q_W = q & (edop_W > 0)
                    _add_weighted(sum_wW, sum_w_W, ti[q_W], di,
                                  dop_W[q_W], 1.0 / edop_W[q_W]**2)

            # ── East: zonal wind U ─────────────────────────────────────────
            elif direction == 'East' and 'U' in variables:
                ze_rad = night_ze[d_mask] * np.pi / 180.0
                U  = (
                    (night_corr[d_mask]
                     - w_interp[d_mask] * np.cos(ze_rad))
                    / np.sin(ze_rad)
                )
                eU = np.sqrt(
                    night_slos[d_mask]**2 + sw_interp[d_mask]**2
                )
                q_U = (eU > 0) & (eU < 50)
                _add_weighted(sum_wU, sum_w_U, ti[q_U], di,
                              U[q_U], 1.0 / eU[q_U]**2)

            # ── North: meridional wind V ───────────────────────────────────
            elif direction == 'North' and 'V' in variables:
                ze_rad = night_ze[d_mask] * np.pi / 180.0
                V  = (
                    (night_corr[d_mask]
                     - w_interp[d_mask] * np.cos(ze_rad))
                    / np.sin(ze_rad)
                )
                eV = np.sqrt(
                    night_slos[d_mask]**2 + sw_interp[d_mask]**2
                )
                q_V = (eV > 0) & (eV < 50)
                _add_weighted(sum_wV, sum_w_V, ti[q_V], di,
                              V[q_V], 1.0 / eV[q_V]**2)

            # South and West are loaded by FPIDataHandler but not plotted
            # by DataSummary — they are available in the Dataset for other
            # downstream uses (e.g. four-direction wind fitting).

    with np.errstate(invalid='ignore', divide='ignore'):
        def _wm(swx, sw):
            return np.where(sw > 0, swx / sw, np.nan)
        return {
            'T': _wm(sum_wT, sum_w_T),
            'U': _wm(sum_wU, sum_w_U),
            'V': _wm(sum_wV, sum_w_V),
            'W': _wm(sum_wW, sum_w_W),
            'I': _wm(sum_wI, sum_w_I),
        }


def _add_weighted(sum_wx, sum_w, ti_arr, di, values, weights):
    """
    Accumulate weighted observations into 2-D bin arrays using
    ``numpy.add.at`` (unbuffered, handles duplicate indices correctly).

    Parameters
    ----------
    sum_wx, sum_w : np.ndarray, shape (n_tbins, n_nights)
        Running sums of ``weight * value`` and ``weight``.
    ti_arr : np.ndarray of int
        Time-bin indices for each observation (0-based; out-of-range
        values are silently ignored).
    di : int
        Date-column index.
    values, weights : np.ndarray
        Observed values and inverse-variance weights (1/sigma^2).
    """
    n_tbins = sum_wx.shape[0]
    mask = (
        (ti_arr >= 0) & (ti_arr < n_tbins)
        & np.isfinite(values)
        & np.isfinite(weights)
        & (weights > 0)
    )
    ti_ok = ti_arr[mask]
    np.add.at(sum_wx[:, di], ti_ok, weights[mask] * values[mask])
    np.add.at(sum_w[:, di],  ti_ok, weights[mask])


def _obs_plot_hour(stored_dt, lon_deg, time_axis='LST'):
    """
    Decimal hour in the 17–32 h night convention.

    FPIDataHandler stores times as local wall-clock (tz stripped by
    ``_strip_tz``).  The two display modes therefore work as follows:

    * ``time_axis='LST'`` — stored hours *are* local solar time; apply
      the standard 17–32 h wrapping directly (same as ``_lt_hour``).
    * ``time_axis='UT'`` — convert local → UT via
      ``UT = (local − lon°/15) mod 24``, then wrap to 17–32 h.
      For western sites (lon < 0) this *adds* hours; for eastern sites
      (lon > 0) it subtracts hours.

    Parameters
    ----------
    stored_dt : datetime-like
        Local wall-clock datetime from the Dataset time coordinate.
    lon_deg : float
        Site longitude in degrees east.  Only used when ``time_axis='UT'``.
    time_axis : {'LST', 'UT'}

    Returns
    -------
    float
        17–24 → evening (PM); 24–32 → early morning.
    """
    h = stored_dt.hour + stored_dt.minute / 60.0 + stored_dt.second / 3600.0
    if time_axis == 'LST':
        # Stored times are already local; just apply the night convention.
        if h <= 12.0:
            h += 24.0
    else:  # 'UT'
        # UT = local - lon/15  (lon < 0 for western sites → adds hours)
        h = (h - lon_deg / 15.0) % 24.0
        if h <= 12.0:
            h += 24.0
    return h


def _weighted_bin_stats(values, weights, shade):
    """
    Weighted mean and shade-band limits for a 1-D observation array.

    Parameters
    ----------
    values, weights : np.ndarray
        Observed values and inverse-variance weights (1/σ²).
        NaN entries and non-positive weights are ignored.
    shade : {'std', 'sem', 'percentile'}
        Shading mode.

    Returns
    -------
    mean, lo, hi : float
        Weighted mean and lower/upper shade boundary.
        All NaN when fewer than 2 valid observations are present.
    """
    valid = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
    if valid.sum() < 2:
        return np.nan, np.nan, np.nan
    v = values[valid]
    w = weights[valid]

    if shade == 'percentile':
        return float(np.mean(v)), float(np.percentile(v, 25)), float(np.percentile(v, 75))

    w_sum = float(w.sum())
    wm    = float((w * v).sum()) / w_sum
    var   = float((w * (v - wm) ** 2).sum()) / w_sum
    std   = float(np.sqrt(max(var, 0.0)))

    if shade == 'std':
        return wm, wm - std, wm + std
    # shade == 'sem'
    sem = std / float(np.sqrt(len(v)))
    return wm, wm - sem, wm + sem


def _project_winds(obs_times_ut, obs_dirs, LOS_corr, sigma_los,
                   ze_arr, night_of_obs, directions_requested):
    """
    Project Doppler-corrected LOS winds to physical horizontal/vertical
    components, using per-night zenith-wind interpolation to remove the
    vertical component from the horizontal LOS measurements.

    Sign convention (positive = toward):
        North  →  V  [m/s, positive northward]   (sign +1)
        South  →  V  [m/s, positive northward]   (sign -1, flip for south-look)
        East   →  U  [m/s, positive eastward]    (sign +1)
        West   →  U  [m/s, positive eastward]    (sign -1, flip for west-look)
        Zenith →  W  [m/s, positive upward]

    For a look direction d at zenith angle ze:
        LOS = sign_d * V_horiz * sin(ze) + W * cos(ze)
        → V_horiz = sign_d * (LOS - W_interp * cos(ze)) / sin(ze)

    If a night has ≤1 zenith observations, horizontal wind for that night is NaN
    (cannot build a zenith-wind interpolator).

    Parameters
    ----------
    obs_times_ut : pd.DatetimeIndex
        UT times for each observation (after any time_range filtering).
    obs_dirs : np.ndarray of str
    LOS_corr : np.ndarray
        Doppler-corrected LOS wind (LOSwind − ref_Dop) [m/s].
    sigma_los : np.ndarray
        LOS wind uncertainty [m/s].
    ze_arr : np.ndarray
        Zenith angle [deg].
    night_of_obs : np.ndarray of datetime.date
        Observing-night date for each observation (from _night_date).
    directions_requested : list of str
        Only project directions that are actually requested; others stay NaN.

    Returns
    -------
    proj_wind, sigma_proj : np.ndarray, shape (N,)
    """
    n = len(obs_dirs)
    proj_wind  = np.full(n, np.nan)
    sigma_proj = np.full(n, np.nan)

    unique_nights = np.unique(night_of_obs)

    for night in unique_nights:
        n_mask = (night_of_obs == night)
        n_idx  = np.where(n_mask)[0]

        night_times = obs_times_ut[n_idx]
        night_dirs  = obs_dirs[n_idx]
        night_LOS   = LOS_corr[n_idx]
        night_sig   = sigma_los[n_idx]
        night_ze    = ze_arr[n_idx]

        # ── Zenith: W = corrected LOS (positive upward) ───────────────────
        if 'Zenith' in directions_requested:
            z_loc  = (night_dirs == 'Zenith')
            z_idx  = n_idx[z_loc]
            q_z    = (night_sig[z_loc] > 0) & (night_sig[z_loc] < 200)
            valid_z = z_idx[q_z]
            proj_wind[valid_z]  = night_LOS[z_loc][q_z]
            sigma_proj[valid_z] = night_sig[z_loc][q_z]

        # ── Horizontal directions: need zenith-wind interpolation ─────────
        horiz_dirs = [d for d in directions_requested if d != 'Zenith']
        if not horiz_dirs:
            continue

        zen_mask = (night_dirs == 'Zenith')
        if zen_mask.sum() <= 1:
            # Cannot build a zenith-wind reference; skip horizontal winds
            continue

        t0     = night_times[0]
        t_secs = np.array([(t - t0).total_seconds() for t in night_times],
                          dtype=float)
        t_zen  = t_secs[zen_mask]
        w_zen  = night_LOS[zen_mask]
        sw_zen = night_sig[zen_mask]

        good_w = np.abs(w_zen) < 200.0
        if good_w.sum() <= 1:
            good_w = np.ones(len(w_zen), dtype=bool)
        if good_w.sum() <= 1:
            continue

        w_interp  = np.interp(t_secs, t_zen[good_w], w_zen[good_w],
                              left=0.0, right=0.0)
        sw_interp = np.interp(t_secs, t_zen[good_w], sw_zen[good_w],
                              left=0.0, right=0.0)

        for direction in horiz_dirs:
            # North and East look toward those headings; positive LOS = gas
            # moving away = gas moving north (or east).  South and West look
            # the opposite way, so we flip the sign to keep positive = N/E.
            sign   = -1 if direction in ('South', 'West') else 1
            d_loc  = (night_dirs == direction)
            if not d_loc.any():
                continue

            d_idx  = n_idx[d_loc]
            ze_rad = night_ze[d_loc] * (np.pi / 180.0)
            sin_ze = np.sin(ze_rad)
            cos_ze = np.cos(ze_rad)

            # Quality gates: reasonable uncertainty; not near-vertical pointing
            # (sin ze > 0.05 avoids division by ≈0 for ze < ~3°)
            q = (
                (night_sig[d_loc] > 0)
                & (night_sig[d_loc] < 50)
                & (sin_ze > 0.05)
            )
            if not q.any():
                continue

            w_int  = w_interp[d_loc]
            sw_int = sw_interp[d_loc]

            wind_proj = sign * (night_LOS[d_loc] - w_int * cos_ze) / sin_ze
            sigma     = (
                np.sqrt(night_sig[d_loc] ** 2 + (sw_int * cos_ze) ** 2)
                / sin_ze
            )

            proj_wind[d_idx[q]]  = wind_proj[q]
            sigma_proj[d_idx[q]] = sigma[q]

    return proj_wind, sigma_proj


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def DataSummary(files, times=None,
                t_start=17.0, t_end=32.0,
                Tmin=500, Tmax=1500, Dmin=-200, Dmax=200, Imin=0, Imax=200,
                cloudy_temperature=-15.0, reference='zenith',
                use_cloud_storage=False, cloud_storage=None, temp_dir=None,
                bin_time=None, bin_days=None, mode='publication',
                variables=None):
    """
    Two-dimensional summary of FPI data.

    The y-axis shows local time (hours); the x-axis shows date (night).
    Each night occupies one column unless ``bin_days`` is set.  Returns
    individual ``(Figure, Axes)`` pairs for each requested variable.

    Parameters
    ----------
    files : list of str  **or**  xr.Dataset
        Either a list of paths to ``.npz`` result files (one per night;
        when ``use_cloud_storage=True`` these are cloud-storage object
        keys), **or** an ``xr.Dataset`` returned by
        ``FPIDataHandler.load_fpi_data()``.  When a Dataset is supplied
        the cloud-storage parameters are ignored.
    times : array-like or None
        Night dates that define the x-axis.  Accepts matplotlib date
        numbers (floats), Python ``datetime`` objects, or a
        ``pandas.DatetimeIndex``.  One element per night.  When *files*
        is an ``xr.Dataset`` and *times* is ``None``, the night dates are
        inferred from the Dataset's time coordinate.  *times* is required
        when *files* is a list.
    t_start, t_end : float
        Local-time range to display in decimal hours (default 17–32,
        i.e. 5 PM to 8 AM the following morning).
    Tmin, Tmax : float
        Temperature colour-scale limits [K].  Default 500–1500.
    Dmin, Dmax : float
        Colour-scale limits for zonal and meridional winds [m/s].
        Default ±200.  Vertical wind is always plotted with ±50 m/s.
    Imin, Imax : float
        Intensity colour-scale limits [arb. units].  Default 0–200.
    cloudy_temperature : float
        Cloud-sensor threshold [°C].  Observations whose cloud-sensor
        reading is below this value are discarded.  Default −15.
    reference : {'zenith', 'laser'}
        Doppler reference method passed to ``FPI.DopplerReference``.
    use_cloud_storage : bool
        If ``True``, download each file from cloud storage before
        loading.  Default ``False``.
    cloud_storage : CloudStorage instance or None
        Required when ``use_cloud_storage=True``.
    temp_dir : str or None
        Directory for temporary cloud downloads.  Falls back to
        ``cloud_storage.config.temp_dir`` when ``None``.
    bin_time : int or None
        Time-axis bin size [minutes].  ``None`` → 30-minute bins
        (matching the original default).
    bin_days : int or None
        Number of consecutive nights to average together along the date
        axis.  ``None`` → one column per night.  Uses
        ``xr.Dataset.coarsen`` internally.
    mode : {'publication', 'presentation'}
        Output style.  ``'publication'`` produces compact, print-ready
        figures; ``'presentation'`` uses larger fonts and higher
        contrast for screen projection.
    variables : list of str or None
        Which quantities to plot.  Any subset of
        ``['T', 'U', 'V', 'W', 'I']`` (temperature, zonal wind,
        meridional wind, vertical wind, 630-nm intensity).
        ``None`` → all five.

    Returns
    -------
    dict
        Keys are variable names (from *variables*); values are
        ``(matplotlib.Figure, matplotlib.Axes)`` tuples.
        Example::

            {'T': (fig_T, ax_T), 'U': (fig_U, ax_U), ...}
    """
    # ── Validate inputs ───────────────────────────────────────────────────
    if variables is None:
        variables = ['T', 'U', 'V', 'W', 'I']
    valid_vars = {'T', 'U', 'V', 'W', 'I'}
    bad = set(variables) - valid_vars
    if bad:
        raise ValueError(
            f"Unknown variable(s): {bad}. Choose from {valid_vars}."
        )
    if mode not in ('publication', 'presentation'):
        raise ValueError("mode must be 'publication' or 'presentation'.")
    if use_cloud_storage and cloud_storage is None:
        raise ValueError(
            "cloud_storage is required when use_cloud_storage=True."
        )
    if use_cloud_storage and temp_dir is None:
        temp_dir = cloud_storage.config.temp_dir

    # ── Time-bin edges ────────────────────────────────────────────────────
    dt_min = 30 if bin_time is None else int(bin_time)
    dt_hr  = dt_min / 60.0
    # np.arange can miss the endpoint due to floating-point; force inclusion.
    time_edges = np.arange(t_start, t_end + dt_hr * 0.01, dt_hr)
    if time_edges[-1] < t_end:
        time_edges = np.append(time_edges, t_end)
    time_centers = 0.5 * (time_edges[:-1] + time_edges[1:])
    n_tbins = len(time_centers)

    # ── Night-date index and data accumulation ────────────────────────────
    if isinstance(files, xr.Dataset):
        # ── Dataset path: files is an xr.Dataset from load_fpi_data() ────
        ds_input  = files
        site_name = ds_input.attrs.get('site', '')
        _em       = ds_input.attrs.get('emission', '')
        emission  = 'RL' if _em == 'xr' else ('GL' if _em == 'xg' else _em.upper())

        if times is None:
            # Infer night dates from the dataset's time coordinate.
            # Use a complete daily range from first to last observed night so
            # that gap nights (no data) become NaN columns in the grid.
            # Without this, pcolormesh stretches adjacent data cells across
            # the gap instead of showing the gray missing-data background.
            obs_times     = pd.DatetimeIndex(ds_input.coords['time'].values)
            unique_nights = sorted(set(_night_date(t) for t in obs_times))
            night_dates   = pd.date_range(unique_nights[0], unique_nights[-1],
                                          freq='D')
        else:
            night_dates = _to_pandas_timestamps(times)

        n_nights   = len(night_dates)
        date_to_di = {ts.date(): i for i, ts in enumerate(night_dates)}

        grids = _accumulate_from_dataset(
            ds_input, time_edges, date_to_di,
            n_tbins, n_nights, variables, cloudy_temperature,
        )

    else:
        # ── File-list path (original behaviour, unchanged) ─────────────────
        if times is None:
            raise ValueError(
                "times is required when files is a list of paths."
            )
        night_dates  = _to_pandas_timestamps(times)   # DatetimeIndex, date-only
        n_nights     = len(night_dates)
        # Map each night's calendar date to its column index
        date_to_di   = {ts.date(): i for i, ts in enumerate(night_dates)}

        # sum_wx[ti, di] = Σ (w·x)  in time-bin ti, night di
        # sum_w [ti, di] = Σ  w     in time-bin ti, night di
        def _empty_pair():
            return (np.zeros((n_tbins, n_nights)),
                    np.zeros((n_tbins, n_nights)))

        sum_wT, sum_w_T = _empty_pair()   # temperature
        sum_wU, sum_w_U = _empty_pair()   # zonal wind
        sum_wV, sum_w_V = _empty_pair()   # meridional wind
        sum_wW, sum_w_W = _empty_pair()   # vertical wind
        sum_wI, sum_w_I = _empty_pair()   # 630-nm intensity

        site_name = None
        emission  = None   # 'RL' (redline, _xr) or 'GL' (greenline, _xg)
        files_sorted = sorted(files)

        # ── Main loop: process each file ──────────────────────────────────
        for f in files_sorted:
            local_filename = None
            try:
                # Load file (from disk or cloud storage)
                if use_cloud_storage:
                    local_filename = os.path.join(temp_dir, os.path.basename(f))
                    if not cloud_storage.download_file(f, local_filename):
                        print(f"Warning: failed to download {f}")
                        continue
                    file_to_load = local_filename
                else:
                    file_to_load = f

                npzfile     = np.load(file_to_load, allow_pickle=True, encoding='latin1')
                FPI_Results = npzfile['FPI_Results'].reshape(-1)[0]
                site        = npzfile['site'].reshape(-1)[0]
                npzfile.close()

                # Capture site name and emission type from first successful file.
                # Emission is inferred from the filename suffix (_xr → RL, _xg → GL).
                if site_name is None:
                    site_name = site.get('Abbreviation', '').upper()
                    stem = os.path.splitext(os.path.basename(f))[0].lower()
                    if stem.endswith('_xr'):
                        emission = 'RL'
                    elif stem.endswith('_xg'):
                        emission = 'GL'

                # Identify which date column this file belongs to
                sky_times   = FPI_Results['sky_times']
                first_night = _night_date(sky_times[0])
                di          = date_to_di.get(first_night)
                if di is None:
                    continue

                # Compute Doppler reference (zenith-based or laser-based)
                ref_Dop, _ = FPI.DopplerReference(FPI_Results, reference=reference)

                # Interpolate vertical wind to all sky_times
                ind_z = np.array(FPI.all_indices('Zenith', FPI_Results['direction']))
                if len(ind_z) == 0:
                    continue

                w_raw  = FPI_Results['LOSwind'][ind_z] - ref_Dop[ind_z]
                sw_raw = FPI_Results['sigma_LOSwind'][ind_z]

                t0 = sky_times[0]

                def _to_secs(arr):
                    return np.array([(x - t0).total_seconds() for x in arr])

                t_sec_z = _to_secs(sky_times[ind_z])
                t_sec_all = _to_secs(sky_times)

                good_w = np.abs(w_raw) < 200.0
                if good_w.sum() <= 1:
                    good_w = np.ones(len(w_raw), dtype=bool)
                if good_w.sum() <= 1:
                    continue

                w_interp  = np.interp(t_sec_all, t_sec_z[good_w], w_raw[good_w],
                                      left=0.0, right=0.0)
                sw_interp = np.interp(t_sec_all, t_sec_z[good_w], sw_raw[good_w],
                                      left=0.0, right=0.0)

                # Cloud sensor availability
                has_clouds = (
                    'Clouds' in FPI_Results
                    and FPI_Results['Clouds'] is not None
                )

                # Iterate over each look direction in this file
                for direction in np.unique(FPI_Results['direction']):
                    ind_d = np.array(
                        FPI.all_indices(direction, FPI_Results['direction'])
                    )

                    # Apply cloud filter: keep observations where the cloud sensor
                    # reads below cloudy_temperature (cold = clear sky).
                    if has_clouds:
                        cloud_vals = FPI_Results['Clouds']['mean'][ind_d]
                        ind_d = ind_d[cloud_vals < cloudy_temperature]
                    if len(ind_d) == 0:
                        continue

                    st = sky_times[ind_d]
                    lt = np.array([_lt_hour(x) for x in st])
                    # 0-based time-bin index for each observation
                    ti = np.digitize(lt, time_edges) - 1

                    # ── Zenith: temperature (T), intensity (I), vertical wind (W) ──
                    if direction == 'Zenith' and any(
                        v in variables for v in ('T', 'I', 'W')
                    ):
                        T      = FPI_Results['T'][ind_d]
                        eT     = FPI_Results['sigma_T'][ind_d]
                        I      = FPI_Results['skyI'][ind_d]
                        eI     = FPI_Results['sigma_skyI'][ind_d]
                        dop_W  = FPI_Results['LOSwind'][ind_d] - ref_Dop[ind_d]
                        edop_W = np.sqrt(
                            FPI_Results['sigma_LOSwind'][ind_d]**2
                            + sw_interp[ind_d]**2
                        )

                        # Primary quality gate: temperature uncertainty
                        q = (eT > 0) & (eT < 100)

                        if 'T' in variables:
                            _add_weighted(sum_wT, sum_w_T, ti[q], di,
                                          T[q], 1.0 / eT[q]**2)

                        if 'I' in variables:
                            # Intensity uses the same T quality gate but its own weight
                            q_I = q & (eI > 0)
                            _add_weighted(sum_wI, sum_w_I, ti[q_I], di,
                                          I[q_I], 1.0 / eI[q_I]**2)

                        if 'W' in variables:
                            q_W = q & (edop_W > 0)
                            _add_weighted(sum_wW, sum_w_W, ti[q_W], di,
                                          dop_W[q_W], 1.0 / edop_W[q_W]**2)

                    # ── East: zonal wind (U) ──────────────────────────────
                    elif direction == 'East' and 'U' in variables:
                        ze_rad = FPI_Results['ze'][ind_d] * np.pi / 180.0
                        U  = (
                            (FPI_Results['LOSwind'][ind_d] - ref_Dop[ind_d]
                             - w_interp[ind_d] * np.cos(ze_rad))
                            / np.sin(ze_rad)
                        )
                        eU = np.sqrt(
                            FPI_Results['sigma_LOSwind'][ind_d]**2
                            + sw_interp[ind_d]**2
                        )
                        q_U = (eU > 0) & (eU < 50)
                        _add_weighted(sum_wU, sum_w_U, ti[q_U], di,
                                      U[q_U], 1.0 / eU[q_U]**2)

                    # ── North: meridional wind (V) ────────────────────────
                    elif direction == 'North' and 'V' in variables:
                        ze_rad = FPI_Results['ze'][ind_d] * np.pi / 180.0
                        V  = (
                            (FPI_Results['LOSwind'][ind_d] - ref_Dop[ind_d]
                             - w_interp[ind_d] * np.cos(ze_rad))
                            / np.sin(ze_rad)
                        )
                        eV = np.sqrt(
                            FPI_Results['sigma_LOSwind'][ind_d]**2
                            + sw_interp[ind_d]**2
                        )
                        q_V = (eV > 0) & (eV < 50)
                        _add_weighted(sum_wV, sum_w_V, ti[q_V], di,
                                      V[q_V], 1.0 / eV[q_V]**2)

            except Exception as e:
                print(f"Error processing {f}: {e}")
            finally:
                # Clean up any temporary cloud download
                if use_cloud_storage and local_filename and os.path.exists(local_filename):
                    try:
                        os.remove(local_filename)
                    except Exception:
                        pass

        # ── Compute weighted means ─────────────────────────────────────────
        with np.errstate(invalid='ignore', divide='ignore'):
            def _wmean(swx, sw):
                return np.where(sw > 0, swx / sw, np.nan)

            grids = {
                'T': _wmean(sum_wT, sum_w_T),
                'U': _wmean(sum_wU, sum_w_U),
                'V': _wmean(sum_wV, sum_w_V),
                'W': _wmean(sum_wW, sum_w_W),
                'I': _wmean(sum_wI, sum_w_I),
            }

    # ── Build xarray Dataset ───────────────────────────────────────────────
    # Dimensions: lt_hour (time of night) × date (one per night)
    coords = {
        'lt_hour': time_centers,
        'date':    night_dates.values,   # numpy datetime64
    }
    ds = xr.Dataset(
        {k: xr.DataArray(v, dims=['lt_hour', 'date'], coords=coords)
         for k, v in grids.items()}
    )

    # ── Optional coarsening along the date axis (bin_days) ────────────────
    # xr.Dataset.coarsen averages groups of bin_days consecutive nights.
    # Empty (all-NaN) bins remain NaN (skipna=True is the default).
    if bin_days is not None and bin_days > 1:
        ds = ds.coarsen(date=int(bin_days), boundary='trim').mean()

    # ── Style settings for the requested mode ─────────────────────────────
    rc, cmap_div, cmap_seq = _mode_rcparams(mode)

    # ── Derive figure size from date span ─────────────────────────────────
    # The original function used 5×1.25" (4:1 ratio) for a year of data.
    # Scale width proportionally with the date range; maintain that ratio.
    date_vals = pd.DatetimeIndex(ds.coords['date'].values)
    span_days = max((date_vals[-1] - date_vals[0]).days, 1)
    fig_w = float(np.clip(span_days / 365.0 * 8.0, 5.0, 12.0))
    if mode == 'presentation':
        fig_h = float(np.clip(fig_w / 3.0, 2.0, 4.0))
    else:
        fig_h = float(np.clip(fig_w / 4.0, 1.25, 3.0))

    # ── Title strings ──────────────────────────────────────────────────────
    d0 = date_vals[0].strftime('%Y-%m-%d')
    dN = date_vals[-1].strftime('%Y-%m-%d')
    site_emission   = ' '.join(filter(None, [site_name, emission]))
    title_site      = f"{site_emission}  " if site_emission else ''
    title_daterange = f"{d0} – {dN}"

    # ── x-axis (date) edges for pcolormesh with shading='flat' ─────────────
    # pcolormesh needs n_dates+1 edges for n_dates columns.
    date_mpl = mdates.date2num(date_vals.to_pydatetime())
    if len(date_mpl) > 1:
        gaps    = np.diff(date_mpl)
        x_edges = np.concatenate([
            [date_mpl[0] - gaps[0] / 2],
            date_mpl[:-1] + gaps / 2,
            [date_mpl[-1] + gaps[-1] / 2],
        ])
    else:
        x_edges = np.array([date_mpl[0] - 0.5, date_mpl[0] + 0.5])

    # ── Local-time tick formatter ──────────────────────────────────────────
    def _lt_fmt(x, pos):
        return f'{int(np.mod(x, 24)):02d}'

    # ── x-axis locator / formatter (adaptive to date span) ────────────────
    def _configure_xaxis(ax):
        if span_days > 365:
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
            ax.xaxis.set_minor_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        elif span_days > 60:
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        else:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

    # ── Generic plotting helper ────────────────────────────────────────────
    def _plot_panel(data2d, vmin, vmax, cmap, cbar_label, panel_title):
        """Create one (Figure, Axes) pair for a single variable."""
        with matplotlib.rc_context(rc):
            fig, ax = plt.subplots(figsize=(fig_w, fig_h),
                                   constrained_layout=True)
            # Gray background makes NaN (missing) cells visually distinct
            # from cells that contain a true zero value.
            ax.set_facecolor('#b0b0b0')
            masked = ma.masked_invalid(data2d)
            pcm = ax.pcolormesh(
                x_edges, time_edges, masked,
                vmin=vmin, vmax=vmax, cmap=cmap, shading='flat',
            )
            ax.set_ylim(t_start, t_end)
            ax.yaxis.set_major_formatter(FuncFormatter(_lt_fmt))
            ax.set_ylabel('LT [hr]')
            ax.set_xlim(x_edges[0], x_edges[-1])
            _configure_xaxis(ax)
            cbar = fig.colorbar(
                pcm, ax=ax,
                ticks=[vmin, (vmin + vmax) / 2.0, vmax],
                shrink=0.85, pad=0.02,
            )
            cbar.set_label(cbar_label)
            ax.set_title(
                f"{title_site}{panel_title}\n{title_daterange}"
            )
            return fig, ax

    # ── Per-variable plot configuration ───────────────────────────────────
    var_cfg = {
        #  key: (vmin,   vmax,  cmap,     colorbar label,           panel title)
        'T': (Tmin,   Tmax,  cmap_seq, 'Temperature [K]',         'Neutral Temperature'),
        'U': (Dmin,   Dmax,  cmap_div, 'Zonal Wind [m/s]',        'Zonal Neutral Wind'),
        'V': (Dmin,   Dmax,  cmap_div, 'Meridional Wind [m/s]',   'Meridional Neutral Wind'),
        'W': (-50.0,  50.0,  cmap_div, 'Vertical Wind [m/s]',     'Vertical Neutral Wind'),
        'I': (Imin,   Imax,  cmap_seq, 'Intensity [arb]',         '630.0-nm Intensity'),
    }

    # ── Plot each requested variable and collect results ───────────────────
    results = {}
    for v in variables:
        vmin, vmax, cmap, cbar_lbl, ptitle = var_cfg[v]
        data2d = ds[v].values   # shape (n_tbins, n_dates_final)
        results[v] = _plot_panel(data2d, vmin, vmax, cmap, cbar_lbl, ptitle)

    return results


def MonthlySummaries(
    ds,
    variables,
    directions,
    bin_time=15,
    time_range=None,
    shade='std',
    time_axis='LST',
    show_sample_counts=False,
    mode='publication',
    plot_los=False,
):
    """
    Monthly climatology panels for FPI data.

    Produces one figure per requested variable group (``'wind'``,
    ``'temperature'``, ``'intensity'``), each containing one panel per
    calendar month present in *ds* (or spanned by *time_range*).  Within
    each panel all requested directions are overlaid as coloured lines with
    uncertainty shading, using the colours defined in :data:`DIRECTION_COLORS`.

    Parameters
    ----------
    ds : xr.Dataset
        Tidy dataset from ``FPIDataHandler.load_fpi_data()``.  Times must be
        stored as UT (the Dataset convention set by FPIDataHandler).
    variables : list of str
        Any combination of ``'wind'``, ``'temperature'``, ``'intensity'``.
    directions : list of str
        Any combination of ``'North'``, ``'South'``, ``'East'``, ``'West'``,
        ``'Zenith'``.
    bin_time : int
        Time-bin width in minutes.  Default 15.
    time_range : tuple or None
        ``(start, end)`` as ``datetime.date``, ``datetime.datetime``, or ISO
        string.  Only nights whose evening date falls in this range are used.
    shade : {'std', 'sem', 'percentile'}
        Uncertainty shading.  ``'std'`` → weighted standard deviation;
        ``'sem'`` → weighted standard error of the mean;
        ``'percentile'`` → 25th–75th percentile (unweighted).
    time_axis : {'LST', 'UT'}
        ``'LST'`` (default) converts UT times to Local Solar Time using the
        site longitude from fpiinfo.  ``'UT'`` displays raw UT hours.
    show_sample_counts : bool
        If ``True``, add a secondary y-axis bar chart of total sample count
        per time bin on each panel.
    mode : {'publication', 'presentation'}
        Output style.
    plot_los : bool
        If ``True``, plot raw Doppler-corrected LOS wind instead of the
        default horizontal projection.  Temperature and intensity unaffected.

    Returns
    -------
    dict of str → matplotlib.Figure
        Keys are variable group names requested; e.g.
        ``{'wind': fig1, 'temperature': fig2}``.

    Notes
    -----
    **Wind projection** (``plot_los=False``, the default):
        North / South look directions are projected to the meridional
        component V [m/s, positive northward]; East / West to the zonal
        component U [m/s, positive eastward]; Zenith to the vertical
        component W [m/s, positive upward].  The zenith vertical wind is
        interpolated per night and subtracted before projection, matching
        the treatment in :func:`DataSummary`.

    **LST conversion**:
        LST ≈ UT + longitude/15 h.  Site longitude is looked up via
        ``fpiinfo.get_site_info``.  On failure, falls back to UT with a
        warning.

    **Panel layout**:
        Grid is 3 columns wide (≤2 months → 1 column), with enough rows
        to hold all months chronologically.  Unused grid cells are hidden;
        if exactly one cell is unused it holds the shared legend.
    """
    # ── Validate inputs ───────────────────────────────────────────────────
    valid_vars  = {'wind', 'temperature', 'intensity'}
    valid_dirs  = {'North', 'South', 'East', 'West', 'Zenith'}
    valid_shade = {'std', 'sem', 'percentile'}

    bad_v = set(variables) - valid_vars
    bad_d = set(directions) - valid_dirs
    if bad_v:
        raise ValueError(f"Unknown variable(s): {bad_v}. Choose from {valid_vars}.")
    if bad_d:
        raise ValueError(f"Unknown direction(s): {bad_d}. Choose from {valid_dirs}.")
    if shade not in valid_shade:
        raise ValueError(f"shade must be one of {valid_shade}.")
    if mode not in ('publication', 'presentation'):
        raise ValueError("mode must be 'publication' or 'presentation'.")
    if time_axis not in ('LST', 'UT'):
        raise ValueError("time_axis must be 'LST' or 'UT'.")
    if ds.sizes.get('time', 0) == 0:
        raise ValueError("Dataset is empty (no observations in time dimension).")

    # ── Site longitude (needed only for time_axis='UT' conversion) ────────
    # FPIDataHandler stores local wall-clock times (tz-stripped).
    # For LST display: use stored hours directly — no shift required.
    # For UT display:  UT = local - lon/15; look up longitude here.
    site = ds.attrs.get('site', '')
    lon_deg = 0.0
    if time_axis == 'UT':
        try:
            lon_deg = fpiinfo.get_site_info(site)['Location'][1]
        except Exception:
            warnings.warn(
                f"Could not look up longitude for site '{site}'; "
                "UT times will approximate local time (no longitude correction).",
                stacklevel=2,
            )

    # ── Mode-dependent style ──────────────────────────────────────────────
    rc, _cmap_div, _cmap_seq = _mode_rcparams(mode)
    lw_line = rc.get('lines.linewidth', 1.0)
    # Individual panel size in inches
    panel_w, panel_h = (3.5, 2.5) if mode == 'presentation' else (2.5, 2.0)

    # ── Time-bin edges in the 17–32 h night convention ────────────────────
    t_start, t_end = 17.0, 32.0
    dt_hr      = bin_time / 60.0
    time_edges = np.arange(t_start, t_end + dt_hr * 0.01, dt_hr)
    if time_edges[-1] < t_end:
        time_edges = np.append(time_edges, t_end)
    time_centers = 0.5 * (time_edges[:-1] + time_edges[1:])
    n_tbins = len(time_centers)

    # ── Extract raw arrays from Dataset ──────────────────────────────────
    obs_times_ut = pd.DatetimeIndex(ds.coords['time'].values)
    obs_dirs     = ds.coords['direction'].values.copy()
    LOS          = ds['LOSwind'].values.copy()
    ref_Dop      = ds['ref_Dop'].values.copy()
    sigma_los    = ds['sigma_LOSwind'].values.copy()
    T_arr        = ds['T'].values.copy()
    sigma_T      = ds['sigma_T'].values.copy()
    skyI_arr     = ds['skyI'].values.copy()
    sigma_skyI   = ds['sigma_skyI'].values.copy()
    ze_arr       = ds['ze'].values.copy()
    LOS_corr     = LOS - ref_Dop        # Doppler-corrected LOS wind

    # ── Night assignment ──────────────────────────────────────────────────
    night_of_obs = np.array([_night_date(t) for t in obs_times_ut])
    # Integer key: year*100 + month (e.g. 202303 for March 2023)
    obs_ym = np.array([d.year * 100 + d.month for d in night_of_obs])

    # ── Optional time-range filter ────────────────────────────────────────
    if time_range is not None:
        def _norm(d):
            if isinstance(d, str):
                return datetime.date.fromisoformat(d)
            if isinstance(d, datetime.datetime):
                return d.date()
            return d
        tr_start = _norm(time_range[0])
        tr_end   = _norm(time_range[1])
        keep = np.array([tr_start <= nd <= tr_end for nd in night_of_obs])
        if not keep.any():
            raise ValueError("No observations fall within the specified time_range.")
        obs_times_ut = obs_times_ut[keep]
        obs_dirs     = obs_dirs[keep]
        LOS_corr     = LOS_corr[keep]
        sigma_los    = sigma_los[keep]
        T_arr        = T_arr[keep]
        sigma_T      = sigma_T[keep]
        skyI_arr     = skyI_arr[keep]
        sigma_skyI   = sigma_skyI[keep]
        ze_arr       = ze_arr[keep]
        night_of_obs = night_of_obs[keep]
        obs_ym       = obs_ym[keep]

    # ── Months to include (chronological order) ───────────────────────────
    unique_ym = sorted(set(obs_ym.tolist()))
    n_months  = len(unique_ym)
    ym_label  = {
        ym: datetime.date(ym // 100, ym % 100, 1).strftime('%b %Y')
        for ym in unique_ym
    }

    # ── Per-observation plot hours (17–32 h convention) ───────────────────
    obs_plot_hours = np.array(
        [_obs_plot_hour(t, lon_deg, time_axis) for t in obs_times_ut]
    )

    # ── Quality-gated per-observation values ──────────────────────────────
    # Temperature: sigma_T in (0, 100) K
    T_ok    = (sigma_T > 0) & (sigma_T < 100) & np.isfinite(T_arr)
    obs_T   = np.where(T_ok, T_arr,   np.nan)
    obs_T_w = np.where(T_ok, 1.0 / sigma_T ** 2, np.nan)

    # Intensity: sigma_skyI > 0
    I_ok    = (sigma_skyI > 0) & np.isfinite(skyI_arr)
    obs_I   = np.where(I_ok, skyI_arr,   np.nan)
    obs_I_w = np.where(I_ok, 1.0 / sigma_skyI ** 2, np.nan)

    # Wind
    if 'wind' in variables:
        if plot_los:
            # Raw Doppler-corrected LOS: no projection, no zenith removal
            los_ok    = (sigma_los > 0) & (sigma_los < 200) & np.isfinite(LOS_corr)
            obs_wind   = np.where(los_ok, LOS_corr, np.nan)
            obs_wind_w = np.where(los_ok, 1.0 / sigma_los ** 2, np.nan)
        else:
            proj, sig_proj = _project_winds(
                obs_times_ut, obs_dirs, LOS_corr, sigma_los,
                ze_arr, night_of_obs, directions,
            )
            pw_ok      = np.isfinite(proj) & (sig_proj > 0)
            obs_wind   = np.where(pw_ok, proj,   np.nan)
            obs_wind_w = np.where(pw_ok, 1.0 / sig_proj ** 2, np.nan)
    else:
        obs_wind   = np.full(len(obs_dirs), np.nan)
        obs_wind_w = np.full(len(obs_dirs), np.nan)

    # ── Panel grid layout ─────────────────────────────────────────────────
    # Prefer 3 columns; fall back to 1 column for ≤2 months (spec: "single
    # column for ≤2 months").  nrows determined by ceil(n_months / ncols).
    ncols = 1 if n_months <= 2 else 3
    nrows = math.ceil(n_months / ncols)

    # ── X-axis tick locations (major every 2 h) ───────────────────────────
    xtick_locs = np.arange(
        np.ceil(t_start / 2) * 2,
        t_end + 0.01,
        2.0,
    )
    xtick_locs = xtick_locs[(xtick_locs >= t_start) & (xtick_locs <= t_end)]

    def _xt_label(h):
        return f'{int(h % 24):02d}:00'

    xlabel_str = 'LST' if time_axis == 'LST' else 'UT'

    # ── Per-variable-group figure creation ───────────────────────────────
    var_cfg = {
        # group: (obs_values,  obs_weights, y-axis label)
        'wind':        (obs_wind,  obs_wind_w,  'LOS Wind [m/s]' if plot_los else 'Wind [m/s]'),
        'temperature': (obs_T,     obs_T_w,     'Temperature [K]'),
        'intensity':   (obs_I,     obs_I_w,     'Intensity [arb]'),
    }

    # Emission label for figure title
    em_raw = ds.attrs.get('emission', '')
    em_str = 'RL' if em_raw == 'xr' else ('GL' if em_raw == 'xg' else em_raw.upper())
    date_range_attr = ds.attrs.get('date_range', '')

    results = {}

    for var_group in variables:
        obs_vals, obs_wts, ylabel = var_cfg[var_group]

        # ── Bin statistics per (month, direction) ─────────────────────────
        # stats[(ym, direction)] = {'mean', 'lo', 'hi', 'n'} arrays of len n_tbins
        # or None when no observations exist.
        stats = {}
        for ym in unique_ym:
            for direction in directions:
                mask = (obs_ym == ym) & (obs_dirs == direction)
                if not mask.any():
                    stats[(ym, direction)] = None
                    continue

                ph  = obs_plot_hours[mask]
                val = obs_vals[mask]
                wt  = obs_wts[mask]
                ti  = np.digitize(ph, time_edges) - 1   # 0-based bin index

                mean_arr = np.full(n_tbins, np.nan)
                lo_arr   = np.full(n_tbins, np.nan)
                hi_arr   = np.full(n_tbins, np.nan)
                n_arr    = np.zeros(n_tbins, dtype=int)

                for b in range(n_tbins):
                    b_mask  = (ti == b)
                    n_valid = int(np.sum(b_mask & np.isfinite(val)))
                    n_arr[b] = n_valid
                    if n_valid >= 2:
                        mean_arr[b], lo_arr[b], hi_arr[b] = _weighted_bin_stats(
                            val[b_mask], wt[b_mask], shade
                        )

                stats[(ym, direction)] = {
                    'mean': mean_arr, 'lo': lo_arr,
                    'hi': hi_arr,     'n':  n_arr,
                }

        # ── Shared y-axis limits (5 % padding) ───────────────────────────
        all_lo, all_hi = [], []
        for st in stats.values():
            if st is None:
                continue
            all_lo.extend(st['lo'][np.isfinite(st['lo'])].tolist())
            all_hi.extend(st['hi'][np.isfinite(st['hi'])].tolist())

        if not all_lo:
            # No data at all for this variable group
            with matplotlib.rc_context(rc):
                fig, ax = plt.subplots(figsize=(panel_w, panel_h),
                                       constrained_layout=True)
                ax.text(0.5, 0.5, 'No data', transform=ax.transAxes,
                        ha='center', va='center')
                ax.set_axis_off()
            results[var_group] = fig
            continue

        data_lo = min(all_lo)
        data_hi = max(all_hi)
        pad     = 0.05 * (data_hi - data_lo) if data_hi > data_lo else 1.0
        ylim_lo = data_lo - pad
        ylim_hi = data_hi + pad

        # ── Build figure ──────────────────────────────────────────────────
        fig_w = ncols * panel_w
        fig_h = nrows * panel_h

        with matplotlib.rc_context(rc):
            fig, axes = plt.subplots(
                nrows, ncols,
                figsize=(fig_w, fig_h),
                sharex=True,
                sharey=True,
                constrained_layout=True,
                squeeze=False,
            )
            axes_flat = axes.flatten()

            # Hide unused panels (may be repurposed for legend below)
            n_cells  = nrows * ncols
            n_unused = n_cells - n_months
            for ax in axes_flat[n_months:]:
                ax.set_visible(False)

            # Shared axis limits (applied via the first axes; sharey/x propagate)
            axes_flat[0].set_xlim(t_start, t_end)
            axes_flat[0].set_ylim(ylim_lo, ylim_hi)
            axes_flat[0].set_xticks(xtick_locs)
            axes_flat[0].xaxis.set_major_formatter(
                FuncFormatter(lambda h, _: _xt_label(h))
            )

            legend_handles = []
            legend_labels  = []

            for mi, ym in enumerate(unique_ym):
                ax = axes_flat[mi]

                # Optional secondary y-axis for sample counts
                if show_sample_counts:
                    ax2 = ax.twinx()
                    ax2.set_ylabel(
                        'N samples',
                        fontsize=rc.get('ytick.labelsize', 8),
                        color='gray',
                    )
                    ax2.tick_params(axis='y', colors='gray', labelsize=rc.get('ytick.labelsize', 8))
                    n_total = np.zeros(n_tbins, dtype=int)
                    for direction in directions:
                        st = stats.get((ym, direction))
                        if st is not None:
                            n_total += st['n']
                    # Draw bars behind data lines
                    ax2.bar(
                        time_centers, n_total,
                        width=dt_hr * 0.8,
                        color='gray', alpha=0.3,
                        align='center', zorder=0,
                    )
                    # Keep secondary axis from perturbing primary autoscale
                    ax2.set_ylim(bottom=0)

                any_data = False
                for direction in directions:
                    color = DIRECTION_COLORS.get(direction, '#333333')
                    st    = stats.get((ym, direction))
                    if st is None or not np.any(np.isfinite(st['mean'])):
                        continue
                    any_data = True

                    valid = np.isfinite(st['mean'])
                    line, = ax.plot(
                        time_centers[valid], st['mean'][valid],
                        color=color, linewidth=lw_line, label=direction,
                    )

                    band = valid & np.isfinite(st['lo']) & np.isfinite(st['hi'])
                    if band.any():
                        ax.fill_between(
                            time_centers[band],
                            st['lo'][band], st['hi'][band],
                            color=color, alpha=_SHADE_ALPHA,
                        )

                    if direction not in legend_labels:
                        legend_handles.append(line)
                        legend_labels.append(direction)

                if not any_data:
                    ax.text(
                        0.5, 0.5, 'No data',
                        transform=ax.transAxes,
                        ha='center', va='center',
                        fontsize=rc.get('font.size', 9),
                        color='gray',
                    )

                ax.set_title(ym_label[ym], fontsize=rc.get('axes.titlesize', 10))

            # ── Axis labels on border panels only ─────────────────────────
            # y-labels on left column
            for row in range(nrows):
                idx = row * ncols
                if idx < n_months:
                    axes_flat[idx].set_ylabel(
                        ylabel, fontsize=rc.get('axes.labelsize', 9)
                    )
            # x-labels and rotated tick-labels on bottom-visible row
            bottom_indices = []
            for col in range(ncols):
                # Walk up from the last row to find the bottom visible panel
                for row in range(nrows - 1, -1, -1):
                    idx = row * ncols + col
                    if idx < n_months:
                        bottom_indices.append(idx)
                        break
            for idx in bottom_indices:
                axes_flat[idx].set_xlabel(
                    xlabel_str, fontsize=rc.get('axes.labelsize', 9)
                )
                plt.setp(
                    axes_flat[idx].get_xticklabels(),
                    rotation=45, ha='right',
                )

            # ── Shared legend ─────────────────────────────────────────────
            if show_sample_counts:
                import matplotlib.patches as mpatches
                legend_handles.append(
                    mpatches.Patch(color='gray', alpha=0.3)
                )
                legend_labels.append('N samples')

            if legend_handles:
                if n_unused > 0:
                    # Repurpose the first unused panel as the legend host
                    leg_ax = axes_flat[n_months]
                    leg_ax.set_visible(True)
                    leg_ax.axis('off')
                    leg_ax.legend(
                        legend_handles, legend_labels,
                        loc='center',
                        fontsize=rc.get('font.size', 9),
                        frameon=True,
                    )
                else:
                    # No unused panel: place legend below the grid
                    fig.legend(
                        legend_handles, legend_labels,
                        loc='lower center',
                        ncol=len(legend_handles),
                        fontsize=rc.get('font.size', 9),
                        bbox_to_anchor=(0.5, -0.02),
                        frameon=True,
                    )

            # ── Figure title ──────────────────────────────────────────────
            title_parts = [
                p for p in [site.upper(), em_str,
                             var_group.capitalize(), date_range_attr]
                if p
            ]
            fig.suptitle(
                '  |  '.join(title_parts),
                fontsize=rc.get('axes.titlesize', 10),
            )

        results[var_group] = fig

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Demo / smoke-test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import glob
    import sys

    # ── Configuration — edit these paths to match your data ───────────────
    NPZ_DIR = '/rdata/airglow/fpi/results'
    SITE    = 'ann'
    YEAR    = 2023

    pattern = os.path.join(NPZ_DIR, f'*_{SITE}_{YEAR}*.npz')
    files   = sorted(glob.glob(pattern))

    if not files:
        print(f"No files matched: {pattern}")
        print("Edit NPZ_DIR, SITE, YEAR at the top of the __main__ block.")
        sys.exit(1)

    # Build a daily times array covering the full year
    start  = datetime.datetime(YEAR, 1, 1)
    times  = [start + datetime.timedelta(days=i) for i in range(366)]

    # ── Example 1: all variables, no extra binning, publication mode ───────
    print("Example 1: publication, 30-min bins (default), all variables …")
    results = DataSummary(files, times, mode='publication')
    for var, (fig, ax) in results.items():
        fname = f'datasummary_{SITE}_{YEAR}_{var}_pub.png'
        fig.savefig(fname, dpi=150)
        print(f"  saved {fname}")
    plt.close('all')

    # ── Example 2: 60-min time bins, 7-day date bins, presentation mode ───
    print("Example 2: presentation, 60-min time bins, 7-day date bins …")
    results = DataSummary(files, times,
                          bin_time=60, bin_days=7,
                          mode='presentation')
    for var, (fig, ax) in results.items():
        fname = f'datasummary_{SITE}_{YEAR}_{var}_pres_binned.png'
        fig.savefig(fname, dpi=100)
        print(f"  saved {fname}")
    plt.close('all')

    # ── Example 3: winds only, publication mode ────────────────────────────
    print("Example 3: publication, winds only (U, V, W) …")
    results = DataSummary(files, times,
                          variables=['U', 'V', 'W'],
                          mode='publication')
    for var, (fig, ax) in results.items():
        fname = f'datasummary_{SITE}_{YEAR}_{var}_pub_winds.png'
        fig.savefig(fname, dpi=150)
        print(f"  saved {fname}")
    plt.close('all')

    print("Done.")
