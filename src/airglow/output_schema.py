"""
output_schema.py
-----------------
Assembles `final_estimation.py`'s per-wave output into the permanent output
schema and (via `run_airglow_pipeline`) writes it to disk. Two tables:

    wave_frames.csv -- one row per (wave_id, time), every frame, whether
        valid or not. `final_estimation.estimate_wave_final`'s own per-frame
        table (episode_id, valid, final_*, lobe_*) plus raw and
        causal-tracker context columns attached by the caller (see
        `build_wave_frame_rows`) -- not recomputed here. `tuning_*` columns
        are the causal, real-time Kalman filter's diagnostic output
        (`phase_speed_kalman.analyze_all_waves_kalman`, kept for legacy
        comparison) and must never be treated as authoritative; `final_*`
        is the only authoritative column family.

    wave_hourly.csv -- hourly aggregate, built ONLY from `final_*` columns
        (`build_wave_hourly_table`) -- deliberately not mixing any
        first-pass or causal-tracker-derived values into an otherwise
        final-estimation-only aggregate.

Not built here, by design: a `raw_omega_signed_2nd` column --
`final_estimation._smooth_episode` computes this quantity internally
(`episode_sign * np.abs(np.angle(A)) / dt`) but does not currently expose
it in `estimate_wave_final`'s return, so there is nothing for this module
to attach; exposing it would be a `final_estimation.py` change. A
suppressed-waves table is also not built here -- `kf_merge_log.csv`
(`phase_speed_kalman.build_kf_merge_log`) already covers equivalent ground
under a different name and is unaffected by this module.
"""

import numpy as np
import pandas as pd

from airglow.angles import circular_mean_full_deg, full_circle_diff_deg


WAVE_FRAMES_COLUMNS = [
    "wave_id", "time", "frame_idx", "episode_id", "valid",
    "raw_amplitude_1st", "raw_amplitude_2nd", "raw_kx_2nd", "raw_ky_2nd",
    "tuning_orient_deg", "tuning_wavelength_km", "tuning_wavespeed_ms",
    "final_azimuth_deg", "final_wavelength_km", "final_speed_ms",
    "final_azimuth_std_deg", "final_wavelength_std_km", "final_speed_std_ms",
    "lobe_sign_frac", "lobe_disagreement", "lobe_majority_frac",
    "collision_id", "collision_primary",
]


def build_wave_frame_rows(
    wave_id, est_df: pd.DataFrame, *,
    raw_amplitude_1st, raw_amplitude_2nd, raw_kx_2nd, raw_ky_2nd,
    tuning_orient_deg, tuning_wavelength_km, tuning_wavespeed_ms,
    collision_id, collision_primary,
) -> pd.DataFrame:
    """
    One wave's full `wave_frames.csv` row set: `estimate_wave_final`'s own
    per-frame table plus this pass's raw/tuning/collision context columns.

    All keyword arrays must already be aligned to the SAME full night grid
    `est_df` covers (one value per frame, `est_df`'s own row order) --
    alignment is the caller's responsibility (e.g. a timestamp-based
    `searchsorted` when the source is a per-wave, edge-trimmed causal-KF
    slice, as `run_airglow_pipeline` does for the `tuning_*` columns). No
    re-alignment is performed here.
    """
    df = est_df.copy()
    df.insert(0, "wave_id", wave_id)
    df["raw_amplitude_1st"] = raw_amplitude_1st
    df["raw_amplitude_2nd"] = raw_amplitude_2nd
    df["raw_kx_2nd"] = raw_kx_2nd
    df["raw_ky_2nd"] = raw_ky_2nd
    df["tuning_orient_deg"] = tuning_orient_deg
    df["tuning_wavelength_km"] = tuning_wavelength_km
    df["tuning_wavespeed_ms"] = tuning_wavespeed_ms
    df["collision_id"] = collision_id
    df["collision_primary"] = collision_primary
    return df[WAVE_FRAMES_COLUMNS]


def build_wave_frames_table(per_wave_rows: list) -> pd.DataFrame:
    """Concatenate one `wave_frames.csv` row set per wave (`build_wave_frame_rows`'s
    own output) into the single permanent per-frame table."""
    if not per_wave_rows:
        return pd.DataFrame(columns=WAVE_FRAMES_COLUMNS)
    return pd.concat(per_wave_rows, ignore_index=True)


def build_wave_hourly_table(
    wave_frames: pd.DataFrame, bin_width_min: int = 60, min_valid_frames: int = 5,
) -> pd.DataFrame:
    """
    Hourly aggregate, built ONLY from `wave_frames.csv`'s `final_*` columns
    -- deliberately not mixing in any first-pass or causal-tracker-derived
    values. Amplitude weighting uses `raw_amplitude_2nd` (this pass's own
    signal amplitude), matching `unified_validity_mask`'s convention of
    each pass using its own amplitude rather than a first-pass proxy.

    Bins are whole-hour-aligned (fixed-epoch construction, matching
    `phase_speed_kalman.compute_binned_wave_stats`), covering
    `[wave_frames['time'].min(), wave_frames['time'].max()]`. A bin with
    fewer than `min_valid_frames` valid frames reports NaN aggregates
    (still keeping `n_frames_total`/`n_frames_valid`/`duty_cycle`).

    **A real, physically genuine wave can show a large `azimuth_deg_std`,
    or adjacent hourly bins that differ by ~180 deg, within one `wave_id`**:
    `final_estimation.py`'s lobe resolution keeps each episode's own
    independently-resolved direction by default (see that module's own
    docstring and `SIGN_FRAC_DECISIVE_THRESHOLD`), rather than forcing
    every episode of a wave to agree. A discontinuity here can be
    `final_estimation.py` correctly reporting a genuine mid-life direction
    reversal -- it is not automatically a data-quality problem and should
    not be mistaken for one when reading this table.

    Returns
    -------
    pd.DataFrame with columns: wave_id, bin_start, bin_end,
    n_frames_total, n_frames_valid, duty_cycle,
    azimuth_deg_mean, azimuth_deg_std,
    wavelength_km_mean, wavelength_km_std,
    speed_ms_mean, speed_ms_std.
    """
    if len(wave_frames) == 0:
        return pd.DataFrame()

    def _weighted_mean_std(values, weights):
        ok = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
        if not np.any(ok):
            return np.nan, np.nan
        v, w = values[ok], weights[ok]
        wsum = w.sum()
        wmean = float(np.sum(w * v) / wsum)
        if len(v) < 2:
            return wmean, np.nan
        wstd = float(np.sqrt(np.sum(w * (v - wmean) ** 2) / wsum))
        return wmean, wstd

    def _weighted_circular_mean_std(azimuth_deg, weights):
        ok = np.isfinite(azimuth_deg) & np.isfinite(weights) & (weights > 0)
        if not np.any(ok):
            return np.nan, np.nan
        v, w = azimuth_deg[ok], weights[ok]
        wmean = circular_mean_full_deg(v, weights=w)
        if len(v) < 2:
            return wmean, np.nan
        d = full_circle_diff_deg(v, wmean)
        wstd = float(np.sqrt(np.sum(w * d ** 2) / w.sum()))
        return wmean, wstd

    bin_width = pd.Timedelta(minutes=bin_width_min)
    wf = wave_frames.copy()
    wf["time_pd"] = pd.to_datetime(wf["time"])

    t_min, t_max = wf["time_pd"].min(), wf["time_pd"].max()
    epoch = pd.Timestamp("2000-01-01")
    n_bins_offset = int((t_min - epoch) / bin_width)
    bin_origin = epoch + n_bins_offset * bin_width

    bin_starts = []
    t = bin_origin
    while t <= t_max:
        bin_starts.append(t)
        t += bin_width

    rows = []
    for wave_id in sorted(wf["wave_id"].unique()):
        wdf = wf[wf["wave_id"] == wave_id]

        for bin_start in bin_starts:
            bin_end = bin_start + bin_width
            mask = (wdf["time_pd"] >= bin_start) & (wdf["time_pd"] < bin_end)
            bin_frames = wdf[mask]
            n_total = len(bin_frames)
            if n_total == 0:
                continue

            valid_frames = bin_frames[bin_frames["valid"]]
            n_valid = len(valid_frames)
            duty_cycle = n_valid / n_total

            if n_valid < min_valid_frames:
                rows.append(dict(
                    wave_id=wave_id, bin_start=bin_start, bin_end=bin_end,
                    n_frames_total=n_total, n_frames_valid=n_valid, duty_cycle=duty_cycle,
                    azimuth_deg_mean=np.nan, azimuth_deg_std=np.nan,
                    wavelength_km_mean=np.nan, wavelength_km_std=np.nan,
                    speed_ms_mean=np.nan, speed_ms_std=np.nan,
                ))
                continue

            w = valid_frames["raw_amplitude_2nd"].to_numpy(dtype=float)
            az_mean, az_std = _weighted_circular_mean_std(
                valid_frames["final_azimuth_deg"].to_numpy(dtype=float), w)
            wl_mean, wl_std = _weighted_mean_std(
                valid_frames["final_wavelength_km"].to_numpy(dtype=float), w)
            sp_mean, sp_std = _weighted_mean_std(
                valid_frames["final_speed_ms"].to_numpy(dtype=float), w)

            rows.append(dict(
                wave_id=wave_id, bin_start=bin_start, bin_end=bin_end,
                n_frames_total=n_total, n_frames_valid=n_valid, duty_cycle=duty_cycle,
                azimuth_deg_mean=az_mean, azimuth_deg_std=az_std,
                wavelength_km_mean=wl_mean, wavelength_km_std=wl_std,
                speed_ms_mean=sp_mean, speed_ms_std=sp_std,
            ))

    return pd.DataFrame(rows)
