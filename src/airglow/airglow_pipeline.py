"""
airglow_pipeline.py
===================
``run_airglow_pipeline()`` — end-to-end batch driver for real airglow data.

Covers the complete analysis chain from a single HDF5 file to outputs:

    1. Data loading & preprocessing       (_load_real_data — fill in your loader)
    2. Sliding-window omega-k analysis    (parallel across windows)
    3. Wave family grouping               (build_wave_families)
    4. Per-frame parameter interpolation  (build_waves_for_gabor)
    5. Adaptive Gabor reconstruction      (reconstruct_all_waves)
    6. Omega-k cross-validation           (Pearson r per frame)
    7. Kalman tracking (speed / orientation / wavelength)
    8. Plots: family timeseries, ownership, KF timeseries, correlation, scatter
    9. Animation (parallel frame rendering → ffmpeg mp4)

Typical usage — multi-night loop:

    from airglow_pipeline import run_airglow_pipeline
    from datetime import datetime

    nights = [
        ("/data/2024_152.hdf5", datetime(2024, 5, 31)),
        ("/data/2024_153.hdf5", datetime(2024, 6,  1)),
    ]
    results = []
    for path, date in nights:
        r = run_airglow_pipeline(path, "outputs/", date=date, n_workers=8)
        results.append(r)

The function returns a dict of all computed products so any additional
analysis can be done without re-running the pipeline.
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import subprocess
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from scipy.stats import pearsonr

# ── Pipeline modules ──────────────────────────────────────────────────────────
from OmegaKFilter       import OmegaKFilterXarray
from OmegaK_enhanced    import analyze_and_plot_waves
from OmegaK_enhanced    import circular_mean as _cmean, circular_std as _cstd
from wave_clustering    import compute_adaptive_gabor_parameters
from phase_speed_kalman import (
    analyze_all_waves_kalman,
    compute_binned_wave_stats,
    circular_diff_deg,
)
from wave_validation import (
    build_wave_families,
    families_to_xarray,
    plot_wave_family_timeseries,
)
from gabor_reconstruction import reconstruct_all_waves, build_waves_for_gabor

warnings.filterwarnings("ignore", category=RuntimeWarning)

__all__ = ["run_airglow_pipeline"]


# =============================================================================
# Real-data loader (STUB — fill in with your HDF5 reader)
# =============================================================================

def _load_real_data(
    hdf5_path: str,
    site: str,
    date: datetime | None,
    moon_elevation_max: float,
    ntaps: int,
    t_lo_min: float,
    t_hi_min: float,
    elevation_min: float,
    verbose: bool,
) -> tuple[xr.Dataset, str, float]:
    """
    Load and preprocess one night of real airglow data from HDF5.

    Parameters
    ----------
    hdf5_path          : path to the HDF5 file
    site               : site identifier (e.g. ``"blo"``)
    date               : observation date (passed to loader, may be None)
    moon_elevation_max : reject frames where moon elevation > this (degrees)
    ntaps              : FIR filter tap count for temporal preprocessing
    t_lo_min           : FIR passband lower period limit (minutes)
    t_hi_min           : FIR passband upper period limit (minutes)
    elevation_min      : minimum CCD elevation for FOV mask (degrees)
    verbose            : if True, print loader diagnostics

    Returns
    -------
    ds_full : xr.Dataset
        Preprocessed dataset with dimensions (time, north, east).
        • ``time``  — np.datetime64 coordinate
        • ``north`` — coordinate in **km** (positive northward)
        • ``east``  — coordinate in **km** (positive eastward)
        • image variable (name returned as ``data_var``)
        Spatial mean should already be removed per frame.  FIR edge
        artifacts will be trimmed automatically (``ntaps // 2`` frames).
    data_var : str
        Name of the image variable in ``ds_full`` (e.g. ``"ImageData"``).
    dx_km : float
        Isotropic pixel spacing in km.

    Returns
    -------
    ds_full : xr.Dataset
        Preprocessed dataset with dimensions (time, north, east).
        ``FilteredImageData`` has the FIR bandpass applied and spatial mean
        removed per frame.  Trimming of FIR edge artefacts (``ntaps // 2``
        frames from each end) is done downstream before the Kalman step.
    data_var : str
        Always ``"FilteredImageData"``.
    dx_km : float
        Mean isotropic pixel spacing in km.
    """
    import Karthik_Test_Mango_L2_1 as MANGO_L2
    import airglow.prepare_agimages as prepare_agimages

    # ── Load HDF5 ─────────────────────────────────────────────────────────────
    ds = MANGO_L2.load_hdf5_to_xarray(hdf5_path)
    times_pd = pd.to_datetime(ds.time.values)

    # ── FIR bandpass filter (operates on (north, east, time) layout) ──────────
    im3d = np.transpose(ds["ImageData"].values, (1, 2, 0))   # → (ny, nx, n_time)
    b    = prepare_agimages.initialize_airglow_filter(ntaps, t_lo_min, t_hi_min, times_pd)
    im3d_filt = prepare_agimages.filter_airglow(im3d, b, ntaps)
    ds["FilteredImageData"] = xr.DataArray(
        np.transpose(im3d_filt, (2, 0, 1)),                  # → (n_time, ny, nx)
        dims=("time", "north", "east"),
        coords={"time": ds.time, "north": ds.north, "east": ds.east},
    )

    # ── Moon-elevation mask ───────────────────────────────────────────────────
    if site and date is not None and moon_elevation_max is not None:
        moon_df = MANGO_L2.load_cloud_and_moon(
            {"site": site, "date": date}, times_pd
        )
        ds["moon_angle"] = xr.DataArray(
            moon_df["Moon Altitude"].values,
            dims=("time",),
            coords={"time": ds.time},
        )
        keep = ds["moon_angle"].values <= moon_elevation_max
        ds   = ds.isel(time=keep)

    # ── FOV / elevation crop ──────────────────────────────────────────────────
    if "Elevation" in ds.data_vars:
        mask  = ds.Elevation > elevation_min
        min_r = np.min([
            np.abs(ds.east.where(mask)).max().values,
            np.abs(ds.north.where(mask)).max().values,
        ])
        r  = np.floor(np.sqrt(2) * min_r / 2.0)
        ds = ds.sel(east=slice(-r, r), north=slice(-r * 0.95, r * 0.95))

    # ── Spatial-mean removal ──────────────────────────────────────────────────
    ds["FilteredImageData"] = (
        ds["FilteredImageData"]
        - ds["FilteredImageData"].mean(dim=["east", "north"])
    )

    # ── Derived scalars ───────────────────────────────────────────────────────
    east_km = ds.east.values
    north_km = ds.north.values
    dx_km = float(np.mean([
        np.abs(np.diff(east_km)).mean(),
        np.abs(np.diff(north_km)).mean(),
    ]))

    if verbose:
        print(f"  Dataset loaded: {dict(ds.dims)}")
        print(f"  dx_km = {dx_km:.2f} km")

    return ds, "FilteredImageData", dx_km


# =============================================================================
# Module-level workers (must be top-level for ProcessPoolExecutor pickling)
# =============================================================================

def _window_worker(args: dict) -> dict:
    """
    Process one sliding-window omega-k analysis.
    Reconstructs a local xr.Dataset from the numpy slice and runs
    the full omega-k pipeline.  Must be module-level for pickling.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as _plt
    import xarray as _xr
    import io as _io
    import contextlib as _ctx
    from OmegaKFilter    import OmegaKFilterXarray as _OKF
    from OmegaK_enhanced import analyze_and_plot_waves as _apw

    data_arr        = args["data_arr"]
    time_arr        = args["time_arr"]
    east_km         = args["east_km"]
    north_km        = args["north_km"]
    center_idx      = args["center_idx"]
    center_time     = args["center_time"]
    data_var        = args["data_var"]
    zp              = args["zero_pad_factor"]
    filter_kwargs   = args["filter_kwargs"]
    analysis_kwargs = args["analysis_kwargs"]
    save_prefix     = args["save_prefix"]
    verbose         = args["verbose"]

    ds_w = _xr.Dataset(
        {data_var: (["time", "north", "east"], data_arr)},
        coords={"time": time_arr, "north": north_km, "east": east_km},
    )

    sink = _io.StringIO()
    ctx  = _ctx.redirect_stdout(sink) if not verbose else _ctx.nullcontext()
    with ctx:
        ok   = _OKF(ds_w, data_var=data_var, zero_pad_factor=zp)
        mask = ok.create_filter(**filter_kwargs)
        fig, wave_params, _, physical_waves = _apw(
            ok_filter=ok, filtered_ds=ds_w, filter_mask=mask,
            save_prefix=save_prefix, **analysis_kwargs,
        )
        _plt.close(fig)

    return {
        "center_index":  center_idx,
        "center_time":   center_time,
        "wave_params":   wave_params,
        "physical_waves": physical_waves,
    }


# Module-level global for animation frame shared data (set by initializer)
_ANIM_SHARED: dict | None = None


def _anim_initializer(shared: dict) -> None:
    """Store panel data once per worker process to avoid re-pickling per frame."""
    global _ANIM_SHARED
    _ANIM_SHARED = shared


def _frame_worker(t: int) -> None:
    """
    Render one animation frame to disk.
    Reads shared panel arrays from the module-level _ANIM_SHARED dict that
    was populated by _anim_initializer() — no large data transfer per call.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as _plt
    import numpy as _np

    global _ANIM_SHARED
    d          = _ANIM_SHARED
    panels     = d["panels"]        # list of (cube, title), cubes are (n_time, ny, nx)
    vmax       = d["vmax_common"]
    n_rows     = d["n_rows"]
    n_cols     = d["n_cols"]
    figsize    = d["figsize"]
    dpi        = d["dpi"]
    frames_dir = d["frames_dir"]
    time_lbl   = d["time_labels"][t]
    n_time     = d["n_time"]

    fig, axes = _plt.subplots(n_rows, n_cols, figsize=figsize)
    axes = _np.array(axes).reshape(n_rows, n_cols)

    n_static   = d.get("n_static_panels", len(panels))
    kf_az      = d.get("kf_az_series",    None)   # list of (n_time,) or None
    kf_lam     = d.get("kf_lam_series",   None)
    kf_spd     = d.get("kf_spd_series",   None)
    kf_valid   = d.get("kf_valid_series", None)

    for idx, (cube, base_title) in enumerate(panels):
        r, c = divmod(idx, n_cols)
        ax   = axes[r, c]
        im   = ax.imshow(
            cube[t], origin="lower", cmap="RdBu_r",
            vmin=-vmax, vmax=vmax, aspect="equal",
        )
        _plt.colorbar(im, ax=ax, shrink=0.85)

        # Build per-frame title for wave panels; static title for overview panels
        if (idx >= n_static and kf_az is not None):
            wi    = idx - n_static
            az    = float(kf_az[wi][t])
            lam   = float(kf_lam[wi][t])
            spd   = abs(float(kf_spd[wi][t]))
            valid = bool(kf_valid[wi][t])
            if _np.isnan(az) or _np.isnan(lam) or _np.isnan(spd):
                line2 = f"az={'---':>5s}°  \u03bb={'---':>5s} km  c={'---':>5s} m/s  "
            else:
                flag  = "  " if valid else " *"
                line2 = f"az={az:5.1f}°  \u03bb={lam:5.1f} km  c={spd:5.1f} m/s{flag}"
            title = f"Wave {wi + 1}\n{line2}"
        else:
            title = base_title

        ax.set_title(title, fontsize=8, family="monospace")
        ax.set_xlabel("East (px)", fontsize=7)
        ax.set_ylabel("North (px)", fontsize=7)
        ax.tick_params(labelsize=6)

    for idx in range(len(panels), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes[r, c].set_visible(False)

    fig.suptitle(f"Frame {t:03d}/{n_time}  —  {time_lbl}", fontsize=10)
    fig.tight_layout()
    fig.savefig(
        os.path.join(frames_dir, f"frame_{t:04d}.png"),
        dpi=dpi, bbox_inches="tight",
    )
    _plt.close(fig)


# =============================================================================
# Internal plot helpers
# =============================================================================

def _plot_kf_timeseries(
    kf_results: pd.DataFrame,
    waves_detected: list,
    time_kf: np.ndarray,
    out_path: str,
    verbose: bool,
) -> None:
    """
    Five-row Kalman diagnostic plot (one column per wave).

    Rows:
        0 — |A(t)| amplitude (SNR proxy)
        1 — Activity fraction
        2 — KF orientation estimate
        3 — KF phase speed
        4 — KF wavelength
    Inactive regions (wave_active == False) shaded in whitesmoke.
    """
    CLR_KF  = "#27ae60"
    CLR_RAW = "#95a5a6"
    CLR_REF = "#7f8c8d"

    n_waves = len(waves_detected)
    if n_waves == 0 or len(kf_results) == 0:
        return

    fig, axes = plt.subplots(5, n_waves, figsize=(6 * n_waves, 20), squeeze=False)
    colors    = plt.cm.tab10(np.linspace(0, 1, max(n_waves, 1)))

    def _shade_inactive(ax, active, t_arr):
        """Shade whitesmoke where wave_active is False."""
        in_off = False
        for i, flag in enumerate(active):
            if not flag and not in_off:
                t0s = t_arr[i]; in_off = True
            elif flag and in_off:
                ax.axvspan(t0s, t_arr[i], color="whitesmoke", zorder=0)
                in_off = False
        if in_off:
            ax.axvspan(t0s, t_arr[-1], color="whitesmoke", zorder=0)

    for col, wave in enumerate(waves_detected):
        wdf   = kf_results[kf_results["wave_id"] == col].copy()
        c     = colors[col]
        t_dt  = pd.to_datetime(wdf["time"])

        col_title = (
            f"{wave['label'][:40]}\n"
            f"orient={wave['orientation_deg']:.0f}°  "
            f"λ={wave['wavelength_km']:.0f} km"
        )

        if len(wdf) == 0:
            axes[0, col].set_title(col_title, fontsize=8, fontweight="bold")
            for row in range(1, 5):
                axes[row, col].set_visible(False)
            continue

        active = wdf["wave_active"].values.astype(bool)
        valid  = wdf["innovation_valid"].values.astype(bool)
        t_arr  = t_dt.values

        # ── Row 0: amplitude ──────────────────────────────────────────────
        ax = axes[0, col]
        ax.plot(t_dt, wdf["amplitude"], "-", color=c, lw=1.2)
        _shade_inactive(ax, active, t_arr)
        ax.set_title(col_title, fontsize=8, fontweight="bold")
        ax.set_ylabel("|A(t)| amplitude")
        ax.grid(True, alpha=0.3)

        # ── Row 1: activity fraction ──────────────────────────────────────
        ax = axes[1, col]
        if "activity_fraction" in wdf.columns:
            ax.fill_between(t_dt, wdf["activity_fraction"], alpha=0.5, color=c)
            ax.plot(t_dt, wdf["activity_fraction"], color=c, lw=1)
        _shade_inactive(ax, active, t_arr)
        ax.set_ylabel("Activity fraction")
        ax.set_ylim(0, 1)
        ax.grid(True, alpha=0.3)

        # ── Row 2: orientation ────────────────────────────────────────────
        ax = axes[2, col]
        if "gabor_orientation_deg" in wdf.columns:
            ax.plot(t_dt, wdf["gabor_orientation_deg"], ".",
                    color=CLR_RAW, ms=3, alpha=0.5, label="Raw obs")
        if "kf_orient_deg" in wdf.columns:
            ax.plot(t_dt[valid], wdf["kf_orient_deg"].values[valid],
                    "-", color=CLR_KF, lw=1.5, label="KF estimate")
        ax.axhline(
            wave["orientation_deg"], color=CLR_REF, ls="--", lw=1,
            label=f"ω-k ref ({wave['orientation_deg']:.0f}°)",
        )
        _shade_inactive(ax, active, t_arr)
        ax.set_ylabel("KF Orientation (deg)")
        ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

        # ── Row 3: phase speed ────────────────────────────────────────────
        ax = axes[3, col]
        if "kf_wavespeed_ms" in wdf.columns:
            ax.plot(
                t_dt[valid],
                np.abs(wdf["kf_wavespeed_ms"].values[valid]),
                "-", color=CLR_KF, lw=1.5, label="KF estimate (valid)",
            )
        ax.axhline(
            wave["phase_speed"], color=CLR_REF, ls="--", lw=1,
            label=f"ω-k ref ({wave['phase_speed']:.0f} m/s)",
        )
        _shade_inactive(ax, active, t_arr)
        ax.set_ylabel("Phase Speed (m/s)")
        ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

        # ── Row 4: wavelength ─────────────────────────────────────────────
        ax = axes[4, col]
        if "lambda_obs_km" in wdf.columns:
            ax.plot(t_dt, wdf["lambda_obs_km"], ".",
                    color=CLR_RAW, ms=3, alpha=0.5, label="Raw obs")
        if "kf_lambda_km" in wdf.columns:
            ax.plot(t_dt[valid], wdf["kf_lambda_km"].values[valid],
                    "-", color=CLR_KF, lw=1.5, label="KF smoothed")
        ax.axhline(
            wave["wavelength_km"], color=CLR_REF, ls="--", lw=1,
            label=f"ω-k ref ({wave['wavelength_km']:.0f} km)",
        )
        _shade_inactive(ax, active, t_arr)
        ax.set_ylabel("Wavelength (km)")
        ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

        for row in range(5):
            axes[row, col].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            axes[row, col].tick_params(axis="x", rotation=30)

    fig.suptitle("Kalman Time Series", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    if verbose:
        print(f"  Saved: {out_path}")


def _plot_ownership(
    waves_detected: list,
    full_time_grid: np.ndarray,
    out_path: str,
    verbose: bool,
) -> None:
    """Gantt-style plot showing which wave family owns each frame."""
    n_waves  = len(waves_detected)
    n_frames = len(full_time_grid)
    t_stamps = pd.to_datetime(full_time_grid)
    colors   = plt.cm.tab10(np.linspace(0, 1, max(n_waves, 1)))

    owned_matrix = np.array([w["_owned_mask"] for w in waves_detected],
                             dtype=bool)  # (n_waves, n_frames)
    t_f64   = full_time_grid.astype("datetime64[ns]").astype(np.float64)
    t_range = t_f64.max() - t_f64.min() if t_f64.max() != t_f64.min() else 1.0

    fig, axes = plt.subplots(
        n_waves + 1, 1,
        figsize=(13, 1.2 * (n_waves + 1)),
        sharex=True,
    )

    def _draw_runs(ax, mask, color):
        ax.barh(0, 1.0, left=0, height=0.7, color="whitesmoke",
                edgecolor="lightgray", lw=0.5)
        in_run = False
        for t in range(n_frames):
            if mask[t] and not in_run:
                rs = t; in_run = True
            elif not mask[t] and in_run:
                left  = (t_f64[rs]   - t_f64[0]) / t_range
                width = (t_f64[t-1]  - t_f64[rs]) / t_range
                ax.barh(0, width, left=left, height=0.7,
                        color=color, alpha=0.85, edgecolor="none")
                in_run = False
        if in_run:
            left  = (t_f64[rs] - t_f64[0]) / t_range
            width = (t_f64[-1] - t_f64[rs]) / t_range
            ax.barh(0, width, left=left, height=0.7,
                    color=color, alpha=0.85, edgecolor="none")

    for wi, wave in enumerate(waves_detected):
        ax    = axes[wi]
        _draw_runs(ax, owned_matrix[wi], colors[wi])
        ax.set_yticks([])
        ax.set_xlim(0, 1)
        n_own = int(owned_matrix[wi].sum())
        ax.set_ylabel(
            f"W{wi+1}\n({100.*n_own/n_frames:.0f}%)",
            fontsize=7, rotation=0, labelpad=42, va="center",
        )
        ax.text(0.005, 0, wave["label"][:38], fontsize=7, va="center")

    # Last row: unowned frames
    any_owned = owned_matrix.any(axis=0)
    _draw_runs(axes[-1], ~any_owned, "lightcoral")
    axes[-1].set_yticks([])
    axes[-1].set_xlim(0, 1)
    axes[-1].set_ylabel("Unowned", fontsize=7, rotation=0,
                        labelpad=42, va="center")

    # Readable x-axis tick labels
    n_ticks  = min(8, n_frames)
    tick_pos = np.linspace(0, 1, n_ticks)
    tick_idx = (tick_pos * (n_frames - 1)).astype(int).clip(0, n_frames - 1)
    axes[-1].set_xticks(tick_pos)
    axes[-1].set_xticklabels(
        [str(t_stamps[i])[:16] for i in tick_idx],
        rotation=30, fontsize=7,
    )

    fig.suptitle("Frame Ownership by Wave Family", fontsize=12)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    if verbose:
        print(f"  Saved: {out_path}")


def _write_kf_table(
    kf_results: pd.DataFrame,
    waves_detected: list,
    out_path: str,
    verbose: bool,
) -> None:
    """Write a plain-text KF summary table (mirrors notebook cell 46)."""
    import io as _io

    buf = _io.StringIO()
    buf.write("KALMAN PHASE-SPEED ESTIMATION SUMMARY\n")
    buf.write("=" * 100 + "\n")
    buf.write("Convention: 0° = North, 90° = East, clockwise.\n\n")

    for wi, wave in enumerate(waves_detected):
        wdf_v = kf_results[
            (kf_results["wave_id"] == wi) & kf_results["innovation_valid"]
        ]
        wdf_a = kf_results[
            (kf_results["wave_id"] == wi) & kf_results["wave_active"]
        ]
        wdf_x = kf_results[kf_results["wave_id"] == wi]
        n_v, n_a, n_x = len(wdf_v), len(wdf_a), len(wdf_x)

        buf.write(f"Wave {wi+1}: {wave['label']}\n")
        if n_v == 0:
            buf.write("  (no valid KF frames)\n\n")
            continue

        spd_m = wdf_v["kf_wavespeed_ms"].abs().mean()
        spd_s = wdf_v["kf_wavespeed_ms"].abs().std()

        if n_a > 0 and "kf_orient_deg" in kf_results.columns:
            ov   = wdf_a["kf_orient_deg"].dropna().values
            or_m = _cmean(ov) if len(ov) > 0 else float("nan")
            or_s = _cstd(ov)  if len(ov) > 1 else 0.0
        else:
            or_m = or_s = float("nan")

        if n_a > 0 and "kf_lambda_km" in kf_results.columns:
            lv   = wdf_a["kf_lambda_km"].dropna().values
            lm   = float(np.mean(lv))  if len(lv) > 0 else float("nan")
            ls   = float(np.std(lv))   if len(lv) > 1 else 0.0
        else:
            lm = ls = float("nan")

        buf.write(
            f"  Speed        KF={spd_m:.1f} ± {spd_s:.1f} m/s"
            f"  (ω-k ref={wave['phase_speed']:.1f} m/s)\n"
            f"  Orientation  KF={or_m:.1f} ± {or_s:.1f}°"
            f"  (ω-k ref={wave['orientation_deg']:.1f}°)\n"
            f"  Wavelength   KF={lm:.1f} ± {ls:.1f} km"
            f"  (ω-k ref={wave['wavelength_km']:.1f} km)\n"
            f"  Valid frames : {n_v}/{n_x} ({100.*n_v/n_x:.0f}%)  "
            f"Active: {n_a}/{n_x} ({100.*n_a/n_x:.0f}%)\n\n"
        )

        # Top-6 active frames by KF phase speed
        if n_a > 0:
            top_idx = wdf_a["kf_wavespeed_ms"].abs().nlargest(6).index
            buf.write("  Top-6 highest-speed active frames:\n")
            for idx in top_idx:
                row = kf_results.loc[idx]
                buf.write(
                    f"    {str(pd.to_datetime(row['time']))[:19]}"
                    f"  speed={abs(row['kf_wavespeed_ms']):.1f} m/s"
                    f"  orient={row.get('kf_orient_deg', float('nan')):.1f}°"
                    f"  λ={row.get('kf_lambda_km', float('nan')):.1f} km\n"
                )
            buf.write("\n")

    txt = buf.getvalue()
    with open(out_path, "w") as fh:
        fh.write(txt)
    if verbose:
        print(txt)
        print(f"  Saved: {out_path}")


# =============================================================================
# Quiet context manager
# =============================================================================

@contextlib.contextmanager
def _maybe_quiet(verbose: bool):
    """Redirect stdout/stderr when verbose is False."""
    if verbose:
        yield
    else:
        with contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            yield


# =============================================================================
# Main pipeline function
# =============================================================================

def run_airglow_pipeline(
    hdf5_path: str,
    output_dir: str,
    *,
    # ── Data loading ──────────────────────────────────────────────────────────
    site: str = "blo",
    date: datetime | None = None,
    moon_elevation_max: float = 0.0,
    ntaps: int = 13,
    t_lo_min: float = 2.0,
    t_hi_min: float = 20.0,
    elevation_min: float = 30.0,
    stem: str | None = None,
    # ── Sliding window ────────────────────────────────────────────────────────
    win_len_min: float = 90.0,
    hop_min: float = 10.0,
    zero_pad_factor: int = 5,
    # ── Omega-k filter ────────────────────────────────────────────────────────
    period_max_min: float = 60.0,
    lambda_h_min_km: float = 5.0,
    lambda_h_max_km: float = 150.0,
    apply_dispersion: bool = False,
    # ── Omega-k peak detection ────────────────────────────────────────────────
    power_threshold: int = 90,
    max_slowness: float = 50.0,
    max_wavelength_plot: float = 150.0,
    use_peak_finding: bool = True,
    peak_min_distance: int = 7,
    min_power_ratio: float = 0.10,
    # ── Wave family grouping ──────────────────────────────────────────────────
    dir_tol_deg: float = 15.0,
    lambda_tol_km: float = 20.0,
    min_detections: int = 2,
    # ── Gabor reconstruction ──────────────────────────────────────────────────
    gabor_n_stds: float = 3.0,
    wavelength_resolution_km: float = 20.0,
    orientation_resolution_deg: float = 15.0,
    rms_calibrate: bool = True,
    # ── Kalman: phase-speed (omega) filter ───────────────────────────────────
    kf_q_omega: float = 1e-8,
    kf_q_domega: float = 1e-12,
    kf_r_sigma_base: float = 0.3,
    kf_snr_min: float = 0.05,
    kf_chi2_gate: float = 9.0,
    kf_hard_chi2_gate: bool = False,
    kf_dwell_time: int = 10,
    kf_dom_window: int = 10,
    # ── Kalman: orientation (theta) filter ───────────────────────────────────
    kf_q_theta: float = 5e-5,
    kf_q_dtheta: float = 1e-8,
    kf_r_theta_deg: float = 5.0,
    # ── Kalman: wavelength (k_h) filter ──────────────────────────────────────
    kf_q_kh: float = 1e-12,
    kf_q_dkh: float = 1e-16,
    kf_r_kh_frac: float = 0.20,
    # ── Kalman: activity metric & binned statistics ───────────────────────────
    activity_threshold: float = 0.10,
    bin_width_min: int = 60,
    min_valid_frames: int = 5,
    # ── Animation ─────────────────────────────────────────────────────────────
    frame_dpi: int = 72,
    movie_fps: int = 5,
    make_animation: bool = True,
    # ── Execution ─────────────────────────────────────────────────────────────
    n_workers: int = 1,
    verbose: bool = False,
) -> dict:
    """
    Run the full airglow gravity-wave analysis pipeline on one night of data.

    Parameters
    ----------
    hdf5_path : str
        Path to the HDF5 data file for this night.
    output_dir : str
        Root directory for all output files.  Sub-directories ``results/``,
        ``movies/``, and ``windows/`` are created automatically.
    site : str
        Site code passed to the data loader (e.g. ``"blo"``).
    date : datetime, optional
        Observation date passed to the data loader.
    moon_elevation_max : float
        Reject frames where moon elevation > this threshold (degrees).
    ntaps : int
        FIR filter tap count; first/last ``ntaps//2`` frames are trimmed before
        the Kalman step to remove filter edge artefacts.
    t_lo_min, t_hi_min : float
        FIR bandpass lower / upper period limits (minutes).
    elevation_min : float
        Minimum CCD elevation for the FOV mask (degrees).
    stem : str, optional
        Label used in output filenames.  Defaults to the HDF5 filename stem.
    win_len_min : float
        Sliding-window duration (minutes).  Default 90.
    hop_min : float
        Advance between successive windows (minutes).  Default 10.
    zero_pad_factor : int
        Temporal zero-padding multiplier inside each window FFT.
    period_max_min : float
        Omega-k filter upper period limit (minutes).
    lambda_h_min_km, lambda_h_max_km : float
        Omega-k filter horizontal wavelength bounds (km).
    apply_dispersion : bool
        Apply dispersion-relation filter.  Keep ``False`` until winds are
        available for Doppler correction.
    power_threshold : int
        Percentile for significant FFT power (0–100).
    max_slowness : float
        Slowness-space plot axis limit (s/km).
    max_wavelength_plot : float
        Wavelength-space plot axis limit (km).
    use_peak_finding : bool
        Use peak-finding for wave detection (recommended).
    peak_min_distance : int
        Minimum bin separation between detected peaks.
    min_power_ratio : float
        Minimum power ratio for secondary detections relative to the dominant.
    dir_tol_deg : float
        Direction spread tolerance within a wave family (degrees).
    lambda_tol_km : float
        Wavelength spread tolerance within a wave family (km).
    min_detections : int
        Minimum window appearances for a family to be retained.
    gabor_n_stds : float
        Gabor kernel spatial half-width in sigma units.
    wavelength_resolution_km : float
        Target wavelength resolution — sets Bf.
    orientation_resolution_deg : float
        Target orientation resolution — sets Btheta.
    rms_calibrate : bool
        Normalise Gabor output RMS to match omega-k.
    kf_q_omega : float
        KF process noise on angular frequency.
    kf_q_domega : float
        KF process noise on frequency rate.
    kf_r_sigma_base : float
        KF measurement noise base (phase-speed filter).
    kf_snr_min : float
        Minimum amplitude SNR for a KF update to be accepted.
    kf_chi2_gate : float
        Chi-squared innovation gate threshold.
    kf_hard_chi2_gate : bool
        Hard-reject (True) or inflate-R (False) beyond the gate.
    kf_dwell_time : int
        Frames before a wave is classified as dominant.
    kf_dom_window : int
        Dominance look-back window (frames).
    kf_q_theta : float
        Orientation KF process noise.
    kf_q_dtheta : float
        Orientation-rate process noise.
    kf_r_theta_deg : float
        Orientation measurement noise (degrees).
    kf_q_kh : float
        Wavenumber KF process noise.
    kf_q_dkh : float
        Wavenumber-rate process noise.
    kf_r_kh_frac : float
        Wavenumber measurement noise as a fraction of the current estimate.
    activity_threshold : float
        Activity-fraction threshold below which a wave is inactive.
    bin_width_min : int
        Bin width for nightly KF statistics (minutes).
    min_valid_frames : int
        Minimum valid frames per bin for statistics to be reported.
    frame_dpi : int
        DPI for animation frames (72 = fast preview, 150 = publication).
    movie_fps : int
        Output mp4 frame rate.
    make_animation : bool
        Set to ``False`` to skip frame rendering and mp4 encoding.
    n_workers : int
        Number of parallel worker processes.  Controls both the sliding-window
        loop and animation rendering.  Use ``1`` inside Jupyter to avoid
        multiprocessing deadlocks.
    verbose : bool
        Print per-step diagnostics.  Default is silent.

    Returns
    -------
    dict
        All computed products keyed by name:

        ``ds_full``             — preprocessed xr.Dataset
        ``window_results``      — list of per-window omega-k dicts
        ``families``            — list of wave family dicts
        ``waves_detected``      — list of per-frame Gabor parameter dicts
        ``wave_cubes``          — list of (n_time, ny, nx) reconstructed arrays
        ``composite``           — (n_time, ny, nx) Gabor composite
        ``complex_amplitudes``  — list of per-wave complex amplitude arrays
        ``data_omegak``         — (n_time, ny, nx) omega-k filtered reference
        ``kf_results``          — pd.DataFrame of frame-by-frame KF output
        ``kf_binned``           — pd.DataFrame of time-binned statistics
        ``correlation_stats``   — dict with per-wave and composite Pearson r
        ``output_paths``        — dict mapping label → file path for all outputs
    """
    # ── Setup ─────────────────────────────────────────────────────────────────
    output_dir  = Path(output_dir)
    results_dir = output_dir / "results"
    movies_dir  = output_dir / "movies"
    windows_dir = output_dir / "windows"
    for d in [results_dir, movies_dir, windows_dir]:
        d.mkdir(parents=True, exist_ok=True)

    if stem is None:
        stem = Path(hdf5_path).stem

    output_paths: dict[str, str] = {}

    def _vprint(*args, **kw):
        if verbose:
            print(*args, **kw)

    # ── Step 1: Load data ─────────────────────────────────────────────────────
    _vprint("[1/9] Loading data ...")
    ds_full, data_var, dx_km = _load_real_data(
        hdf5_path=hdf5_path, site=site, date=date,
        moon_elevation_max=moon_elevation_max, ntaps=ntaps,
        t_lo_min=t_lo_min, t_hi_min=t_hi_min,
        elevation_min=elevation_min, verbose=verbose,
    )

    time_vals = ds_full["time"].values          # (n_time,) datetime64
    east_km   = ds_full["east"].values          # (nx,)
    north_km  = ds_full["north"].values         # (ny,)
    n_time    = ds_full.sizes["time"]
    dt_min    = float(
        (time_vals[1] - time_vals[0]) / np.timedelta64(1, "m")
    )
    edge_frames    = ntaps // 2
    win_len_frames = int(round(win_len_min / dt_min))
    hop_frames     = int(round(hop_min     / dt_min))

    _vprint(
        f"  n_time={n_time}, dt={dt_min:.1f} min, dx={dx_km:.2f} km\n"
        f"  window={win_len_frames} frames, hop={hop_frames} frames, "
        f"edge_trim={edge_frames} frames"
    )

    # Build grouped kwargs from individual parameters
    filter_kwargs = dict(
        period_min       = dt_min * 2,        # Nyquist
        period_max       = period_max_min,
        lambda_h_min     = lambda_h_min_km,
        lambda_h_max     = lambda_h_max_km,
        apply_dispersion = apply_dispersion,
    )
    analysis_kwargs = dict(
        power_threshold   = power_threshold,
        max_slowness      = max_slowness,
        max_wavelength    = max_wavelength_plot,
        use_peak_finding  = use_peak_finding,
        peak_min_distance = peak_min_distance,
        min_power_ratio   = min_power_ratio,
    )

    # ── Spatial-mean removal ──────────────────────────────────────────────────
    _da = ds_full[data_var]
    _da = _da - _da.mean(dim=["east", "north"])
    data_arr_full = _da.transpose("time", "north", "east").values  # (n_time, ny, nx)

    # ── Step 2: Sliding-window omega-k ────────────────────────────────────────
    _vprint(f"[2/9] Sliding-window omega-k "
            f"({'parallel' if n_workers > 1 else 'serial'}, "
            f"{n_workers} worker(s)) ...")

    half    = win_len_frames // 2
    centers = list(range(half, n_time - half, hop_frames))

    window_args = [
        {
            "data_arr":        data_arr_full[c - half : c + half].copy(),
            "time_arr":        time_vals[c - half : c + half],
            "east_km":         east_km,
            "north_km":        north_km,
            "center_idx":      c,
            "center_time":     time_vals[c],
            "data_var":        data_var,
            "zero_pad_factor": zero_pad_factor,
            "filter_kwargs":   filter_kwargs,
            "analysis_kwargs": analysis_kwargs,
            "save_prefix":     str(windows_dir / f"window_{i:03d}"),
            "verbose":         verbose,
        }
        for i, c in enumerate(centers)
    ]

    if n_workers > 1:
        window_results = [None] * len(window_args)
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(_window_worker, a): i
                for i, a in enumerate(window_args)
            }
            for fut in as_completed(futures):
                window_results[futures[fut]] = fut.result()
    else:
        window_results = []
        for a in window_args:
            with _maybe_quiet(verbose):
                window_results.append(_window_worker(a))

    _vprint(
        f"  {len(window_results)} windows done.  "
        f"Detections: {[len(r['physical_waves']) for r in window_results]}"
    )

    # ── Step 3: Wave family grouping ──────────────────────────────────────────
    _vprint("[3/9] Wave family grouping ...")
    with _maybe_quiet(verbose):
        families = build_wave_families(
            window_results,
            dir_tol_deg    = dir_tol_deg,
            lambda_tol_km  = lambda_tol_km,
            min_detections = min_detections,
        )
    _vprint(f"  {len(families)} family/families retained.")

    with _maybe_quiet(verbose):
        fig_ts = plot_wave_family_timeseries(
            families       = families,
            ds_truth       = None,
            win_len_frames = win_len_frames,
            dt_min         = dt_min,
            domain_size_km = float(np.abs(east_km[-1] - east_km[0])),
            fig_title      = f"Wave Families — {stem}",
            save_path      = str(results_dir / f"{stem}_family_timeseries.png"),
        )
    plt.close(fig_ts)
    output_paths["family_timeseries"] = str(results_dir / f"{stem}_family_timeseries.png")

    with _maybe_quiet(verbose):
        ds_families = families_to_xarray(families, ds_truth=None)
    _nc_path = str(results_dir / f"{stem}_omegak_families.nc")
    ds_families.to_netcdf(_nc_path)
    output_paths["families_nc"] = _nc_path

    # ── Step 4: Gabor parameters & per-frame interpolation ────────────────────
    _vprint("[4/9] Gabor parameters & per-frame interpolation ...")
    with _maybe_quiet(verbose):
        gabor_bf, gabor_btheta_full = compute_adaptive_gabor_parameters(
            wavelength_resolution_km   = wavelength_resolution_km,
            orientation_resolution_deg = orientation_resolution_deg,
            resolution_km_per_pixel    = dx_km,
            verbose=verbose,
        )
    gabor_btheta   = gabor_btheta_full / 2.0
    full_time_grid = time_vals.astype("datetime64[ns]")

    with _maybe_quiet(verbose):
        waves_detected = build_waves_for_gabor(
            families      = families,
            time_grid     = full_time_grid,
            n_stds        = gabor_n_stds,
            win_half_min  = win_len_min / 2.0,
            dir_tol_deg   = dir_tol_deg,
            lambda_tol_km = lambda_tol_km,
        )
    _vprint(f"  {len(waves_detected)} wave(s) prepared for Gabor.")

    _own_path = str(results_dir / f"{stem}_ownership.png")
    _plot_ownership(waves_detected, full_time_grid, _own_path, verbose)
    output_paths["ownership"] = _own_path

    # ── Step 5: Gabor reconstruction ──────────────────────────────────────────
    _vprint("[5/9] Gabor reconstruction ...")
    with _maybe_quiet(verbose):
        wave_cubes, composite, complex_amplitudes, kx_obs_arrays, ky_obs_arrays = \
            reconstruct_all_waves(
                data_filtered_temporal = data_arr_full,
                waves                  = waves_detected,
                dx_km                  = dx_km,
                Bf                     = gabor_bf,
                Btheta                 = gabor_btheta,
                n_stds                 = gabor_n_stds,
                rms_calibrate          = rms_calibrate,
            )
    _vprint(f"  wave_cubes: {len(wave_cubes)} × {wave_cubes[0].shape}")

    # ── Step 6: Omega-k full-dataset reference ────────────────────────────────
    _vprint("[6/9] Omega-k full-dataset filter (cross-validation reference) ...")
    _proc_ds = ds_full.copy()
    _proc_ds[data_var] = _da
    with _maybe_quiet(verbose):
        ok_full       = OmegaKFilterXarray(
            _proc_ds.transpose("time", "north", "east"), data_var=data_var,
        )
        mask_full     = ok_full.create_filter(**filter_kwargs)
        filtered_full = ok_full.apply_filter(mask_full)
    data_omegak = (
        filtered_full[data_var + "_filtered"]
        .transpose("time", "north", "east")
        .values
    )

    # ── Step 7: Cross-validation ──────────────────────────────────────────────
    _vprint("[7/9] Cross-validation (Gabor ↔ omega-k) ...")

    def _frame_r(a, b):
        mask = np.isfinite(a) & np.isfinite(b)
        return pearsonr(a[mask].ravel(), b[mask].ravel())[0] if mask.sum() > 1 else np.nan

    sl      = slice(edge_frames, n_time - edge_frames) if edge_frames > 0 else slice(None)
    sl_idx  = list(range(*sl.indices(n_time)))
    time_kf = time_vals[sl]

    corr_per_wave = [
        np.array([_frame_r(cube[t], data_omegak[t]) for t in sl_idx])
        for cube in wave_cubes
    ]
    corrs_comp  = np.array([_frame_r(composite[t], data_omegak[t]) for t in sl_idx])
    mean_r_comp = float(np.nanmean(corrs_comp))
    correlation_stats = {
        "per_wave":         corr_per_wave,
        "composite":        corrs_comp,
        "mean_r_composite": mean_r_comp,
    }
    _vprint(f"  Composite mean Pearson r = {mean_r_comp:.3f}")

    # Correlation time series plot
    colors     = plt.cm.tab10(np.linspace(0, 1, max(len(waves_detected), 1)))
    t_labels   = pd.to_datetime(time_kf)
    fig, ax    = plt.subplots(figsize=(12, 4))
    for wi, (wave, corrs) in enumerate(zip(waves_detected, corr_per_wave)):
        ax.plot(t_labels, corrs, color=colors[wi], alpha=0.5, lw=1.2,
                label=f"Wave {wi+1} (μ={np.nanmean(corrs):.2f})")
    ax.plot(t_labels, corrs_comp, "k-", lw=2.0,
            label=f"Composite (μ={mean_r_comp:.2f})")
    ax.axhline(0, color="gray", lw=0.8, ls="--")
    ax.set_ylim(-1, 1)
    ax.set_xlabel("Time")
    ax.set_ylabel("Pearson r  (Gabor vs omega-k)")
    ax.set_title(f"Frame-by-frame Gabor ↔ omega-k correlation — {stem}")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.tick_params(axis="x", rotation=30)
    ax.legend(fontsize=9)
    fig.tight_layout()
    _corr_path = str(results_dir / f"{stem}_correlation.png")
    fig.savefig(_corr_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    output_paths["correlation"] = _corr_path

    # Scatter plot
    comp_flat = composite[sl].ravel()
    ok_flat   = data_omegak[sl].ravel()
    valid_sc  = np.isfinite(comp_flat) & np.isfinite(ok_flat)
    comp_flat, ok_flat = comp_flat[valid_sc], ok_flat[valid_sc]
    if len(comp_flat) > 500_000:
        _idx      = np.random.default_rng(0).choice(len(comp_flat), 500_000, replace=False)
        comp_flat = comp_flat[_idx]; ok_flat = ok_flat[_idx]
    r_all, _  = pearsonr(comp_flat, ok_flat)
    lim       = max(np.abs(comp_flat).max(), np.abs(ok_flat).max()) * 1.05
    fig, ax   = plt.subplots(figsize=(6, 6))
    ax.scatter(ok_flat, comp_flat, s=0.3, alpha=0.3, rasterized=True, color="steelblue")
    ax.plot([-lim, lim], [-lim, lim], "r--", lw=1.5, label="1:1")
    ax.set_xlim(-lim, lim); ax.set_ylim(-lim, lim)
    ax.set_xlabel("omega-k filtered"); ax.set_ylabel("Gabor composite")
    ax.set_title(f"Gabor composite vs omega-k\n(Pearson r = {r_all:.3f})")
    ax.legend()
    fig.tight_layout()
    _scat_path = str(results_dir / f"{stem}_scatter.png")
    fig.savefig(_scat_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    output_paths["scatter"] = _scat_path

    # Middle-frame panel plot
    mid_t    = n_time // 2
    residual = data_arr_full - composite
    panels_p = (
        [(data_arr_full, "Input (spatial mean removed)"),
         (data_omegak,   "omega-k Filtered"),
         (composite,     f"Gabor Composite ({len(waves_detected)} wave(s))"),
         (residual,      "Residual (Input − Composite)")]
        + [(cube, f"Wave {wi+1}: {w['orientation_deg']:.0f}°  λ={w['wavelength_km']:.0f} km")
           for wi, (cube, w) in enumerate(zip(wave_cubes, waves_detected))]
    )
    n_p, n_cp = len(panels_p), 4
    n_rp      = (n_p + n_cp - 1) // n_cp
    vmax_p    = 2.0 * float(np.nanstd(np.concatenate([c.ravel() for c, _ in panels_p])))
    fig, axes = plt.subplots(n_rp, n_cp, figsize=(20, 5 * n_rp))
    axes      = np.array(axes).reshape(n_rp, n_cp)
    for idx, (cube, title) in enumerate(panels_p):
        r, c = divmod(idx, n_cp)
        im   = axes[r, c].imshow(cube[mid_t], origin="lower", cmap="RdBu_r",
                                  vmin=-vmax_p, vmax=vmax_p, aspect="equal")
        plt.colorbar(im, ax=axes[r, c], shrink=0.85)
        axes[r, c].set_title(title, fontsize=9)
    for idx in range(n_p, n_rp * n_cp):
        r, c = divmod(idx, n_cp); axes[r, c].set_visible(False)
    fig.suptitle(
        f"Frame {mid_t}/{n_time}  —  {str(pd.to_datetime(time_vals[mid_t]))[:19]}"
    )
    fig.tight_layout()
    _panel_path = str(results_dir / f"{stem}_panel.png")
    fig.savefig(_panel_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    output_paths["panel"] = _panel_path

    # ── Step 8: Kalman tracking ───────────────────────────────────────────────
    _vprint("[8/9] Kalman tracking ...")
    cmplx_sl       = [A[sl]  for A in complex_amplitudes]
    kx_obs_sl      = [kx[sl] for kx in kx_obs_arrays]
    ky_obs_sl      = [ky[sl] for ky in ky_obs_arrays]
    wave_cubes_sl  = [c[sl]  for c in wave_cubes]
    rms_omegak     = np.sqrt(np.mean(data_omegak[sl] ** 2, axis=(1, 2)))
    rms_wave_cubes = [np.sqrt(np.mean(c ** 2, axis=(1, 2))) for c in wave_cubes_sl]
    dt_seconds     = float(np.median(np.diff(
        time_kf.astype("datetime64[s]").astype(float)
    )))

    with _maybe_quiet(verbose):
        kf_results = analyze_all_waves_kalman(
            waves_detected     = waves_detected,
            complex_amplitudes = cmplx_sl,
            time_vals          = time_kf,
            dt                 = dt_seconds,
            q_omega            = kf_q_omega,
            q_domega           = kf_q_domega,
            r_sigma_base       = kf_r_sigma_base,
            snr_min_amplitude  = kf_snr_min,
            chi2_gate          = kf_chi2_gate,
            hard_chi2_gate     = kf_hard_chi2_gate,
            dwell_time         = kf_dwell_time,
            dominant_window    = kf_dom_window,
            verbose            = verbose,
            kx_obs_list        = kx_obs_sl,
            ky_obs_list        = ky_obs_sl,
            btheta_rad         = gabor_btheta,
            kf_q_theta         = kf_q_theta,
            kf_q_dtheta        = kf_q_dtheta,
            kf_r_theta_deg     = kf_r_theta_deg,
            rms_omegak         = rms_omegak,
            rms_wave_cubes     = rms_wave_cubes,
            activity_threshold = activity_threshold,
            kf_q_kh            = kf_q_kh,
            kf_q_dkh           = kf_q_dkh,
            kf_r_kh_frac       = kf_r_kh_frac,
            lambda_h_min_km    = lambda_h_min_km,
            lambda_h_max_km    = lambda_h_max_km,
        )

    with _maybe_quiet(verbose):
        kf_binned = compute_binned_wave_stats(
            kf_results,
            bin_width_min    = bin_width_min,
            min_valid_frames = min_valid_frames,
        )

    if len(kf_binned) > 0:
        _kfb_path = str(results_dir / f"{stem}_kf_binned.csv")
        kf_binned.to_csv(_kfb_path, index=False, float_format="%.4f")
        output_paths["kf_binned_csv"] = _kfb_path

    _kf_ts_path = str(results_dir / f"{stem}_kf_timeseries.png")
    _plot_kf_timeseries(kf_results, waves_detected, time_kf, _kf_ts_path, verbose)
    output_paths["kf_timeseries"] = _kf_ts_path

    _kf_tbl_path = str(results_dir / f"{stem}_kf_table.txt")
    _write_kf_table(kf_results, waves_detected, _kf_tbl_path, verbose)
    output_paths["kf_table"] = _kf_tbl_path

    # ── Step 8b: Second-pass Gabor reconstruction using KF estimates ──────────
    # Uses KF-refined per-frame orientation and wavelength as the kernel
    # parameters, producing a better reconstruction than the static first pass.
    # The first-pass Pearson r (step 7) is preserved as the independent
    # cross-validation metric.
    _vprint("[8b/9] Second-pass Gabor reconstruction (KF-refined parameters) ...")

    # Build per-frame orientation / wavelength arrays aligned to the full
    # n_time grid.  KF output only covers the trimmed slice [edge_frames,
    # n_time-edge_frames]; edge frames receive NaN → zero contribution.
    _t_full_ns = time_vals.astype("datetime64[ns]").astype(np.int64)
    waves_kf   = []
    # Also collect KF arrays (n_time-length) for animation labels
    _kf_az_series    = []
    _kf_lam_series   = []
    _kf_spd_series   = []
    _kf_valid_series = []

    for wi, wave in enumerate(waves_detected):
        wdf = kf_results[kf_results["wave_id"] == wi].copy()
        orient_kf = np.full(n_time, np.nan)
        lam_kf    = np.full(n_time, np.nan)
        az_kf     = np.full(n_time, np.nan)
        spd_kf    = np.full(n_time, np.nan)
        valid_kf  = np.zeros(n_time, dtype=bool)

        if len(wdf) > 0:
            _t_kf_ns = pd.to_datetime(wdf["time"].values).astype(np.int64)
            kf_idx   = np.searchsorted(_t_full_ns, _t_kf_ns).clip(0, n_time - 1)
            # Only accept frames whose time matches exactly (within 1 s)
            match = np.abs(_t_full_ns[kf_idx] - _t_kf_ns) < int(1e9)
            kf_idx = kf_idx[match]

            orient_kf[kf_idx] = wdf["kf_orient_deg"].values[match] % 180.0
            lam_kf[kf_idx]    = wdf["kf_lambda_km"].values[match]
            az_kf[kf_idx]     = wdf["kf_orient_deg"].values[match]       # full-circle
            spd_kf[kf_idx]    = np.abs(wdf["kf_wavespeed_ms"].values[match])
            valid_kf[kf_idx]  = wdf["innovation_valid"].values[match].astype(bool)

        wave_kf = dict(wave)
        wave_kf["orientation_deg_series"] = orient_kf
        wave_kf["wavelength_km_series"]   = lam_kf
        waves_kf.append(wave_kf)

        _kf_az_series.append(az_kf)
        _kf_lam_series.append(lam_kf)
        _kf_spd_series.append(spd_kf)
        _kf_valid_series.append(valid_kf)

    with _maybe_quiet(verbose):
        wave_cubes_kf, composite_kf, _, _, _ = reconstruct_all_waves(
            data_filtered_temporal = data_arr_full,
            waves                  = waves_kf,
            dx_km                  = dx_km,
            Bf                     = gabor_bf,
            Btheta                 = gabor_btheta,
            n_stds                 = gabor_n_stds,
            rms_calibrate          = rms_calibrate,
        )
    _vprint("  Second-pass reconstruction complete.")

    # ── Step 9: Animation ─────────────────────────────────────────────────────
    if make_animation:
        _vprint(
            f"[9/9] Animation ({n_time} frames, dpi={frame_dpi}, "
            f"{'parallel' if n_workers > 1 else 'serial'}) ..."
        )
        frames_dir = movies_dir / f"_frames_{stem}"
        frames_dir.mkdir(exist_ok=True)
        movie_path = str(movies_dir / f"{stem}.mp4")

        N_STATIC = 4   # Input / omega-k / Composite / Residual
        panels_a = (
            [(data_arr_full,                "Input (spatial mean removed)"),
             (data_omegak,                  "omega-k Filtered"),
             (composite_kf,                 f"Gabor Composite — KF pass ({len(waves_detected)} wave(s))"),
             (data_arr_full - composite_kf, "Residual (Input − Composite)")]
            + [(cube, f"Wave {wi + 1}")   # title completed per-frame in _frame_worker
               for wi, cube in enumerate(wave_cubes_kf)]
        )
        n_pa   = len(panels_a); n_ca = 4
        n_ra   = (n_pa + n_ca - 1) // n_ca
        vmax_a = 2.0 * float(np.nanstd(np.concatenate([c.ravel() for c, _ in panels_a])))
        shared = {
            "panels":          panels_a,
            "vmax_common":     vmax_a,
            "n_rows":          n_ra,
            "n_cols":          n_ca,
            "figsize":         (14, 3.5 * n_ra),
            "dpi":             frame_dpi,
            "frames_dir":      str(frames_dir),
            "time_labels":     [str(pd.to_datetime(t))[:19] for t in time_vals],
            "n_time":          n_time,
            # Per-frame KF label arrays (one (n_time,) array per wave)
            "n_static_panels": N_STATIC,
            "kf_az_series":    _kf_az_series,
            "kf_lam_series":   _kf_lam_series,
            "kf_spd_series":   _kf_spd_series,
            "kf_valid_series": _kf_valid_series,
        }

        if n_workers > 1:
            with ProcessPoolExecutor(
                max_workers=n_workers,
                initializer=_anim_initializer,
                initargs=(shared,),
            ) as pool:
                futs = {pool.submit(_frame_worker, t): t for t in range(n_time)}
                done = 0
                for fut in as_completed(futs):
                    fut.result()
                    done += 1
                    if verbose and done % 20 == 0:
                        print(f"  Frames: {done}/{n_time}", end="\r", flush=True)
        else:
            _anim_initializer(shared)
            for t in range(n_time):
                _frame_worker(t)
                if verbose and t % 20 == 0:
                    print(f"  Frames: {t+1}/{n_time}", end="\r", flush=True)

        _vprint(f"\n  All {n_time} frames rendered.")

        if shutil.which("ffmpeg") is not None:
            result = subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-framerate", str(movie_fps),
                    "-i", str(frames_dir / "frame_%04d.png"),
                    "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "23",
                    movie_path,
                ],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                size_mb = os.path.getsize(movie_path) / 1e6
                shutil.rmtree(frames_dir)
                output_paths["movie"] = movie_path
                _vprint(f"  Saved: {movie_path}  ({size_mb:.1f} MB)")
            else:
                _vprint(f"  ffmpeg error:\n{result.stderr[-800:]}")
                output_paths["frames_dir"] = str(frames_dir)
        else:
            _vprint("  ffmpeg not found — frames left in", str(frames_dir))
            output_paths["frames_dir"] = str(frames_dir)
    else:
        _vprint("[9/9] Animation skipped (make_animation=False).")

    _vprint(f"\nPipeline complete.  Outputs in: {output_dir.resolve()}")
    _vprint(f"  Composite mean r = {mean_r_comp:.3f}")

    return {
        "ds_full":            ds_full,
        "window_results":     window_results,
        "families":           families,
        "waves_detected":     waves_detected,
        "wave_cubes":         wave_cubes,         # first-pass (independent cross-val)
        "composite":          composite,
        "wave_cubes_kf":      wave_cubes_kf,      # KF-refined second pass
        "composite_kf":       composite_kf,
        "complex_amplitudes": complex_amplitudes,
        "data_omegak":        data_omegak,
        "kf_results":         kf_results,
        "kf_binned":          kf_binned,
        "correlation_stats":  correlation_stats,
        "output_paths":       output_paths,
    }
