"""
wave_validation.py
------------------
Utilities for validating the omega-k pipeline against synthetic wave inputs.

Functions
---------
sampling_properties_table   -- HTML summary of sampling limits and resolutions
boxcar_expected             -- Expected sliding-window average of a truth signal
sliding_window_analysis     -- Run the omega-k pipeline over a sliding window
build_wave_families         -- Cluster per-window detections into coherent wave families
families_to_xarray          -- Convert family list to xarray Dataset (Gabor/KF handoff)
plot_wave_family_timeseries -- 3-panel time series: speed, wavelength, azimuth
"""

import numpy as np
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from IPython.display import HTML


# ---------------------------------------------------------------------------
# Sampling properties table
# ---------------------------------------------------------------------------

def sampling_properties_table(
    dx_km: float,
    dt_min: float,
    win_len_frames: int,
    zero_pad_factor: int = 1,
    wavelengths_km: list = None,
) -> HTML:
    """
    Render an HTML table summarising sampling limits and velocity resolutions.

    Parameters
    ----------
    dx_km          : spatial pixel size (km)
    dt_min         : temporal sampling interval (minutes)
    win_len_frames : number of frames per analysis window
    zero_pad_factor: temporal zero-padding factor used in FFT
    wavelengths_km : list of wavelengths (km) for the Nyquist-speed and
                     resolution rows.  Defaults to [20, 40, 80, 150].
    """
    if wavelengths_km is None:
        wavelengths_km = [20, 40, 80, 150]

    dt_sec     = dt_min * 60.0
    T_win_sec  = win_len_frames * dt_sec
    T_win_min  = T_win_sec / 60.0
    T_eff_sec  = T_win_sec * zero_pad_factor   # zero-padding sharpens peak location

    nyq_wl_km  = 2.0 * dx_km
    nyq_T_min  = 2.0 * dt_min
    rec_wl_km  = 4.0 * dx_km
    rec_T_min  = 4.0 * dt_min

    rows_speed = []
    for lam in wavelengths_km:
        c_nyq  = (lam * 1000.0) / (2.0 * dt_sec)
        dv_true = (lam * 1000.0) / T_win_sec
        dv_interp = (lam * 1000.0) / T_eff_sec
        rows_speed.append((lam, c_nyq, dv_true, dv_interp))

    html = """
<style>
  .sv-table {{ border-collapse: collapse; font-family: monospace; font-size: 13px; }}
  .sv-table th {{ background: #2c3e50; color: white; padding: 6px 12px; text-align: left; }}
  .sv-table td {{ padding: 5px 12px; border-bottom: 1px solid #ddd; }}
  .sv-table tr:nth-child(even) {{ background: #f8f8f8; }}
  .sv-section {{ background: #ecf0f1; font-weight: bold; }}
  .warn {{ color: #c0392b; font-weight: bold; }}
</style>
<table class="sv-table">
<tr><th colspan="2">SAMPLING PROPERTIES SUMMARY</th></tr>

<tr class="sv-section"><td colspan="2">Grid &amp; Temporal Sampling</td></tr>
<tr><td>Spatial pixel size (dx)</td><td>{dx:.2f} km</td></tr>
<tr><td>Temporal sampling (dt)</td><td>{dt:.1f} min</td></tr>
<tr><td>Window length</td><td>{wf} frames = {wm:.0f} min</td></tr>
<tr><td>Zero-pad factor</td><td>{zp}&times;  (padded window = {wpf} frames)</td></tr>

<tr class="sv-section"><td colspan="2">Nyquist / Reliability Limits</td></tr>
<tr><td>Spatial Nyquist wavelength (hard limit)</td><td>{nyq_wl:.1f} km</td></tr>
<tr><td>Temporal Nyquist period (hard limit)</td><td>{nyq_T:.1f} min</td></tr>
<tr><td>Recommended wavelength (4&times; sampling)</td><td>{rec_wl:.1f} km</td></tr>
<tr><td>Recommended period (4&times; sampling)</td><td>{rec_T:.1f} min</td></tr>

<tr class="sv-section"><td colspan="2">Velocity Resolution &amp; Nyquist Speed per Wavelength</td></tr>
<tr>
  <th>&lambda; (km)</th>
  <th>c_Nyquist (m/s)</th>
  <th>&delta;v true resolution (m/s)</th>
  <th>&delta;v interpolated (m/s) [zero-padded]</th>
</tr>
""".format(
        dx=dx_km, dt=dt_min,
        wf=win_len_frames, wm=T_win_min,
        zp=zero_pad_factor, wpf=win_len_frames * zero_pad_factor,
        nyq_wl=nyq_wl_km, nyq_T=nyq_T_min,
        rec_wl=rec_wl_km, rec_T=rec_T_min,
    )

    for lam, c_nyq, dv_true, dv_interp in rows_speed:
        html += (
            f"<tr><td>{lam:.0f}</td>"
            f"<td>{c_nyq:.1f}</td>"
            f"<td>{dv_true:.1f}</td>"
            f"<td>{dv_interp:.1f} "
            f"{'<span class=\"warn\">(zero-padding ≠ real resolution)</span>' if zero_pad_factor > 1 else ''}"
            f"</td></tr>\n"
        )

    html += """
<tr class="sv-section"><td colspan="2">Aliasing Wrap Formula</td></tr>
<tr><td colspan="2">
  c_alias = c_true − λ/dt  when c_true &gt; c_Nyquist<br>
  Direction reversal occurs when c_alias &lt; 0
</td></tr>
</table>
"""
    return HTML(html)


# ---------------------------------------------------------------------------
# Boxcar expected
# ---------------------------------------------------------------------------

def boxcar_expected(
    t_hours: np.ndarray,
    truth_values: np.ndarray,
    window_hr: float = 1.5,
    dt_hr: float = None,
) -> tuple:
    """
    Compute the expected sliding-window boxcar average of any truth time series.

    Parameters
    ----------
    t_hours      : time axis of the truth signal (hours)
    truth_values : truth signal values (any units)
    window_hr    : window length (hours)
    dt_hr        : output time step (hours); defaults to input spacing

    Returns
    -------
    t_centers   : window centre times (hours)
    v_expected  : boxcar-averaged values
    """
    half = window_hr / 2.0
    if dt_hr is None:
        dt_hr = t_hours[1] - t_hours[0]

    t_centers  = np.arange(t_hours[0] + half, t_hours[-1] - half, dt_hr)
    v_expected = np.empty_like(t_centers)

    for i, tc in enumerate(t_centers):
        in_window      = (t_hours >= tc - half) & (t_hours <= tc + half)
        v_expected[i]  = truth_values[in_window].mean()

    return t_centers, v_expected


# ---------------------------------------------------------------------------
# Sliding window pipeline
# ---------------------------------------------------------------------------

def sliding_window_analysis(
    ds_full: xr.Dataset,
    data_var: str,
    win_len_frames: int,
    hop_frames: int,
    filter_kwargs: dict,
    analysis_kwargs: dict,
    zero_pad_factor: int = 5,
    output_dir: str = ".",
    verbose: bool = False,
) -> list:
    """
    Run the omega-k pipeline over a sliding window and collect per-window results.

    Parameters
    ----------
    ds_full        : full-duration Dataset (time, north, east)
    data_var       : name of the image data variable
    win_len_frames : number of time frames per window
    hop_frames     : number of frames to advance between windows
    filter_kwargs  : kwargs forwarded to OmegaKFilterXarray.create_filter()
                     (period_min, period_max, lambda_h_min, lambda_h_max, …)
    analysis_kwargs: kwargs forwarded to analyze_and_plot_waves()
                     (power_threshold, max_slowness, max_wavelength, …)
    zero_pad_factor: temporal zero-padding factor
    output_dir     : directory for per-window saved figures
    verbose        : if True, print per-window diagnostics

    Returns
    -------
    window_results : list of dicts, one per window, each containing:
        center_index   : int
        center_time    : np.datetime64
        wave_params    : dict  (raw ω-k output for this window)
        physical_waves : list of dicts (clustered wave detections)
    """
    # Late imports so this module has no hard dependency on the filter classes
    from OmegaKFilter    import OmegaKFilterXarray
    from OmegaK_enhanced import analyze_and_plot_waves
    import os

    os.makedirs(output_dir, exist_ok=True)

    nT   = ds_full.sizes["time"]
    half = win_len_frames // 2

    window_results = []
    win_idx = 0

    centers = range(half, nT - half, hop_frames)
    print(f"Running sliding window: {len(list(centers))} windows "
          f"(win={win_len_frames} frames, hop={hop_frames} frames)")

    for center in range(half, nT - half, hop_frames):
        t0   = center - half
        t1   = center + half
        ds_w = ds_full.isel(time=slice(t0, t1)).copy()

        if verbose:
            print(f"  Window {win_idx:03d}  centre = {ds_full['time'].values[center]}")

        ok = OmegaKFilterXarray(ds_w, data_var=data_var,
                                zero_pad_factor=zero_pad_factor)
        mask = ok.create_filter(**filter_kwargs)

        save_prefix = os.path.join(output_dir, f"window_{win_idx:03d}")

        fig, wave_params, _, physical_waves = analyze_and_plot_waves(
            ok_filter=ok,
            filtered_ds=ds_w,
            filter_mask=mask,
            save_prefix=save_prefix,
            **analysis_kwargs,
        )
        plt.close(fig)   # avoid flooding the notebook with per-window figures

        window_results.append({
            "center_index":  center,
            "center_time":   ds_full["time"].values[center],
            "wave_params":   wave_params,
            "physical_waves": physical_waves,
        })
        win_idx += 1

    print(f"Done. {len(window_results)} windows processed.")
    return window_results


# ---------------------------------------------------------------------------
# Wave family building
# ---------------------------------------------------------------------------

def build_wave_families(
    window_results: list,
    dir_tol_deg: float = 15.0,
    lambda_tol_km: float = 20.0,
    min_detections: int = 1,
) -> list:
    """
    Cluster per-window detections into temporally coherent wave families.

    Grouping is based only on direction and wavelength (not speed), so a
    family can track a wave whose speed evolves over time.

    Parameters
    ----------
    window_results  : output of sliding_window_analysis()
    dir_tol_deg     : max azimuth difference (degrees) to merge into same family
    lambda_tol_km   : max wavelength difference (km) to merge into same family
    min_detections  : minimum number of windows a family must appear in to be kept

    Returns
    -------
    families : list of dicts, each containing:
        family_id         : int
        direction_center  : mean azimuth (degrees, 0=N, 90=E, cw)
        wavelength_center : mean wavelength (km)
        members           : list of per-detection dicts with keys:
                            time, direction, wavelength, speed, period,
                            power, confidence
    """

    def _angular_diff(a, b):
        d = abs(a - b) % 180
        return min(d, 180 - d)

    # Flatten all detections
    all_waves = []
    for r in window_results:
        for w in r["physical_waves"]:
            full_az = float(w["azimuth"])
            all_waves.append({
                "time":         r["center_time"],
                "direction":    full_az % 180,          # [0, 180) for Gabor orientation
                "azimuth_full": full_az,                # [0, 360) for KF sign convention
                "wavelength":   float(w["wavelength"]),
                "speed":        float(w["phase_speed"]),
                "period":       float(w["period"]),
                "power":        float(w.get("power",      np.nan)),
                "confidence":   float(w.get("confidence", np.nan)),
            })

    print(f"Total detections across all windows: {len(all_waves)}")

    # Greedy grouping — centroid is FIXED at the founding detection.
    #
    # Critically, we do NOT update direction_center / wavelength_center
    # after a group is created.  Comparing against a running mean allows
    # slow parameter drift to chain detections arbitrarily far from the
    # founding values (e.g. a wave rotating 60° can be absorbed into one
    # family via 24 incremental 2.5° steps).  Using a fixed founding
    # centroid bounds the total parameter spread to ≤ dir_tol_deg and
    # ≤ lambda_tol_km, which is the intended behaviour.
    #
    # Consequence: a wave whose azimuth rotates by 60° over 6 hours will
    # be split into ~(60/dir_tol_deg) families, each covering one segment
    # of the rotation.  The Voronoi step in build_waves_for_gabor then
    # stitches their reconstructions into a seamless time series.
    groups = []
    for w in all_waves:
        matched = False
        for g in groups:
            if (
                _angular_diff(w["direction"], g["direction_center"]) < dir_tol_deg
                and abs(w["wavelength"] - g["wavelength_center"]) < lambda_tol_km
            ):
                g["members"].append(w)
                # Do NOT update direction_center or wavelength_center here.
                # The founding centroid is the reference for all future matches.
                matched = True
                break
        if not matched:
            groups.append({
                "direction_center":  w["direction"],
                "wavelength_center": w["wavelength"],
                "members":           [w],
            })

    # Per-window deduplication: when the peak-finder splits one wave's power
    # across two nearby k-bins (common during parameter ramps), both detections
    # land in the same family at the same timestamp.  Keep only the
    # strongest-power detection per (family, window-time) pair so the time
    # series has at most one point per window.
    for g in groups:
        seen_times = {}
        for m in g["members"]:
            t = m["time"]
            if t not in seen_times or m["power"] > seen_times[t]["power"]:
                seen_times[t] = m
        n_before = len(g["members"])
        g["members"] = list(seen_times.values())
        n_after = len(g["members"])
        if n_after < n_before:
            print(f"  Dedup: removed {n_before - n_after} weaker co-temporal "
                  f"peak(s) in family "
                  f"(dir~{g['direction_center']:.0f} deg, "
                  f"lam~{g['wavelength_center']:.0f} km)")

    # Filter and sort
    families = [g for g in groups if len(g["members"]) >= min_detections]
    families = sorted(families, key=lambda g: -len(g["members"]))

    for i, f in enumerate(families):
        f["family_id"] = i + 1

    print(f"Wave families found (≥{min_detections} detections): {len(families)}")
    for f in families:
        times  = [m["time"]  for m in f["members"]]
        speeds = [m["speed"] for m in f["members"]]
        print(f"  Family {f['family_id']}: dir={f['direction_center']:.1f}°  "
              f"λ={f['wavelength_center']:.1f} km  "
              f"n={len(f['members'])}  "
              f"speed {min(speeds):.0f}–{max(speeds):.0f} m/s")

    return families


# ---------------------------------------------------------------------------
# Convert to xarray for Gabor / KF handoff
# ---------------------------------------------------------------------------

def families_to_xarray(
    families: list,
    ds_truth: xr.Dataset = None,
) -> xr.Dataset:
    """
    Convert wave-family list to an xarray Dataset suitable for handoff to
    the Gabor filter and Kalman-smoother pipeline stages.

    Each family is represented along a ``family`` dimension.  Within each
    family, detections are interpolated onto a uniform ``time`` grid
    (union of all window-centre times), with NaN where the family had no
    detection.

    Parameters
    ----------
    families  : output of build_wave_families()
    ds_truth  : optional synthetic truth Dataset (from build_synthetic_wave).
                If provided, truth variables are merged in for easy comparison.

    Returns
    -------
    ds : xr.Dataset  with dimensions (family, time) and variables:
            phase_speed   – m/s
            wavelength    – km
            azimuth       – degrees (0=N, 90=E, cw)
            period        – minutes
            power         – spectral power weight
            confidence    – detection confidence score
         plus (if ds_truth provided):
            truth_phase_speed
            truth_wavelength
            truth_azimuth
         and attributes describing the grouping parameters.
    """
    if not families:
        raise ValueError("families list is empty")

    # Build a common sorted time axis from all detection times
    all_times = sorted({
        m["time"]
        for f in families
        for m in f["members"]
    })
    time_index = np.array(all_times, dtype="datetime64[ns]")

    n_fam  = len(families)
    n_time = len(time_index)

    # Allocate arrays (NaN where no detection)
    speed_arr  = np.full((n_fam, n_time), np.nan)
    wl_arr     = np.full((n_fam, n_time), np.nan)
    az_arr     = np.full((n_fam, n_time), np.nan)
    period_arr = np.full((n_fam, n_time), np.nan)
    power_arr  = np.full((n_fam, n_time), np.nan)
    conf_arr   = np.full((n_fam, n_time), np.nan)

    time_to_idx = {t: i for i, t in enumerate(time_index)}

    for fi, fam in enumerate(families):
        for m in fam["members"]:
            ti = time_to_idx[m["time"]]
            speed_arr [fi, ti] = m["speed"]
            wl_arr    [fi, ti] = m["wavelength"]
            az_arr    [fi, ti] = m["direction"]
            period_arr[fi, ti] = m["period"]
            power_arr [fi, ti] = m["power"]
            conf_arr  [fi, ti] = m["confidence"]

    family_ids = np.array([f["family_id"] for f in families])

    coords = {
        "family":       family_ids,
        "time":         time_index,
        "direction_center":  ("family", [f["direction_center"]  for f in families]),
        "wavelength_center": ("family", [f["wavelength_center"] for f in families]),
        "n_detections":      ("family", [len(f["members"])      for f in families]),
    }

    ds = xr.Dataset(
        {
            "phase_speed": xr.DataArray(
                speed_arr,  dims=("family", "time"), coords=coords,
                attrs={"long_name": "Measured phase speed", "units": "m/s"},
            ),
            "wavelength": xr.DataArray(
                wl_arr,     dims=("family", "time"), coords=coords,
                attrs={"long_name": "Measured wavelength",  "units": "km"},
            ),
            "azimuth": xr.DataArray(
                az_arr,     dims=("family", "time"), coords=coords,
                attrs={"long_name": "Propagation azimuth (0=N, 90=E, cw)",
                       "units": "degrees"},
            ),
            "period": xr.DataArray(
                period_arr, dims=("family", "time"), coords=coords,
                attrs={"long_name": "Wave period", "units": "minutes"},
            ),
            "power": xr.DataArray(
                power_arr,  dims=("family", "time"), coords=coords,
                attrs={"long_name": "Spectral power weight"},
            ),
            "confidence": xr.DataArray(
                conf_arr,   dims=("family", "time"), coords=coords,
                attrs={"long_name": "Detection confidence (0–1)"},
            ),
        },
        attrs={
            "description": "Omega-k sliding-window detections, grouped into wave families",
            "n_families":  n_fam,
            "pipeline_stage": "omega_k_output / gabor_kf_input",
            "azimuth_convention": "0=North, 90=East, clockwise",
        },
    )

    # Merge truth if provided
    if ds_truth is not None:
        for var, truth_var in [("phase_speed", "truth_phase_speed"),
                               ("wavelength",  "truth_wavelength"),
                               ("azimuth",     "truth_azimuth")]:
            src = var if var in ds_truth else None
            if src is not None:
                truth_on_grid = ds_truth[src].interp(
                    time=xr.DataArray(time_index, dims="time"),
                    method="linear",
                )
                ds[truth_var] = truth_on_grid.expand_dims(
                    {"family": family_ids}, axis=0
                ).assign_coords(coords)

    return ds


# ---------------------------------------------------------------------------
# Diagnostic time series plot
# ---------------------------------------------------------------------------

def spatial_resolutions(lam_km: float, domain_size_km: float) -> tuple:
    """
    Theoretical spatial resolution limits for wavelength and azimuth.

    Both are set by the spatial aperture of the image (domain_size_km),
    analogous to how dv is set by the temporal aperture (window duration).

    Parameters
    ----------
    lam_km         : wavelength (km)
    domain_size_km : spatial extent of the image domain (km).
                     Use the smaller of east/north extent to be conservative.

    Returns
    -------
    d_lambda_km : wavelength resolution (km)   delta_lambda = lambda^2 / L
    d_theta_deg : azimuth resolution (degrees) delta_theta  = (lambda/L) * (180/pi)

    Notes
    -----
    These are hard limits set by spatial aperture. Zero-padding in space
    sharpens peak *location* but does not improve true resolution, just as
    temporal zero-padding sharpens speed estimates without reducing delta_v.
    """
    d_lambda_km = lam_km ** 2 / domain_size_km
    d_theta_deg = np.rad2deg(lam_km / domain_size_km)
    return d_lambda_km, d_theta_deg


def plot_wave_family_timeseries(
    families: list,
    ds_truth: xr.Dataset = None,
    win_len_frames: int = None,
    dt_min: float = None,
    domain_size_km: float = None,
    fig_title: str = "Wave Family Time Series",
    save_path: str = None,
) -> plt.Figure:
    """
    Three-panel time series plot for each wave family:
      Top    : Phase speed (m/s)      -- error bars: dv = lambda / T_win  (temporal)
      Middle : Wavelength (km)        -- error bars: d_lambda = lambda^2 / L  (spatial)
      Bottom : Azimuth (degrees, unwrapped) -- error bars: d_theta = lambda/L (spatial)

    Truth overlays are drawn if ds_truth is provided.

    Parameters
    ----------
    families       : output of build_wave_families()
    ds_truth       : synthetic truth Dataset from build_synthetic_wave()
    win_len_frames : frames per analysis window  -> enables dv speed bars
    dt_min         : temporal sampling (minutes) -> enables dv speed bars
    domain_size_km : spatial extent of image (km); use min(east_extent,
                     north_extent) to be conservative -> enables d_lambda and d_theta bars
    fig_title      : overall figure title
    save_path      : if given, save figure here

    Returns
    -------
    fig : matplotlib Figure
    """
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    n_fam = len(families)
    if n_fam == 0:
        print("No families to plot.")
        return None

    fig, axes = plt.subplots(3, n_fam, figsize=(7 * n_fam, 9), squeeze=False,
                             sharex="col")

    truth_time = None
    if ds_truth is not None:
        truth_time = ds_truth["time"].values.astype("datetime64[ns]")

    for fi, fam in enumerate(families):
        ax_speed = axes[0, fi]
        ax_wl    = axes[1, fi]
        ax_az    = axes[2, fi]

        members = sorted(fam["members"], key=lambda x: x["time"])
        times   = np.array([m["time"]       for m in members], dtype="datetime64[ns]")
        speeds  = np.array([m["speed"]      for m in members])
        wls     = np.array([m["wavelength"] for m in members])
        azs     = np.array([m["direction"]  for m in members])

        col    = colors[fi % len(colors)]
        lam_km = fam["wavelength_center"]

        # --- Compute resolutions ---
        dv, d_lam, d_theta = None, None, None

        if win_len_frames is not None and dt_min is not None:
            T_win_sec = win_len_frames * dt_min * 60.0
            dv = (lam_km * 1000.0) / T_win_sec

        if domain_size_km is not None:
            d_lam, d_theta = spatial_resolutions(lam_km, domain_size_km)

        # Title summarises whichever resolutions are available
        res_parts = [f"lambda={lam_km:.0f} km", f"dir={fam['direction_center']:.0f} deg"]
        if dv      is not None: res_parts.append(f"dv={dv:.1f} m/s")
        if d_lam   is not None: res_parts.append(f"d_lam={d_lam:.1f} km")
        if d_theta is not None: res_parts.append(f"d_theta={d_theta:.1f} deg")

        # --- Phase speed ---
        ax_speed.plot(times, speeds, "o-", color=col, ms=5, label="Measured")
        if dv is not None:
            ax_speed.errorbar(times, speeds, yerr=dv, fmt="none",
                              color=col, alpha=0.4, capsize=3)
        if ds_truth is not None and "phase_speed" in ds_truth:
            ax_speed.plot(truth_time, ds_truth["phase_speed"].values,
                          "r-", lw=1.5, label="Truth")
        ax_speed.set_ylabel("Phase speed (m/s)")
        ax_speed.grid(True, alpha=0.3)
        ax_speed.legend(fontsize=8)
        ax_speed.set_title(f"Family {fam['family_id']}\n" + "  ".join(res_parts),
                           fontsize=9)

        # --- Wavelength ---
        ax_wl.plot(times, wls, "s-", color=col, ms=5, label="Measured")
        if d_lam is not None:
            ax_wl.errorbar(times, wls, yerr=d_lam, fmt="none",
                           color=col, alpha=0.4, capsize=3)
        if ds_truth is not None and "wavelength" in ds_truth:
            ax_wl.plot(truth_time, ds_truth["wavelength"].values,
                       "r-", lw=1.5, label="Truth")
        ax_wl.set_ylabel("Wavelength (km)")
        ax_wl.grid(True, alpha=0.3)
        ax_wl.legend(fontsize=8)

        # --- Azimuth (unwrapped) ---
        azs_unwrapped = np.unwrap(azs, period=360)
        ax_az.plot(times, azs_unwrapped, "^-", color=col, ms=5, label="Measured")
        if d_theta is not None:
            ax_az.errorbar(times, azs_unwrapped, yerr=d_theta, fmt="none",
                           color=col, alpha=0.4, capsize=3)
        if ds_truth is not None and "azimuth" in ds_truth:
            truth_az_unwrapped = np.unwrap(ds_truth["azimuth"].values, period=360)
            ax_az.plot(truth_time, truth_az_unwrapped, "r-", lw=1.5, label="Truth")
        ax_az.set_ylabel("Azimuth (deg, 0=N cw)")
        ax_az.grid(True, alpha=0.3)
        ax_az.legend(fontsize=8)

        for ax in [ax_speed, ax_wl, ax_az]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
            ax.tick_params(axis="x", rotation=30)

    fig.suptitle(fig_title, fontsize=13, weight="bold", y=1.01)
    plt.tight_layout()

    if save_path is not None:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {save_path}")

    return fig
