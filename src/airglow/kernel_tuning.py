"""
kernel_tuning.py
-----------------
Builds the offline-smoothed orientation/wavelength signal that drives the
second-pass Gabor kernel. Runs `smoothing.py`'s offline RTS engine on
first-pass per-wave observations, in place of `phase_speed_kalman.py`'s
causal `analyze_all_waves_kalman`, for this one purpose only.

Everything this module returns is an internal/diagnostic tuning INPUT --
never written to the permanent output schema. `final_estimation.py` (run on
the second-pass reconstruction, after lobe disambiguation) is what actually
produces the persisted final tables.

Deliberately narrower than `analyze_all_waves_kalman`: only orientation and
wavelength are smoothed here, since those are the only two quantities that
shape a Gabor kernel (orientation + passband center wavelength). Phase
speed/omega tracking feeds final per-frame speed estimates, not kernel
shape, and is entirely out of scope here -- this module runs no omega
tracker and does no dominant-wave/dwell-time selection.

Each wave's per-frame validity here is self-contained (`amp > 0` and the
SNR floor, matching `smooth_orientation`/`smooth_wavenumber`'s own built-in
gating) -- it does not additionally gate on an omega tracker's own validity
mask the way the causal trackers in `phase_speed_kalman.py` cross-gate
orientation/wavenumber against omega. This is a deliberate simplification,
acceptable because this module's output is diagnostic-only (feeding the
second-pass kernel, not a persisted quantity), not an oversight.
"""

import numpy as np

from airglow.smoothing import smooth_orientation, smooth_wavenumber


def build_kernel_tuning_signal(
    waves: list,
    kx_obs_list: list,
    ky_obs_list: list,
    complex_amplitudes: list,
    time_vals: np.ndarray,
    dt: float,
    btheta_rad: float,
    *,
    lambda_h_min_km: float = 5.0,
    lambda_h_max_km: float = 150.0,
    kf_q_theta: float = 1e-8,
    kf_q_dtheta: float = 1e-12,
    kf_r_theta_deg: float = 15.0,
    wavelength_drift_frac_per_hr: float = 0.03,
    kf_r_kh_frac: float = 0.20,
    snr_min_amplitude: float = 0.05,
    chi2_gate: float = 9.0,
    verbose: bool = True,
) -> list:
    """
    Build the offline-smoothed per-frame orientation/wavelength signal that
    feeds step 4's second-pass Gabor kernel construction.

    Parameters
    ----------
    waves : list of dict
        First-pass wave list (`wave_identity.resolve_frame_ownership`'s
        output). Must have `'azimuth'`, `'wavelength_km'`; may optionally
        have `'orientation_deg_series'`/`'wavelength_km_series'` (the
        per-frame series from frame-ownership resolution), used ONLY as the
        adaptive-kernel-center reference for `kernel_mismatch` -- not
        overwritten, and not the same thing as this function's own smoothed
        output under the same key names.
    kx_obs_list, ky_obs_list : list of ndarray (n_time,)
        First-pass amplitude-weighted spatial phase gradient, one per wave
        -- `reconstruct_all_waves`'s own `kx_obs_arrays`/`ky_obs_arrays`
        return values (Step 5/first-pass reconstruction). Raw, unsmoothed.
    complex_amplitudes : list of ndarray (n_time,) complex
        First-pass cross-correlation phasor, one per wave -- also
        `reconstruct_all_waves`'s own return value.
    time_vals : ndarray of datetime64
        Time grid matching `kx_obs_list[i]`/`ky_obs_list[i]` (whatever
        slice the caller passes -- this function is grid-agnostic, same
        philosophy as `wave_identity.py`'s `resolve_collisions`; edge-frame
        exclusion, if wanted, is the caller's decision, matching how
        `airglow_pipeline.py`'s existing Step 8 already slices before
        calling `analyze_all_waves_kalman`).
    dt : float
        Timestep, seconds.
    btheta_rad : float
        Gabor kernel full bandwidth (radians) -- used only for
        `kernel_mismatch`'s threshold.
    lambda_h_min_km, lambda_h_max_km : float
        Physical wavelength bounds, unchanged from `analyze_all_waves_kalman`'s
        defaults (5-150 km) -- converted to `k_h_min`/`k_h_max` clamp bounds.
    kf_q_theta, kf_q_dtheta, kf_r_theta_deg : float
        Passed straight through to `smooth_orientation` (see its docstring
        for what each governs).
    wavelength_drift_frac_per_hr : float
        Passed straight through to `smooth_wavenumber`. Default 0.03
        (3%/hr), empirically derived -- see that function's docstring.
    kf_r_kh_frac : float
        Measurement-noise fraction for the wavenumber tracker, also reused
        as `wavelength_mismatch_frac` for `wavelength_kernel_mismatch`.
    snr_min_amplitude, chi2_gate : float
        Passed straight through to both trackers.

    Returns
    -------
    list of dict, one per wave (same order as `waves`), each with:
        label                          : str, for logging/QC only.
        orientation_deg_series         : (n_time,) float, [0, 180) --
            OFFLINE-SMOOTHED axis. This is the value step 4 should use to
            build the second-pass Gabor kernel (same key convention
            `reconstruct_all_waves` already expects for `waves_kf`).
        wavelength_km_series           : (n_time,) float -- OFFLINE-SMOOTHED
            wavelength, same purpose.
        orientation_deg_series_causal,
        wavelength_km_series_causal    : (n_time,) float -- the forward-only
            (causal) pass, kept for QC comparison against the current
            production pipeline's causal output. NOT what step 4 should
            consume.
        kernel_mismatch, wavelength_kernel_mismatch : (n_time,) bool,
            evaluated against the CAUSAL pass, matching the ported
            originals' own semantics (`_kalman_theta_tracker`/
            `_kalman_kh_tracker` compute this against their own causal
            estimate; smoothing.py's `smooth_orientation`/
            `smooth_wavenumber` do the same for the same reason -- see
            their own docstrings).
        n_valid_frames                 : int, diagnostic -- count of frames
            with real signal (`amp > 0`) for this wave.
        q_kh_used                      : float, diagnostic -- the actual
            per-wave derived `q_kh` (scale-aware, varies by wave; see
            `smooth_wavenumber`'s docstring).
    """
    n_waves = len(waves)
    n_time = len(time_vals)
    if n_waves == 0:
        return []

    k_h_min = 2.0 * np.pi / (lambda_h_max_km * 1000.0)
    k_h_max = 2.0 * np.pi / (lambda_h_min_km * 1000.0)
    r_theta_base_rad = np.deg2rad(kf_r_theta_deg)

    if verbose:
        print("=" * 70)
        print("KERNEL-TUNING SIGNAL (offline, step 3)")
        print("=" * 70)
        print(f"  Waves: {n_waves}   Frames: {n_time}   dt={dt:.0f} s")
        print(f"  wavelength_drift_frac_per_hr={wavelength_drift_frac_per_hr:.3f} "
              f"(empirically derived, see smoothing.py)")

    out = []
    for wi, wave in enumerate(waves):
        A = complex_amplitudes[wi]
        amp_obs = np.abs(A)
        theta_init_rad = np.deg2rad(float(wave["azimuth"]))
        lam_m = float(wave["wavelength_km"]) * 1000.0
        k_h_init = float(np.clip(
            2.0 * np.pi / lam_m if lam_m > 0 else k_h_min, k_h_min, k_h_max,
        ))
        r_kh_base = kf_r_kh_frac * k_h_init

        kx = np.asarray(kx_obs_list[wi], dtype=float)
        ky = np.asarray(ky_obs_list[wi], dtype=float)

        _orient_series = wave.get("orientation_deg_series", None)
        _ks_deg = (np.asarray(_orient_series, dtype=float)
                   if _orient_series is not None else None)
        _wl_series = wave.get("wavelength_km_series", None)
        _kwl_km = (np.asarray(_wl_series, dtype=float)
                   if _wl_series is not None else None)

        th = smooth_orientation(
            kx, ky, amp_obs, dt, theta_init_rad, btheta_rad,
            q_theta=kf_q_theta, q_dtheta=kf_q_dtheta,
            r_theta_base_rad=r_theta_base_rad,
            snr_min_amplitude=snr_min_amplitude, chi2_gate=chi2_gate,
            innov_valid_mask=None, kernel_center_series_deg=_ks_deg,
        )

        kh_obs = np.sqrt(kx ** 2 + ky ** 2)
        kh_obs = np.where((kh_obs > 1e-6) & np.isfinite(kh_obs), kh_obs, np.nan)
        n_valid_kh = int(np.sum(np.isfinite(kh_obs)))

        if n_valid_kh == 0:
            kh_smooth = np.full(n_time, k_h_init)
            kh_est = np.full(n_time, k_h_init)
            wl_mismatch = np.zeros(n_time, dtype=bool)
            q_kh_used = np.nan
            if verbose:
                print(f"  [{wi + 1}/{n_waves}] {wave.get('label', wi)}: "
                      f"all k_h_obs NaN -- falling back to ω-k reference "
                      f"({wave['wavelength_km']:.1f} km).")
        else:
            kh = smooth_wavenumber(
                kh_obs, amp_obs, dt, k_h_init,
                wavelength_drift_frac_per_hr=wavelength_drift_frac_per_hr,
                r_kh_base=r_kh_base, k_h_min=k_h_min, k_h_max=k_h_max,
                valid_mask=None, kernel_wavelength_series_km=_kwl_km,
                wavelength_mismatch_frac=kf_r_kh_frac,
            )
            kh_smooth = kh["kh_smooth"]
            kh_est = kh["kh_est"]
            wl_mismatch = kh["wavelength_kernel_mismatch"]
            q_kh_used = kh["q_kh_used"]

        orient_smooth_deg = th["theta_smooth_deg"] % 180.0
        orient_causal_deg = th["theta_est_deg"] % 180.0
        lam_smooth_km = (2.0 * np.pi / kh_smooth) / 1000.0
        lam_causal_km = (2.0 * np.pi / kh_est) / 1000.0

        n_valid = int(np.sum(amp_obs > 0))

        rec = dict(
            label=wave.get("label", f"wave{wi}"),
            orientation_deg_series=orient_smooth_deg,
            wavelength_km_series=lam_smooth_km,
            orientation_deg_series_causal=orient_causal_deg,
            wavelength_km_series_causal=lam_causal_km,
            kernel_mismatch=th["kernel_mismatch"],
            wavelength_kernel_mismatch=wl_mismatch,
            n_valid_frames=n_valid,
            q_kh_used=q_kh_used,
        )
        out.append(rec)

        if verbose:
            mm_frac = np.mean(rec["kernel_mismatch"]) if n_time > 0 else 0.0
            wmm_frac = np.mean(rec["wavelength_kernel_mismatch"]) if n_time > 0 else 0.0
            print(f"  [{wi + 1}/{n_waves}] {rec['label']}: {n_valid} valid frames, "
                  f"q_kh={q_kh_used:.2e}, kernel_mismatch={100 * mm_frac:.0f}%, "
                  f"wavelength_kernel_mismatch={100 * wmm_frac:.0f}%")

    return out
