"""
gabor_reconstruction.py
-----------------------
Gabor wave reconstruction for the airglow pipeline.

Provides a unified entry point ``reconstruct_all_waves()`` that works in two
modes depending on whether per-frame parameter arrays are supplied:

Fixed-kernel mode (single omega-k → Gabor):
    Each wave uses one kernel built from a fixed orientation_deg and
    wavelength_km (the omega-k detection values) for the whole night.

Adaptive-kernel mode (sliding-window omega-k → Gabor):
    Each wave receives per-frame orientation_deg_series and
    wavelength_km_series arrays interpolated from the sliding-window
    omega-k family estimates.  A new Gabor kernel is built for each
    frame, allowing the filter to track slowly-drifting wave parameters.

In both modes the outputs are identical in structure so all downstream
steps (cross-validation, Kalman tracker) work unchanged.

Public API
----------
reconstruct_all_waves(data, waves, dx_km, wavelength_resolution_km, Btheta, ...)
    Unified dispatcher — use this in notebooks. Bf (the Gabor frequency
    bandwidth) is derived per wave from wavelength_resolution_km and that
    wave's own wavelength_km, not passed in directly — see
    compute_bf_for_wavelength() in wave_clustering.py for why a single Bf
    can't be correct across all wavelengths at once.
"""

import numpy as np
from scipy import signal
from scipy.stats import pearsonr

# The existing Gabor implementation — imported, never reimplemented here.
from airglow.cluster_reconstruction import create_gabor_kernel
from airglow.wave_clustering import compute_bf_for_wavelength


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def reconstruct_all_waves(
    data_filtered_temporal,
    waves,
    dx_km,
    wavelength_resolution_km,
    Btheta=np.pi / 4,
    n_stds=3.0,
    rms_calibrate=True,
):
    """
    Reconstruct detected waves using Gabor filtering.

    Dispatches to fixed-kernel or adaptive-kernel mode depending on
    whether the wave dicts contain per-frame parameter arrays.

    Parameters
    ----------
    data_filtered_temporal : np.ndarray, shape (n_time, ny, nx)
        Input image cube (temporally filtered, spatial-mean removed).

    waves : list of dict
        One dict per wave.  Required keys (fixed-kernel mode):
            orientation_deg  – propagation orientation in [0°, 180°)
            wavelength_km    – horizontal wavelength in km
            label            – string label for printouts
        Optional keys (adaptive-kernel mode, both must be present):
            orientation_deg_series  – np.ndarray (n_time,)  [0°, 180°)
            wavelength_km_series    – np.ndarray (n_time,)  km

    dx_km : float
        Pixel spacing in km (east = north assumed).

    wavelength_resolution_km : float
        Target wavelength bandwidth (km) for each wave's Gabor kernel.  Bf is
        computed per wave (fixed mode) / per kernel rebuild (adaptive mode)
        from this wave's own wavelength_km via compute_bf_for_wavelength, so
        the kernel's absolute km-wide selectivity matches this value at every
        wave's own scale, instead of one Bf computed at a fixed reference
        wavelength and reused everywhere (correct only near that reference).

    Btheta : float
        Gabor angular bandwidth (radians).  Wavelength-independent, so this
        stays a single value shared across all waves.

    n_stds : float
        Kernel half-width in sigma units.

    rms_calibrate : bool
        If True, scale the composite so its RMS matches the input RMS.

    Returns
    -------
    wave_cubes : list of np.ndarray, each (n_time, ny, nx)
        Per-wave real-valued reconstructions.

    composite : np.ndarray, (n_time, ny, nx)
        Sum of all wave_cubes (RMS-calibrated if requested).

    complex_amplitudes : list of np.ndarray, each (n_time,) complex
        Cross-correlation phasors A(t) = mean(conj(conv_t) * conv_{t-1}).
        angle(A) / dt gives instantaneous angular frequency.

    kx_obs_arrays : list of np.ndarray, each (n_time,)
        Amplitude-weighted mean east wavenumber (rad/m) per frame.

    ky_obs_arrays : list of np.ndarray, each (n_time,)
        Amplitude-weighted mean north wavenumber (rad/m) per frame.
    """
    adaptive = (
        "orientation_deg_series" in waves[0]
        and "wavelength_km_series" in waves[0]
    )

    if adaptive:
        return _reconstruct_adaptive(
            data_filtered_temporal, waves, dx_km, wavelength_resolution_km, Btheta, n_stds,
            rms_calibrate,
        )
    else:
        return _reconstruct_fixed(
            data_filtered_temporal, waves, dx_km, wavelength_resolution_km, Btheta, n_stds,
            rms_calibrate,
        )


# ---------------------------------------------------------------------------
# Fixed-kernel reconstruction
# ---------------------------------------------------------------------------

def _reconstruct_fixed(
    data_filtered_temporal,
    waves,
    dx_km,
    wavelength_resolution_km,
    Btheta,
    n_stds,
    rms_calibrate,
):
    """Fixed Gabor kernel per wave, applied to all frames."""
    n_time, ny, nx = data_filtered_temporal.shape
    n_waves = len(waves)
    wave_cubes, complex_amplitudes, kx_obs_arrays, ky_obs_arrays = [], [], [], []

    for wi, wave in enumerate(waves):
        label           = wave["label"]
        orientation_deg = wave["orientation_deg"]
        wavelength_km   = wave["wavelength_km"]
        w_n_stds        = wave.get("n_stds", n_stds)
        Bf              = compute_bf_for_wavelength(wavelength_resolution_km, wavelength_km)

        print(
            f"  [{wi+1}/{n_waves}] {label}: "
            f"lambda={wavelength_km:.1f} km, orient={orientation_deg:.1f} deg, "
            f"Bf={Bf:.3f} octaves, n_stds={w_n_stds}  [fixed kernel]"
        )

        kernel = create_gabor_kernel(
            wavelength_km=wavelength_km,
            orientation_deg=orientation_deg,
            resolution_km=dx_km,
            n_stds=w_n_stds,
            Bf=Bf,
            Btheta=Btheta,
        )
        g_sum = float(np.sum(np.abs(kernel)))
        print(f"    kernel: {kernel.shape[0]}x{kernel.shape[1]} px,  G_sum = {g_sum:.4e}")

        cube      = np.zeros((n_time, ny, nx), dtype=np.float64)
        cmplx_amp = np.zeros(n_time, dtype=np.complex128)
        kx_obs_w  = np.full(n_time, np.nan)
        ky_obs_w  = np.full(n_time, np.nan)
        scale     = 2.0 / g_sum
        dx_m      = dx_km * 1000.0
        prev_conv = None

        for t in range(n_time):
            conv    = signal.fftconvolve(data_filtered_temporal[t], kernel, mode="same")
            cube[t] = np.real(conv) * scale
            if prev_conv is not None:
                cmplx_amp[t] = np.mean(np.conj(conv) * prev_conv) * (scale ** 2)

            kx_obs_w[t], ky_obs_w[t] = _spatial_phase_gradient(conv, dx_m)
            prev_conv = conv

            if t % 10 == 0 or t == n_time - 1:
                print(f"    frame {t:3d}/{n_time}", end="\r", flush=True)

        print(f"    frame {n_time}/{n_time} -- done        ")
        wave_cubes.append(cube)
        complex_amplitudes.append(cmplx_amp)
        kx_obs_arrays.append(kx_obs_w)
        ky_obs_arrays.append(ky_obs_w)

    composite = _build_composite(
        data_filtered_temporal, wave_cubes, rms_calibrate
    )
    return wave_cubes, composite, complex_amplitudes, kx_obs_arrays, ky_obs_arrays


# ---------------------------------------------------------------------------
# Adaptive-kernel reconstruction  (sliding-window omega-k → Gabor)
# ---------------------------------------------------------------------------

def _reconstruct_adaptive(
    data_filtered_temporal,
    waves,
    dx_km,
    wavelength_resolution_km,
    Btheta,
    n_stds,
    rms_calibrate,
):
    """
    Per-frame Gabor kernel rebuilt from sliding-window omega-k estimates.

    Each frame uses orientation_deg_series[t] and wavelength_km_series[t]
    to construct a kernel tuned to the wave parameters at that moment.

    Cross-correlation phasors A(t) = mean(conj(conv_t) * conv_{t-1}) are
    still computed; when adjacent frames use slightly different kernels this
    remains a valid (if slightly noisier) phase-advance estimate provided
    the kernel changes slowly relative to the wave period.
    """
    n_time, ny, nx = data_filtered_temporal.shape
    n_waves = len(waves)
    wave_cubes, complex_amplitudes, kx_obs_arrays, ky_obs_arrays = [], [], [], []

    for wi, wave in enumerate(waves):
        label         = wave["label"]
        orient_series = np.asarray(wave["orientation_deg_series"])   # (n_time,)
        wl_series     = np.asarray(wave["wavelength_km_series"])     # (n_time,)
        w_n_stds      = wave.get("n_stds", n_stds)

        n_det = int(np.sum(np.isfinite(orient_series)))
        with np.errstate(all='ignore'):
            o_lo = np.nanmin(orient_series); o_hi = np.nanmax(orient_series)
            w_lo = np.nanmin(wl_series);     w_hi = np.nanmax(wl_series)
        print(
            f'  [{wi+1}/{n_waves}] {label}  [adaptive kernel]\n'
            f'    orient: {o_lo:.1f}-{o_hi:.1f} deg, '
            f'wavelength: {w_lo:.1f}-{w_hi:.1f} km, '
            f'active frames: {n_det}/{n_time}'
        )


        cube      = np.zeros((n_time, ny, nx), dtype=np.float64)
        cmplx_amp = np.zeros(n_time, dtype=np.complex128)
        kx_obs_w  = np.full(n_time, np.nan)
        ky_obs_w  = np.full(n_time, np.nan)
        dx_m      = dx_km * 1000.0
        prev_conv  = None
        prev_scale = None

        # Cache kernel builds: only rebuild when parameters change by more
        # than a small threshold (saves time when parameters vary smoothly).
        _cache_orient = None
        _cache_wl     = None
        _cache_kernel = None
        _cache_gsum   = None
        _ORIENT_TOL   = 0.5   # degrees
        _WL_TOL       = 0.5   # km

        for t in range(n_time):

            od = float(orient_series[t])
            wl = float(wl_series[t])

            # NaN parameters -> family not detected in this frame's window.
            # Output zero (no reconstruction) and reset prev_conv so the
            # cross-correlation phasor is not computed across the gap.
            if not np.isfinite(od) or not np.isfinite(wl):
                cube[t]      = 0.0
                cmplx_amp[t] = 0.0 + 0.0j
                prev_conv    = None   # break the phasor chain
                prev_scale   = None
                if t % 10 == 0 or t == n_time - 1:
                    print(f'    frame {t:3d}/{n_time} [no detection]',
                          end='\r', flush=True)
                continue

            # Rebuild kernel only when parameters shift noticeably
            if (
                _cache_kernel is None
                or abs(od - _cache_orient) > _ORIENT_TOL
                or abs(wl - _cache_wl)     > _WL_TOL
            ):
                Bf = compute_bf_for_wavelength(wavelength_resolution_km, wl)
                _cache_kernel = create_gabor_kernel(
                    wavelength_km=wl,
                    orientation_deg=od,
                    resolution_km=dx_km,
                    n_stds=w_n_stds,
                    Bf=Bf,
                    Btheta=Btheta,
                )
                _cache_gsum   = float(np.sum(np.abs(_cache_kernel)))
                _cache_orient = od
                _cache_wl     = wl

            kernel = _cache_kernel
            scale  = 2.0 / _cache_gsum if _cache_gsum > 0 else 1.0

            conv    = signal.fftconvolve(data_filtered_temporal[t], kernel, mode="same")
            cube[t] = np.real(conv) * scale

            if prev_conv is not None:
                # Cross-correlation phasor with mixed-kernel correction:
                # use geometric mean of the two scales so the amplitude
                # normalization is consistent across the kernel change.
                s_mix = 0.5 * (scale + prev_scale)
                cmplx_amp[t] = np.mean(np.conj(conv) * prev_conv) * (s_mix ** 2)

            kx_obs_w[t], ky_obs_w[t] = _spatial_phase_gradient(conv, dx_m)
            prev_conv  = conv
            prev_scale = scale

            if t % 10 == 0 or t == n_time - 1:
                print(f"    frame {t:3d}/{n_time}", end="\r", flush=True)

        print(f"    frame {n_time}/{n_time} -- done        ")
        wave_cubes.append(cube)
        complex_amplitudes.append(cmplx_amp)
        kx_obs_arrays.append(kx_obs_w)
        ky_obs_arrays.append(ky_obs_w)

    composite = _build_composite(
        data_filtered_temporal, wave_cubes, rms_calibrate
    )
    return wave_cubes, composite, complex_amplitudes, kx_obs_arrays, ky_obs_arrays


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _spatial_phase_gradient(conv, dx_m):
    """
    Amplitude-squared weighted spatial phase gradient of a complex convolution.

    Wrap-free formula:
        k_e = Im(conj(c) * dc/d_east)  / |c|^2
        k_n = Im(conj(c) * dc/d_north) / |c|^2

    Returns (kx_mean, ky_mean) in rad/m.  Returns (nan, nan) when the
    convolution is effectively zero everywhere.
    """
    abs_sq = np.abs(conv) ** 2
    weight_sum = abs_sq.sum()
    if weight_sum == 0:
        return np.nan, np.nan

    dconv_dn, dconv_de = np.gradient(conv, dx_m, dx_m)
    with np.errstate(invalid="ignore", divide="ignore"):
        k_n = np.where(abs_sq > 0, np.imag(np.conj(conv) * dconv_dn) / abs_sq, 0.0)
        k_e = np.where(abs_sq > 0, np.imag(np.conj(conv) * dconv_de) / abs_sq, 0.0)

    kx_mean = float(np.sum(abs_sq * k_e) / weight_sum)
    ky_mean = float(np.sum(abs_sq * k_n) / weight_sum)
    return kx_mean, ky_mean


def _build_composite(data_filtered_temporal, wave_cubes, rms_calibrate):
    """Sum wave cubes and optionally RMS-calibrate to match input."""
    composite = np.sum(wave_cubes, axis=0)
    if rms_calibrate:
        orig_rms  = np.sqrt(np.mean(np.asarray(data_filtered_temporal, dtype=np.float64) ** 2))
        recon_rms = np.sqrt(np.mean(composite ** 2))
        print(f"  orig_rms = {orig_rms:.4f},  recon_rms = {recon_rms:.4f}")
        if recon_rms > 0 and np.isfinite(recon_rms):
            scale_cal = orig_rms / recon_rms
            composite  = composite * scale_cal
            wave_cubes[:] = [c * scale_cal for c in wave_cubes]
            print(f"  RMS calibration scale = {scale_cal:.4f}")
        else:
            print("  WARNING: recon_rms is zero or non-finite — calibration skipped")
    return composite


def check_duplicate_waves(wave_cubes, waves, threshold=0.5):
    """
    Flag pairs of reconstructed waves that are suspiciously similar to each
    other, suggesting they may be the same physical wave split into two
    families by detection noise rather than genuinely distinct waves.

    Family grouping (`wave_identity.cluster_detections_into_families`) and
    the Gabor kernel bandwidth both reduce this risk, but neither
    guarantees it away -- two families
    can still survive as separate detections while their Gabor
    reconstructions end up highly correlated with each other. For a
    climatology this matters directly: counting the same physical wave as
    two separate detections inflates occurrence statistics even if both
    detections' wavelength/direction would land in the same output bin.

    This checks the reconstructions directly against each other (not
    against the independent omega-k reference used in Section 8's
    cross-validation) -- a direct, cheap test purpose-built for this
    specific failure mode.

    Parameters
    ----------
    wave_cubes : list of np.ndarray, each (n_time, ny, nx)
        Output of reconstruct_all_waves().
    waves : list of dict
        The corresponding wave dicts (for labeling in the printed report).
    threshold : float
        Mean frame-by-frame Pearson r at or above which a pair is flagged.

    Returns
    -------
    flagged : list of (i, j, mean_r) tuples, i < j, for pairs at or above
        threshold.
    corr_matrix : np.ndarray, shape (n_waves, n_waves)
        Symmetric matrix of mean frame-by-frame Pearson r between every
        pair (diagonal is 1.0). Frames where either cube is all-zero
        (unowned/inactive, see `wave_identity.resolve_frame_ownership`)
        are skipped rather than counted as trivially agreeing.
    """
    n = len(wave_cubes)
    corr_matrix = np.eye(n)
    flagged = []

    for i in range(n):
        for j in range(i + 1, n):
            cube_i, cube_j = wave_cubes[i], wave_cubes[j]
            n_time = cube_i.shape[0]
            frame_rs = []
            for t in range(n_time):
                a, b = cube_i[t].ravel(), cube_j[t].ravel()
                if not (np.any(a) and np.any(b)):
                    continue
                m = np.isfinite(a) & np.isfinite(b)
                if m.sum() > 1:
                    r, _ = pearsonr(a[m], b[m])
                    if np.isfinite(r):
                        frame_rs.append(r)
            mean_r = float(np.mean(frame_rs)) if frame_rs else 0.0
            corr_matrix[i, j] = corr_matrix[j, i] = mean_r
            if mean_r >= threshold:
                flagged.append((i, j, mean_r))

    if flagged:
        print(f"WARNING: {len(flagged)} wave pair(s) show high mutual correlation "
              f"(>= {threshold:.2f}) between their Gabor reconstructions -- possibly "
              f"the same physical wave split into two families rather than genuinely "
              f"distinct waves:")
        for i, j, r in flagged:
            print(f"  {waves[i]['label']}  <->  {waves[j]['label']}   mean r = {r:.3f}")
    else:
        print(f"No wave pairs exceeded the duplicate-correlation threshold ({threshold:.2f}).")

    return flagged, corr_matrix


# ---------------------------------------------------------------------------
# Utility: interpolate family parameters to the full time grid
# ---------------------------------------------------------------------------
