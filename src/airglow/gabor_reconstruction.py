"""
gabor_reconstruction.py
-----------------------
Gabor wave reconstruction for the airglow pipeline.

Provides a unified entry point ``reconstruct_all_waves()`` that works in two
modes depending on whether per-frame parameter arrays are supplied:

Fixed-kernel mode (single omega-k → Gabor, same as realdata_validate.ipynb):
    Each wave uses one kernel built from a fixed orientation_deg and
    wavelength_km (the omega-k detection values).  Identical to the
    existing realdata pipeline.

Adaptive-kernel mode (sliding-window omega-k → Gabor):
    Each wave receives per-frame orientation_deg_series and
    wavelength_km_series arrays interpolated from the sliding-window
    omega-k family estimates.  A new Gabor kernel is built for each
    frame, allowing the filter to track slowly-drifting wave parameters.

In both modes the outputs are identical in structure so all downstream
steps (cross-validation, Kalman tracker) work unchanged.

Public API
----------
reconstruct_all_waves(data, waves, dx_km, Bf, Btheta, ...)
    Unified dispatcher — use this in notebooks.
"""

import numpy as np
from scipy import signal

# The existing Gabor implementation — imported, never reimplemented here.
from cluster_reconstruction import create_gabor_kernel


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def reconstruct_all_waves(
    data_filtered_temporal,
    waves,
    dx_km,
    Bf=1.0,
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

    Bf : float
        Gabor frequency bandwidth (octaves).

    Btheta : float
        Gabor angular bandwidth (radians).

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
            data_filtered_temporal, waves, dx_km, Bf, Btheta, n_stds,
            rms_calibrate,
        )
    else:
        return _reconstruct_fixed(
            data_filtered_temporal, waves, dx_km, Bf, Btheta, n_stds,
            rms_calibrate,
        )


# ---------------------------------------------------------------------------
# Fixed-kernel reconstruction  (identical to realdata_validate.ipynb)
# ---------------------------------------------------------------------------

def _reconstruct_fixed(
    data_filtered_temporal,
    waves,
    dx_km,
    Bf,
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

        print(
            f"  [{wi+1}/{n_waves}] {label}: "
            f"lambda={wavelength_km:.1f} km, orient={orientation_deg:.1f} deg, "
            f"n_stds={w_n_stds}  [fixed kernel]"
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
    Bf,
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


# ---------------------------------------------------------------------------
# Utility: interpolate family parameters to the full time grid
# ---------------------------------------------------------------------------

def interpolate_family_to_time_grid(
    family,
    time_grid,
    parameter_keys=("direction", "wavelength", "speed"),
    win_half_ns=0.0,
):
    """
    Map family member parameters onto the full dataset time grid using
    Option 1 (half-window extension) coverage semantics.

    A detection at centre time t_c represents the interval
    [t_c - W/2, t_c + W/2].  A grid frame is "covered" by this family
    if it falls within W/2 of **any** detection centre.  Because adjacent
    window centres are typically separated by one hop (~10 min) and
    W/2 is ~45 min, any within-family gap smaller than W is automatically
    bridged by the overlapping half-window balls.  Gaps larger than W
    produce NaN naturally.

    No extrapolation beyond first/last detection centre ± W/2.

    This function is called per-family.  The **Voronoi (Option 2)**
    tie-breaking between families is handled by ``build_waves_for_gabor``,
    which calls this function for each family and then assigns ownership
    of contested frames to the family whose nearest detection centre is
    closest in time.

    Parameters
    ----------
    family : dict
        One element from ``build_wave_families()``.
    time_grid : np.ndarray of np.datetime64
        Full-dataset time axis.
    parameter_keys : tuple of str
        Member dict keys to interpolate.  ``"direction"`` is remapped to
        ``"orientation_deg"`` in the output.
    win_half_ns : float
        Half the analysis window duration in **nanoseconds**.
        Pass ``WIN_LEN_MIN / 2 * 60 * 1e9``.

    Returns
    -------
    out : dict
        ``"orientation_deg"``, ``"wavelength"``, ``"speed"`` — each
        ``np.ndarray (n_time,)`` with NaN where not covered.
        ``"_valid_mask"``   — bool array, True where covered.
        ``"_nearest_dist"`` — float array, ns distance to nearest
                              detection centre (used for Voronoi).
    """
    members = sorted(family["members"], key=lambda m: m["time"])
    t_mem = np.array(
        [np.datetime64(m["time"], "ns") for m in members],
        dtype="datetime64[ns]",
    ).astype(np.float64)
    t_grid = time_grid.astype(np.float64)

    # Distance from each grid frame to its nearest detection centre (ns)
    # shape: (n_time,)
    dist_matrix  = np.abs(t_grid[:, None] - t_mem[None, :])   # (n_time, n_det)
    nearest_dist = dist_matrix.min(axis=1)                     # (n_time,)

    # Option 1: valid if within W/2 of any detection centre
    valid_mask = nearest_dist <= win_half_ns

    n_valid = int(valid_mask.sum())
    n_total = len(t_grid)

    out = {"_valid_mask": valid_mask, "_nearest_dist": nearest_dist}

    for key in parameter_keys:
        vals = np.array([m[key] for m in members], dtype=np.float64)
        out_key = "orientation_deg" if key == "direction" else key

        if key == "direction":
            # ── Circular interpolation for orientation [0°, 180°) ────────
            # Linear np.interp in raw degrees goes the wrong way through the
            # 0°/180° boundary (e.g. 172° → 6° sweeps 166° instead of 14°).
            # Fix: double the angles to map [0°,180°) onto the full circle
            # [0°,360°), interpolate the unit-vector (cos, sin) components
            # separately (always choosing the short arc), then halve back.
            angle2_rad = np.deg2rad(vals * 2.0)
            cos_vals   = np.cos(angle2_rad)
            sin_vals   = np.sin(angle2_rad)

            cos_interp = np.interp(t_grid, t_mem, cos_vals,
                                   left=cos_vals[0], right=cos_vals[-1])
            sin_interp = np.interp(t_grid, t_mem, sin_vals,
                                   left=sin_vals[0], right=sin_vals[-1])

            angle2_interp = np.arctan2(sin_interp, cos_interp)
            orient_interp = np.rad2deg(angle2_interp) / 2.0 % 180.0
            result = np.where(valid_mask, orient_interp, np.nan)
        else:
            # Non-angular parameters: plain linear interpolation
            interp_full = np.interp(t_grid, t_mem, vals,
                                    left=vals[0], right=vals[-1])
            result = np.where(valid_mask, interp_full, np.nan)

        out[out_key] = result

    pct = 100.0 * n_valid / n_total if n_total > 0 else 0.0
    print(f"    [family {family.get('family_id','?')}] "
          f"{n_valid}/{n_total} frames covered ({pct:.0f}%)")
    return out


def build_waves_for_gabor(families, time_grid, n_stds=3.0,
                          win_half_min=45.0,
                          dir_tol_deg=15.0, lambda_tol_km=20.0,
                          voronoi_dir_tol=None, voronoi_lam_tol=None):
    """
    Build the ``waves_detected``-style list with per-frame parameter arrays,
    using Option 1 (half-window extension) + Option 2 (Voronoi) to give
    every frame an unambiguous owner.

    **Option 1 — half-window extension**
        Each detection at centre t_c covers [t_c - W/2, t_c + W/2].
        A grid frame is valid for a family if it falls within W/2 of any
        of that family's detection centres.

    **Option 2 — Voronoi tie-breaking**
        When multiple families are simultaneously valid at a frame
        (possible during parameter transitions), ownership goes to the
        family whose nearest detection centre is closest in time.
        This produces a clean partition with no gaps and no overlap:
        every frame belongs to exactly one family (or to none if no
        family is valid at all).

    The two rules together guarantee:
    - No artificial gap at family transitions (Option 1 extends coverage).
    - No double-reconstruction of any frame (Option 2 partitions overlaps).
    - Genuine absences (wave disappeared for longer than W) remain NaN.

    Parameters
    ----------
    families    : output of ``build_wave_families()``
    time_grid   : full-dataset time axis (np.ndarray of np.datetime64)
    n_stds      : Gabor kernel half-width in sigma units
    win_half_min : half the sliding-window duration in **minutes**
                  (pass ``WIN_LEN_MIN / 2``).
    dir_tol_deg  : direction tolerance used when building families (degrees).
                   Controls the default Voronoi grouping threshold.
    lambda_tol_km : wavelength tolerance used when building families (km).
                    Controls the default Voronoi grouping threshold.
    voronoi_dir_tol : max direction difference (degrees) between two family
        founding centroids for them to be treated as the **same physical
        wave** (and therefore subject to Voronoi tie-breaking rather than
        independent ownership).  Defaults to ``dir_tol_deg`` (1×), matching
        the family-building threshold.
        Rule of thumb: set equal to ``DIR_TOL_DEG`` used in
        ``build_wave_families()``.  The old behaviour was ``2*dir_tol_deg``
        which caused transitive over-grouping when three or more families
        lined up incrementally in direction or wavelength space.
    voronoi_lam_tol : same as ``voronoi_dir_tol`` but for wavelength (km).
        Defaults to ``lambda_tol_km`` (1×).

    Returns
    -------
    waves : list of dict, one per family
    """
    win_half_ns = win_half_min * 60.0 * 1e9   # minutes → nanoseconds
    n_time      = len(time_grid)
    n_fam       = len(families)

    # ── Pass 1: per-family interpolation and coverage ─────────────────────
    all_interp     = []
    valid_stack    = np.zeros((n_fam, n_time), dtype=bool)
    dist_stack     = np.full( (n_fam, n_time), np.inf)

    for fi, fam in enumerate(families):
        interp = interpolate_family_to_time_grid(
            fam, time_grid,
            parameter_keys=("direction", "wavelength", "speed"),
            win_half_ns=win_half_ns,
        )
        all_interp.append(interp)
        valid_stack[fi] = interp["_valid_mask"]
        dist_stack[fi]  = interp["_nearest_dist"]
    # ── Pass 2: group families into physical waves, Voronoi within groups ──
    #
    # Two families belong to the same physical wave if their founding
    # centroids are within voronoi_dir_tol and voronoi_lam_tol of each other.
    # Voronoi tie-breaking is applied ONLY within such groups so that truly
    # distinct simultaneous waves each get independent ownership of their
    # covered frames.
    #
    # Default: 1× the family-building tolerances.  The grouping is
    # TRANSITIVE (union-find), so a 2× multiplier can chain families across
    # large parameter gaps (e.g. λ=43, 72, 108 km all in one group via two
    # 36-km hops even though 43↔108 km is clearly two different waves).
    if voronoi_dir_tol is None:
        voronoi_dir_tol = dir_tol_deg
    if voronoi_lam_tol is None:
        voronoi_lam_tol = lambda_tol_km

    def _adiff(a, b):
        d = abs(a - b) % 180
        return min(d, 180 - d)

    group_id = list(range(n_fam))
    for i in range(n_fam):
        for j in range(i + 1, n_fam):
            if (_adiff(families[i]["direction_center"],
                       families[j]["direction_center"]) < voronoi_dir_tol
                    and abs(families[i]["wavelength_center"]
                            - families[j]["wavelength_center"]) < voronoi_lam_tol):
                old_g = group_id[j]; new_g = group_id[i]
                group_id = [new_g if g == old_g else g for g in group_id]

    unique_groups = sorted(set(group_id))
    print(f"\nWave group assignment "
          f"(voronoi_dir_tol={voronoi_dir_tol:.0f} deg, "
          f"voronoi_lam_tol={voronoi_lam_tol:.0f} km):")
    for g in unique_groups:
        mig = [fi for fi, gid in enumerate(group_id) if gid == g]
        labels = ", ".join(
            f"Family {families[fi]['family_id']} "
            f"(dir={families[fi]['direction_center']:.0f} deg, "
            f"lam={families[fi]['wavelength_center']:.0f} km)"
            for fi in mig
        )
        print(f"  Wave group {g}: {labels}")

    # Start from full valid_stack; Voronoi only within same wave group
    owned_stack = valid_stack.copy()
    for g in unique_groups:
        mig = [fi for fi, gid in enumerate(group_id) if gid == g]
        if len(mig) < 2:
            continue
        for t in range(n_time):
            competing = [fi for fi in mig if valid_stack[fi, t]]
            if len(competing) < 2:
                continue
            best = competing[int(np.argmin([dist_stack[fi, t] for fi in competing]))]
            for fi in competing:
                if fi != best:
                    owned_stack[fi, t] = False

    print(f"\nOwnership summary ({n_time} frames total):")
    any_unowned = np.all(~owned_stack, axis=0)
    n_unowned   = int(any_unowned.sum())
    for fi, fam in enumerate(families):
        n_owned = int(owned_stack[fi].sum())
        print(f"  Family {fam['family_id']}: {n_owned} frames "
              f"({100.*n_owned/n_time:.0f}%)")
    if n_unowned > 0:
        print(f"  Frames with no family active: {n_unowned} "
              f"({100.*n_unowned/n_time:.0f}%)")
        print(f"  NOTE: unowned frames receive zero reconstruction.")
        print(f"  Causes: edges of dataset (first/last W/2={win_half_min:.0f} min),")
        print(f"          or inter-family gaps wider than W={2*win_half_min:.0f} min.")
    else:
        print(f"  All frames covered by at least one family.")

    waves = []
    for fi, (fam, interp) in enumerate(zip(families, all_interp)):
        members = fam["members"]

        owned_mask    = owned_stack[fi]
        orient_series = np.where(owned_mask, interp["orientation_deg"], np.nan)
        wl_series     = np.where(owned_mask, interp["wavelength"],      np.nan)

        # Median scalar values for KF initialisation
        median_orient  = float(np.median([m["direction"]  for m in members]))
        median_wl      = float(np.median([m["wavelength"] for m in members]))
        median_speed   = float(np.median([m["speed"]      for m in members]))
        full_azimuths  = [m.get("azimuth_full", m["direction"]) for m in members]
        median_azimuth = float(np.median(full_azimuths))

        waves.append({
            "label":            (f"Family {fam['family_id']}  "
                                 f"(az~{median_azimuth:.0f} deg, "
                                 f"lam~{median_wl:.0f} km)"),
            # Scalar KF seed values
            "orientation_deg":  median_orient,
            "wavelength_km":    median_wl,
            "azimuth":          median_azimuth,
            "phase_speed":      median_speed,
            "n_stds":           n_stds,
            # Per-frame arrays (NaN where family does not own this frame)
            "orientation_deg_series": orient_series,
            "wavelength_km_series":   wl_series,
            # Ownership mask — useful for diagnostics and the KF
            "_owned_mask":            owned_mask,
            "_win_half_min":          win_half_min,
            "_group_id":              group_id[fi],
        })

    return waves
