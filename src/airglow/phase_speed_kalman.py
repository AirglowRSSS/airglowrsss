"""
Phase-speed estimation via Kalman filter on complex Gabor amplitudes.

Replaces ``rolling_cross_periodogram.py`` with a more stable estimator.

Design
------
For each Gabor filter (λ_i, θ_i) in the bank:

1.  Extract the spatially-averaged mean complex amplitude::

        A(t) = (2/G_sum) * mean_pixels(conv(x, y, t))

    where ``conv`` is the full complex output of the Gabor convolution before
    taking the real part.  ``|A(t)|`` is used as a wave-amplitude / SNR proxy;
    ``φ(t) = angle(A(t))`` as the measured wave phase.

    **Important caveat**: for a plane wave with many wavelengths across the image,
    the spatial mean of the complex convolution averages toward zero (the complex
    exponential component cancels over many cycles).  In that regime ``|A(t)|``
    is small, the Kalman filter assigns high measurement noise to those frames,
    and the state estimate coasts on the dynamical model.  For localized wave
    packets—the common case in airglow—the spatial mean is dominated by the
    packet centroid and carries a well-defined phase.  If you need a more robust
    single-phasor representation regardless of spatial extent, replace ``mean``
    with the temporal cross-correlation ``sum(conj(conv_t-1) * conv_t) / N`` and
    interpret ``angle(A(t)) / dt`` directly as ω_obs; set ``kf_observe_omega=True``
    in ``analyze_all_waves_kalman``.

2.  Run a 3-state Kalman filter on φ_obs(t) to estimate [φ, ω, dω/dt]:

    * Transition: constant-acceleration (kinematic) model.
    * Process noise: diagonal Q(q_omega, q_domega) — ``q_omega`` is the main
      tuning knob and controls how fast ω is allowed to vary.
    * Measurement noise: R(t) ∝ 1/|A(t)|² (high amplitude → low noise).
    * Innovation: phase difference wrapped to [−π, π] to handle wrap-around.

3.  Derive wave parameters from the Kalman state::

        v(t)  = |ω(t)| × λ / (2π)        # always non-negative (m/s)
        az(t) = wave['azimuth']           # fixed from ω-k (Convention B [0°,360°))

    λ and θ come directly from the Gabor filter identity; they are NOT
    re-estimated from any cross-spectrum.  Direction is fixed from ω-k to
    avoid sign-flip artefacts: noise bursts that push the cross-correlation
    phasor across zero contaminate the sign estimate for many frames due to
    slow Kalman gain.  Tracking |ω| eliminates all such artefacts.

4.  Filter-bank selection: at each timestep pick the wave with the highest
    rolling-mean |A(t)|, with a configurable minimum dwell time.

Coordinate convention
---------------------
``orientation_deg`` throughout is Convention B (0° = North, 90° = East,
clockwise, [0°, 180°)).  Propagation azimuth ``kf_prop_azimuth_deg`` is
Convention B full-circle [0°, 360°).
"""

import numpy as np
import pandas as pd
import warnings

from OmegaK_enhanced import circular_mean, circular_std

warnings.filterwarnings('ignore', category=RuntimeWarning)


# ---------------------------------------------------------------------------
# Circular helper
# ---------------------------------------------------------------------------

def circular_diff_deg(a, b):
    """Signed circular difference (a - b) in degrees, result in [-180, 180)."""
    diff = (a - b + 180.0) % 360.0 - 180.0
    return diff


# ---------------------------------------------------------------------------
# Core Kalman filter
# ---------------------------------------------------------------------------

def _kalman_omega_tracker(
    omega_obs: np.ndarray,
    amp_obs: np.ndarray,
    dt: float,
    *,
    q_omega: float = 1e-8,
    q_domega: float = 1e-12,
    r_sigma_base: float = 0.3,
    snr_min_amplitude: float = 0.05,
    chi2_gate: float = 9.0,
    hard_chi2_gate: bool = False,
    omega_init: float = 0.0,
) -> dict:
    """
    Track instantaneous angular frequency with a 2-state Kalman filter.

    State vector: ``x = [ω, dω/dt]``.

    The observation at each step is the directly-measured angular frequency::

        ω_obs(t) = angle(conj(conv_t) · conv_{t-1}) / dt          [rad/s]

    where ``conv_t`` is the full complex Gabor convolution frame and the
    angle is spatially averaged via the cross-correlation phasor
    ``A_cross(t) = mean_pixels(conj(conv_t) · conv_{t-1))``.

    This eliminates the 180° direction-flip problem that afflicts absolute-phase
    tracking (``angle(mean(conv))``).  Because the cross-correlation phasor
    ``A_cross`` goes to zero only when the wave itself vanishes (not when it has
    many cycles across the image), ``|A_cross|`` is a reliable SNR proxy and
    ``angle(A_cross) / dt`` is a stable, sign-consistent estimate of ω.

    Sign convention
    ---------------
    ``angle(conj(conv_t) · conv_{t-1}) = +ω·dt``

    So positive ω means the wave phase advances forward in time, i.e. the wave
    propagates in the ``orientation_deg`` direction (Convention B).  Negative ω
    means propagation in the opposite direction.

    Parameters
    ----------
    omega_obs : ndarray, shape (n_time,)
        Directly observed angular frequency ``angle(A_cross(t)) / dt`` (rad/s).
        NaN or entries where ``amp_obs == 0`` are treated as missing
        (predict-only steps).
    amp_obs : ndarray, shape (n_time,)
        Magnitude of the cross-correlation phasor ``|A_cross(t)|``.  Large
        value → reliable ω estimate → low measurement noise.
    dt : float
        Timestep in seconds.
    q_omega : float
        Process noise variance on ω per timestep ((rad/s)²/step).
        **Primary tuning parameter**.  Default 1e-8.
    q_domega : float
        Process noise variance on dω/dt per timestep ((rad/s²)²/step).
        Default 1e-12.
    r_sigma_base : float
        Measurement noise sigma in *phase* at unit normalised amplitude (rad).
        Converted to ω noise as ``σ_ω = r_sigma_base / dt``, so
        ``R(t) = (r_sigma_base / dt / snr(t))²``.
        Default 0.3 (≈ ±17° phase noise → ±0.0025 rad/s at dt=120 s).
    snr_min_amplitude : float
        Fraction of ``max(|A_cross|)`` below which a frame is considered
        too noisy.  Flagged frames still receive a (high-noise) update so
        the covariance does not diverge.  Default 0.05.
    chi2_gate : float
        Chi-squared gate threshold for the innovation QC flag.
        Default 9.0 (≈ 3σ).
    hard_chi2_gate : bool
        If True, frames that fail the chi² gate coast on the Kalman prediction
        (no state update); P continues to grow via Q so the filter reopens
        quickly when valid observations resume.
        If False (default), the update runs on all finite observations and
        ``innov_valid`` is a diagnostic label only.
    omega_init : float
        Initial ω estimate (rad/s).  Should come from the ω-k result so
        the filter starts with the correct sign.  Default 0.0.

    Returns
    -------
    dict with keys:

    ``omega``
        ndarray (n_time,) — posterior ω estimate (rad/s).
    ``domega``
        ndarray (n_time,) — posterior dω/dt estimate (rad/s²).
    ``P_omega``
        ndarray (n_time,) — posterior variance of ω.
    ``innovation``
        ndarray (n_time,) — raw Kalman innovation (rad/s).
    ``innov_var``
        ndarray (n_time,) — innovation variance S(t).
    ``innov_valid``
        ndarray bool (n_time,) — True if frame passes chi2 gate and SNR threshold.
    ``R``
        ndarray (n_time,) — measurement noise variance used at each step.
    """
    n = len(omega_obs)

    # --- State transition: constant-acceleration kinematic model on ω ---
    F = np.array([[1.0, dt ],
                  [0.0, 1.0]])

    # --- Observation: measure ω directly ---
    H = np.array([[1.0, 0.0]])   # shape (1, 2)

    # --- Process noise ---
    Q = np.diag([q_omega, q_domega])

    # --- Amplitude normalisation ---
    amp_finite = amp_obs[np.isfinite(amp_obs) & (amp_obs > 0)]
    amp_ref = float(np.max(amp_finite)) if len(amp_finite) > 0 else 1.0
    if amp_ref == 0.0:
        amp_ref = 1.0

    # --- Initialise state from ω-k prior ---
    x = np.array([omega_init, 0.0])

    # Initial covariance: moderate uncertainty on ω (30% of init), tight on drift
    omega_init_scale = max(abs(omega_init), 1e-4)
    P = np.diag([(0.3 * omega_init_scale) ** 2,   # ω: ±30% initial uncertainty
                 (1e-6) ** 2])                      # dω/dt: nearly zero initially

    # --- Output arrays ---
    omega_out  = np.full(n, np.nan)
    domega_out = np.full(n, np.nan)
    P_omega    = np.full(n, np.nan)
    innov      = np.full(n, np.nan)
    innov_var  = np.full(n, np.nan)
    innov_valid = np.zeros(n, dtype=bool)
    R_out      = np.full(n, np.nan)

    I2 = np.eye(2)

    for t in range(n):
        # --- Predict ---
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q

        # --- Amplitude-dependent measurement noise ---
        a_raw = float(amp_obs[t]) if np.isfinite(amp_obs[t]) else 0.0
        a_eff = max(a_raw / amp_ref, snr_min_amplitude)
        # Phase noise σ_φ = r_sigma_base / a_eff  →  ω noise σ_ω = σ_φ / dt
        R_t   = (r_sigma_base / a_eff / dt) ** 2
        R_out[t] = R_t

        # --- Update ---
        z = omega_obs[t]
        snr_ok = (a_raw / amp_ref) >= snr_min_amplitude

        if np.isfinite(z) and a_raw > 0.0:
            e = z - float((H @ x_pred)[0])    # linear innovation (ω is not circular)

            S = float((H @ P_pred @ H.T)[0, 0]) + R_t

            innov[t]     = e
            innov_var[t] = S
            innov_valid[t] = snr_ok and ((e ** 2 / S) <= chi2_gate)

            if innov_valid[t] or not hard_chi2_gate:
                K = (P_pred @ H.T) / S        # shape (2, 1)
                x = x_pred + K.ravel() * e
                P = (I2 - K @ H) @ P_pred
            else:
                x = x_pred                    # coast: prediction only
                P = P_pred
        else:
            x = x_pred
            P = P_pred

        omega_out[t]  = x[0]
        domega_out[t] = x[1]
        P_omega[t]    = P[0, 0]

    return dict(
        omega=omega_out,
        domega=domega_out,
        P_omega=P_omega,
        innovation=innov,
        innov_var=innov_var,
        innov_valid=innov_valid,
        R=R_out,
    )


# ---------------------------------------------------------------------------
# Orientation Kalman tracker
# ---------------------------------------------------------------------------

def _kalman_theta_tracker(
    kx_obs: np.ndarray,
    ky_obs: np.ndarray,
    amp_obs: np.ndarray,
    dt: float,
    theta_init_rad: float,
    btheta_rad: float,
    *,
    q_theta: float = 1e-8,
    q_dtheta: float = 1e-12,
    r_theta_base_rad: float = np.deg2rad(15.0),
    snr_min_amplitude: float = 0.05,
    chi2_gate: float = 9.0,
    innov_valid_mask: np.ndarray = None,
    kernel_center_series_deg: np.ndarray = None,
) -> dict:
    """
    Track instantaneous wave propagation azimuth with a 2-state Kalman filter.

    State vector: x = [theta, dtheta/dt].  Operates on the full-circle
    propagation azimuth in radians [0, 2π).

    Parameters
    ----------
    kx_obs : ndarray, shape (n_time,)
        Amplitude-weighted mean east wavenumber (rad/m). NaN = missing.
    ky_obs : ndarray, shape (n_time,)
        Amplitude-weighted mean north wavenumber (rad/m). NaN = missing.
    amp_obs : ndarray, shape (n_time,)
        |A_cross(t)| — SNR proxy (same as omega filter).
    dt : float
        Timestep in seconds.
    theta_init_rad : float
        Initial azimuth in radians (full-circle, from wave['azimuth']).
    btheta_rad : float
        Gabor kernel half-bandwidth in radians.
    q_theta : float
        Process noise on theta per step (rad²/step).
    q_dtheta : float
        Process noise on dtheta/dt per step ((rad/s)²/step).
    r_theta_base_rad : float
        Measurement noise base at peak SNR (radians).
    snr_min_amplitude : float
        Same minimum SNR threshold as the omega filter.
    chi2_gate : float
        Chi-squared gate threshold.
    innov_valid_mask : ndarray bool, shape (n_time,), optional
        Validity mask from the omega filter.  Frames that are invalid there
        are also skipped here (coast on prediction).
    kernel_center_series_deg : ndarray float, shape (n_time,), optional
        Per-frame Gabor kernel centre orientation in **half-circle** degrees
        [0°, 180°).  Supplied in adaptive Gabor mode from
        ``wave['orientation_deg_series']``.  When present, ``kernel_mismatch``
        is computed frame-by-frame against this series using half-circle
        arithmetic rather than against the fixed full-circle
        ``kernel_center_deg`` derived from ``theta_init_rad``.  NaN entries
        (frames with no active kernel) are treated as no-mismatch.  When
        absent the original fixed-centre behaviour is preserved.

    Returns
    -------
    dict with keys:

    ``theta_est_deg``
        ndarray (n_time,) — posterior azimuth estimate [0, 360).
    ``theta_std_deg``
        ndarray (n_time,) — 1-sigma uncertainty from P, in degrees.
    ``lambda_obs_km``
        ndarray (n_time,) — raw observed horizontal wavelength (km, unfiltered).
    ``kernel_mismatch``
        ndarray bool (n_time,) — True when estimate deviates > btheta/2 from
        the kernel center orientation.
    ``theta_obs_rad``
        ndarray (n_time,) — raw theta observations (radians, for diagnostics).
    """
    n = len(kx_obs)

    # Edge case: sequence too short for Kalman — fill with kernel centre value
    if n < 3:
        theta_deg_init = np.rad2deg(theta_init_rad) % 360.0
        return dict(
            theta_est_deg=np.full(n, theta_deg_init),
            theta_std_deg=np.full(n, np.nan),
            lambda_obs_km=np.full(n, np.nan),
            kernel_mismatch=np.zeros(n, dtype=bool),
            theta_obs_rad=np.full(n, np.nan),
        )

    # --- Derive scalar observations from (kx, ky) ---
    theta_obs_raw = np.full(n, np.nan)
    lambda_obs_km = np.full(n, np.nan)
    for t in range(n):
        kx_t = kx_obs[t]
        ky_t = ky_obs[t]
        if np.isnan(kx_t) or np.isnan(ky_t):
            continue
        k_h = np.sqrt(kx_t ** 2 + ky_t ** 2)
        if k_h < 1e-6:  # near-DC: skip
            continue
        theta_obs_raw[t] = np.arctan2(-kx_t, -ky_t)        # Convention B, rad
        lambda_obs_km[t] = (2.0 * np.pi / k_h) / 1000.0   # km

    # --- Resolve 180° ambiguity ---
    # The spatial phase gradient arctan2(-kx, -ky) has an inherent 180° sign
    # ambiguity: cos(k·r) = cos(-k·r), so the gradient can point along +k or -k
    # with equal validity.  Fold each observation to whichever of (z, z+π) is
    # closer to theta_init_rad (the ω-k propagation direction), which provides
    # the ground truth on which half-circle the wave is actually traveling.
    for t in range(n):
        if not np.isfinite(theta_obs_raw[t]):
            continue
        z     = theta_obs_raw[t]
        z_alt = (z + np.pi) % (2.0 * np.pi)
        # Unsigned circular distance to init
        d_z   = abs((z     - theta_init_rad + np.pi) % (2.0 * np.pi) - np.pi)
        d_alt = abs((z_alt - theta_init_rad + np.pi) % (2.0 * np.pi) - np.pi)
        if d_alt < d_z:
            theta_obs_raw[t] = z_alt

    # --- Kalman setup ---
    F = np.array([[1.0, dt],
                  [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.diag([q_theta, q_dtheta])

    # Amplitude normalisation
    amp_finite = amp_obs[np.isfinite(amp_obs) & (amp_obs > 0)]
    amp_ref = float(np.max(amp_finite)) if len(amp_finite) > 0 else 1.0
    if amp_ref == 0.0:
        amp_ref = 1.0

    # Initialise
    x = np.array([theta_init_rad, 0.0])
    P = np.diag([np.deg2rad(10.0) ** 2,
                 (np.deg2rad(1.0) / dt) ** 2])

    # Output arrays
    theta_est_out = np.full(n, np.nan)
    theta_std_out = np.full(n, np.nan)
    kernel_mm     = np.zeros(n, dtype=bool)
    I2 = np.eye(2)

    kernel_center_deg = np.rad2deg(theta_init_rad) % 360.0
    btheta_half_deg   = np.rad2deg(btheta_rad) / 2.0

    for t in range(n):
        # Predict
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q

        # SNR-weighted measurement noise
        a_raw = float(amp_obs[t]) if np.isfinite(amp_obs[t]) else 0.0
        a_eff = max(a_raw / amp_ref, snr_min_amplitude)
        R_t   = (r_theta_base_rad / a_eff) ** 2

        # External validity from omega filter
        ext_valid = True if innov_valid_mask is None else bool(innov_valid_mask[t])

        z = theta_obs_raw[t]
        snr_ok = (a_raw / amp_ref) >= snr_min_amplitude

        if np.isfinite(z) and a_raw > 0.0 and ext_valid:
            # Circular innovation — wrap to [-pi, pi]
            innovation = z - float((H @ x_pred)[0])
            innovation = (innovation + np.pi) % (2.0 * np.pi) - np.pi

            S = float((H @ P_pred @ H.T)[0, 0]) + R_t
            chi2_ok = (innovation ** 2 / S) <= chi2_gate

            if snr_ok and chi2_ok:
                K = (P_pred @ H.T) / S
                x = x_pred + K.ravel() * innovation
                x[0] = x[0] % (2.0 * np.pi)
                P = (I2 - K @ H) @ P_pred
            else:
                x = x_pred
                x[0] = x[0] % (2.0 * np.pi)
                P = P_pred
        else:
            x = x_pred
            x[0] = x[0] % (2.0 * np.pi)
            P = P_pred

        theta_est_out[t] = x[0]
        theta_std_out[t] = np.sqrt(max(P[0, 0], 0.0))

        # Kernel mismatch flag —————————————————————————————————————————————
        # In adaptive Gabor mode the kernel centre shifts every frame, so
        # kernel_center_series_deg[t] is the true reference.  We compare in
        # half-circle space ([0°, 180°)) to avoid a spurious 180° difference
        # when the KF full-circle azimuth and the half-circle kernel centre
        # are on opposite lobes of the same physical orientation.
        #
        # In non-adaptive mode (kernel_center_series_deg is None) we fall back
        # to the original full-circle comparison against kernel_center_deg.
        if (kernel_center_series_deg is not None
                and np.isfinite(kernel_center_series_deg[t])):
            kc_half = float(kernel_center_series_deg[t])          # [0°, 180°)
            theta_half = np.rad2deg(x[0]) % 180.0                 # fold to [0°, 180°)
            half_diff = abs((theta_half - kc_half + 90.0) % 180.0 - 90.0)
            kernel_mm[t] = half_diff > btheta_half_deg
        elif kernel_center_series_deg is not None:
            # NaN frame (no active kernel) — not a mismatch
            kernel_mm[t] = False
        else:
            # Original fixed-centre full-circle comparison
            theta_deg_t = np.rad2deg(x[0]) % 360.0
            delta = abs(circular_diff_deg(theta_deg_t, kernel_center_deg))
            kernel_mm[t] = delta > btheta_half_deg

    # Warn once per run if mismatch fraction > 30%
    mismatch_frac = np.sum(kernel_mm) / n
    if mismatch_frac > 0.30:
        warnings.warn(
            f'Orientation KF: {100 * mismatch_frac:.0f}% frames flagged as '
            f'kernel_mismatch (estimate > Btheta/2 from kernel centre).',
            stacklevel=2,
        )

    return dict(
        theta_est_deg=np.rad2deg(theta_est_out) % 360.0,
        theta_std_deg=np.rad2deg(theta_std_out),
        lambda_obs_km=lambda_obs_km,
        kernel_mismatch=kernel_mm,
        theta_obs_rad=theta_obs_raw,
    )


# ---------------------------------------------------------------------------
# Wavenumber Kalman tracker (Feature C)
# ---------------------------------------------------------------------------
#
# BIAS CAVEAT — READ BEFORE USE
# ─────────────────────────────
# The spatial phase-gradient observable k_h_obs(t) = sqrt(kx_mean² + ky_mean²)
# is derived from the output of a Gabor filter whose kernel is tuned to a fixed
# (theta_kernel, lambda_kernel).  When the true wave parameters are well within
# the kernel's acceptance band the observable accurately reflects the actual
# horizontal wavenumber.  When the true wavelength drifts outside the kernel's
# passband Bf, the gradient is pulled toward the kernel center wavelength.
#
# Practical consequence
# ─────────────────────
# • kernel_mismatch = False  →  orientation is within bandwidth; k_h_obs is
#   also likely reliable (both orientation and wavelength within the passband).
# • kernel_mismatch = True   →  treat kf_lambda_km as biased toward the ω-k
#   kernel center wavelength.  A warning is logged at the mismatch location
#   in _kalman_theta_tracker().
#
# Despite the bias, the smoother is still useful: it reveals trends in k_h that
# are completely invisible in the noisy raw observations.  Use kf_lambda_km
# together with kernel_mismatch to evaluate the reliability of any given
# frame's wavelength estimate.

def _kalman_kh_tracker(
    kh_obs: np.ndarray,
    amp_obs: np.ndarray,
    dt: float,
    k_h_init: float,
    *,
    q_kh: float = 1e-12,
    q_dkh: float = 1e-16,
    r_kh_base: float = None,
    k_h_min: float,
    k_h_max: float,
    innovation_valid: np.ndarray = None,
) -> dict:
    """
    Track horizontal wavenumber magnitude with a 2-state Kalman filter.

    State vector: ``x = [k_h (rad/m), dk_h_dt (rad/m/s)]``.

    Tracks the horizontal wavenumber magnitude derived from the
    amplitude-weighted spatial phase gradient of the Gabor convolution
    output.  The raw observable is the same quantity used to compute
    ``kf_lambda_obs_km`` in the orientation tracker, but smoothed with a
    constant-velocity kinematic model so that the noisy frame-by-frame
    scatter is greatly reduced.

    See the bias caveat in the comment block above this function.

    Parameters
    ----------
    kh_obs : ndarray, shape (n_time,)
        Raw wavenumber magnitude observations (rad/m).
        NaN or zero = missing/near-DC frame; filter coasts on prediction.
    amp_obs : ndarray, shape (n_time,)
        |A_cross(t)| — SNR proxy, same as omega/theta filters.
    dt : float
        Timestep in seconds.
    k_h_init : float
        Initial k_h (rad/m) from the ω-k result: ``2π / (λ_km × 1000)``.
    q_kh : float
        Process noise on k_h per step ((rad/m)²/step).  Default 1e-12.
    q_dkh : float
        Process noise on dk_h/dt per step ((rad/m/s)²/step).  Default 1e-16.
    r_kh_base : float or None
        Measurement noise base at peak SNR (rad/m).  Computed by the caller
        as ``kf_r_kh_frac × k_h_init``.  If None, defaults to
        ``0.20 × k_h_init`` (≈ ±20% wavelength uncertainty at full amplitude).
    k_h_min : float
        Minimum physical k_h (rad/m), from ``2π / (lambda_h_max_km × 1000)``.
    k_h_max : float
        Maximum physical k_h (rad/m), from ``2π / (lambda_h_min_km × 1000)``.
    innovation_valid : ndarray bool, shape (n_time,), optional
        Validity mask from the omega filter.  Frames flagged invalid there
        coast on the prediction here (same gating as the theta tracker).

    Returns
    -------
    dict with keys:

    ``kh_est``
        ndarray (n_time,) — posterior k_h estimate (rad/m).
    ``kh_std``
        ndarray (n_time,) — 1-sigma uncertainty sqrt(P[0,0]) (rad/m).
    """
    n = len(kh_obs)

    if r_kh_base is None:
        r_kh_base = 0.20 * k_h_init

    # Edge case: sequence too short for meaningful filtering
    if n < 3:
        return dict(
            kh_est=np.full(n, k_h_init),
            kh_std=np.full(n, np.nan),
        )

    # --- State transition: constant-velocity kinematic model ---
    F = np.array([[1.0, dt],
                  [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.diag([q_kh, q_dkh])

    # --- Amplitude normalisation ---
    amp_finite = amp_obs[np.isfinite(amp_obs) & (amp_obs > 0)]
    amp_ref = float(np.max(amp_finite)) if len(amp_finite) > 0 else 1.0
    if amp_ref == 0.0:
        amp_ref = 1.0

    # --- Initialise state ---
    x = np.array([k_h_init, 0.0])
    P = np.diag([
        (0.20 * k_h_init) ** 2,         # ~20% initial wavelength uncertainty
        (k_h_init * 0.01 / dt) ** 2,    # 1% per frame drift rate uncertainty
    ])

    # --- Output arrays ---
    kh_est_out = np.full(n, np.nan)
    kh_std_out = np.full(n, np.nan)
    I2 = np.eye(2)
    snr_min = 0.05  # same floor as omega/theta trackers

    for t in range(n):
        # Predict
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q

        # SNR-weighted measurement noise
        a_raw = float(amp_obs[t]) if np.isfinite(amp_obs[t]) else 0.0
        a_eff = max(a_raw / amp_ref, snr_min)
        R_t = (r_kh_base / a_eff) ** 2

        # External validity from omega filter
        ext_valid = True if innovation_valid is None else bool(innovation_valid[t])

        z = kh_obs[t]

        if np.isfinite(z) and z > 0.0 and a_raw > 0.0 and ext_valid:
            # Linear innovation — no circular wrapping needed for k_h
            e = z - float((H @ x_pred)[0])
            S = float((H @ P_pred @ H.T)[0, 0]) + R_t

            K = (P_pred @ H.T) / S
            x = x_pred + K.ravel() * e
            P = (I2 - K @ H) @ P_pred
        else:
            # Coast: propagate prediction, let P grow via Q
            x = x_pred
            P = P_pred

        # Clamp to physical bounds (guard against nonphysical drift)
        x[0] = np.clip(x[0], k_h_min, k_h_max)

        kh_est_out[t] = x[0]
        kh_std_out[t] = np.sqrt(max(P[0, 0], 0.0))

    # Final guard: any residual non-positive values (should never happen)
    kh_est_out = np.where(kh_est_out > 0, kh_est_out, k_h_init)

    return dict(kh_est=kh_est_out, kh_std=kh_std_out)


# ---------------------------------------------------------------------------
# Dominant filter selection
# ---------------------------------------------------------------------------

def _select_dominant_filter(
    amp_matrix: np.ndarray,
    dwell_time: int,
    window: int,
) -> np.ndarray:
    """
    Select the dominant Gabor filter at each timestep.

    Uses a rolling-mean of ``|A(t)|`` with a minimum dwell time to suppress
    rapid toggling between filters.

    Parameters
    ----------
    amp_matrix : ndarray, shape (n_waves, n_time)
        ``|A(t)|`` for each wave and each time step.
    dwell_time : int
        Minimum number of frames before the dominant filter can switch.
    window : int
        Rolling window size (frames) for computing the mean ``|A(t)|``
        used in the dominance comparison.

    Returns
    -------
    dominant : ndarray, shape (n_time,), dtype int
        Index (into the first axis of ``amp_matrix``) of the dominant wave
        at each timestep.
    """
    n_waves, n_time = amp_matrix.shape
    if n_waves <= 1:
        return np.zeros(n_time, dtype=int)

    # Rolling mean of amplitude per wave
    rolling = np.zeros_like(amp_matrix)
    for wi in range(n_waves):
        a = amp_matrix[wi]
        for t in range(n_time):
            t0 = max(0, t - window + 1)
            rolling[wi, t] = np.nanmean(a[t0:t + 1])

    # Instantaneous best filter
    best_raw = np.argmax(rolling, axis=0)  # shape (n_time,)

    # Enforce minimum dwell time
    dominant = np.zeros(n_time, dtype=int)
    dominant[0] = int(best_raw[0])
    frames_since_switch = 0

    for t in range(1, n_time):
        frames_since_switch += 1
        if frames_since_switch >= dwell_time and int(best_raw[t]) != dominant[t - 1]:
            dominant[t] = int(best_raw[t])
            frames_since_switch = 0
        else:
            dominant[t] = dominant[t - 1]

    return dominant


# ---------------------------------------------------------------------------
# Feature C — wavelength conversion helpers
# ---------------------------------------------------------------------------

def _kh_to_lambda_km(kh_est: float) -> float:
    """Convert k_h estimate (rad/m) to wavelength (km). Returns NaN if kh <= 0."""
    if not np.isfinite(kh_est) or kh_est <= 0.0:
        return np.nan
    return (2.0 * np.pi / kh_est) / 1000.0


def _kh_std_to_lambda_std_km(kh_est: float, kh_std: float) -> float:
    """Propagate k_h uncertainty to wavelength uncertainty (km).

    Uses d(lambda)/d(k_h) = -2π/k_h², so
    sigma_lambda = sigma_kh * 2π / k_h².
    """
    if (not np.isfinite(kh_est) or kh_est <= 0.0
            or not np.isfinite(kh_std)):
        return np.nan
    return kh_std * (2.0 * np.pi / kh_est ** 2) / 1000.0


# ---------------------------------------------------------------------------
# Filter-bank orchestration
# ---------------------------------------------------------------------------

def analyze_all_waves_kalman(
    waves_detected: list,
    complex_amplitudes: list,
    time_vals,
    dt: float,
    *,
    q_omega: float = 1e-8,
    q_domega: float = 1e-12,
    r_sigma_base: float = 0.3,
    snr_min_amplitude: float = 0.05,
    chi2_gate: float = 9.0,
    hard_chi2_gate: bool = False,
    dwell_time: int = 10,
    dominant_window: int = 10,
    verbose: bool = True,
    # --- Feature A: orientation tracker ---
    kx_obs_list: list = None,
    ky_obs_list: list = None,
    btheta_rad: float = None,
    kf_q_theta: float = 1e-8,
    kf_q_dtheta: float = 1e-12,
    kf_r_theta_deg: float = 15.0,
    # --- Feature B: wave activity metric ---
    rms_omegak: np.ndarray = None,
    rms_wave_cubes: list = None,
    activity_threshold: float = 0.10,
    # --- Feature C: wavenumber / wavelength tracker ---
    kf_q_kh: float = 1e-12,
    kf_q_dkh: float = 1e-16,
    kf_r_kh_frac: float = 0.20,
    lambda_h_min_km: float = 5.0,
    lambda_h_max_km: float = 150.0,
) -> pd.DataFrame:
    """
    Run a Kalman ω-tracker for each wave in the filter bank, then select
    the dominant filter at each timestep.

    ``complex_amplitudes[i][t]`` must be the **temporal cross-correlation
    phasor**::

        A_cross(t) = mean_pixels(conj(conv_t) · conv_{t-1})

    so that ``angle(A_cross(t)) / dt = ω(t)`` and the sign of ω encodes the
    propagation direction unambiguously (see ``_kalman_omega_tracker``).

    Each wave's Kalman filter is initialised with ``ω_init`` derived from the
    ω-k phase-speed and propagation azimuth so the correct sign is locked in
    from the first frame.

    Parameters
    ----------
    waves_detected : list of dict
        Each dict must have ``'label'``, ``'wavelength_km'``,
        ``'orientation_deg'`` (Convention B, [0°, 180°)), ``'azimuth'``
        (full-circle propagation azimuth from ω-k, [0°, 360°)), and
        ``'phase_speed'`` (m/s).
    complex_amplitudes : list of ndarray, each shape (n_time,), dtype complex
        Temporal cross-correlation phasors ``A_cross(t)`` per wave.
        Entry ``[0]`` should be 0+0j (no predecessor for the first frame).
    time_vals : array-like of datetime64
        UTC timestamp for each frame.
    dt : float
        Timestep in seconds.
    q_omega : float
        Angular-frequency process noise per step ((rad/s)²/step).
        **Primary tuning knob**.  Default 1e-8.
    q_domega : float
        Frequency-drift-rate process noise per step ((rad/s²)²/step).
        Default 1e-12.
    r_sigma_base : float
        Phase measurement noise sigma at unit normalised amplitude (rad).
        Converted to ω noise as ``σ_ω = r_sigma_base / dt``.  Default 0.3.
    snr_min_amplitude : float
        Minimum ``|A_cross|/max(|A_cross|)`` for a frame to be flagged valid.
        Default 0.05.
    chi2_gate : float
        Chi-squared threshold for innovation QC flag.  Default 9.0 (≈ 3σ).
    hard_chi2_gate : bool
        Passed directly to ``_kalman_omega_tracker``.  If True, chi²-failing
        frames coast on the prediction rather than updating the state.
        Default False.
    dwell_time : int
        Minimum frames before the dominant filter can switch.  Default 10.
    dominant_window : int
        Rolling window (frames) for dominance comparison.  Default 10.
    verbose : bool
        Print per-wave summary lines.

    Returns
    -------
    pd.DataFrame with columns:

    ``wave_id``
        Integer index matching position in ``waves_detected``.
    ``time``
        UTC timestamp (datetime64).
    ``frame_idx``
        Integer frame index.
    ``gabor_wavelength_km``
        Fixed wavelength from the Gabor filter identity (km).
    ``gabor_orientation_deg``
        Fixed orientation from the Gabor filter identity (Convention B,
        [0°, 180°)).
    ``kf_phase_rad``
        Not estimated by the 2-state tracker — filled with NaN.
    ``kf_omega_rads``
        Kalman-estimated ω (rad/s).  Positive = propagating in the
        ``orientation_deg`` direction; negative = opposite lobe.
    ``kf_wavespeed_ms``
        Phase speed (m/s), always non-negative (|ω| × λ / 2π).
    ``kf_prop_azimuth_deg``
        Propagation azimuth in Convention B [0°, 360°), fixed from ω-k result.
    ``kf_period_min``
        Wave period ``= 2π / |ω| / 60`` (min).  NaN when ``|ω| < 1e-8``.
    ``amplitude``
        ``|A_cross(t)|`` — cross-correlation coherence magnitude (SNR proxy).
    ``innovation_residual``
        Kalman innovation (rad/s).
    ``innovation_valid``
        True if the frame passes the chi2 gate and SNR threshold.
    ``is_dominant``
        True if this wave is the dominant filter at this timestep.
    ``kf_orient_deg``
        Kalman-smoothed propagation azimuth [0, 360°) from spatial phase gradient.
    ``kf_orient_std_deg``
        1-sigma orientation uncertainty from Kalman P matrix (degrees).
    ``kf_lambda_obs_km``
        Raw (unfiltered) horizontal wavelength observation from spatial gradient.
    ``kernel_mismatch``
        True when ``kf_orient_deg`` deviates > Btheta/2 from kernel centre.
    ``activity_fraction``
        RMS(wave_cube[t]) / RMS(data_omegak[t]).
    ``wave_active``
        True when ``activity_fraction > activity_threshold`` AND ``innovation_valid``.
    ``kf_lambda_km``
        Kalman-smoothed horizontal wavelength (km), converted from the kh
        Kalman state estimate.  Always positive; NaN only when all gradient
        observations are missing for this wave.
    ``kf_lambda_std_km``
        1-sigma wavelength uncertainty (km), propagated from sqrt(P[0,0])
        via d(lambda)/d(k_h) = -2π/k_h².
    """
    n_waves = len(waves_detected)
    n_time  = len(time_vals)

    if n_waves == 0:
        return pd.DataFrame()

    if verbose:
        print('=' * 70)
        print('KALMAN PHASE SPEED ANALYSIS')
        print('=' * 70)
        print(f'  Waves: {n_waves}   Frames: {n_time}   dt={dt:.0f} s')
        print(f'  q_omega={q_omega:.1e}   r_sigma_base={r_sigma_base:.2f}   '
              f'chi2_gate={chi2_gate:.1f}')
        print()

    # --- Build amplitude matrix (|A_cross|) for dominant-filter selection ---
    amp_matrix = np.zeros((n_waves, n_time), dtype=float)
    for wi, A in enumerate(complex_amplitudes):
        amp_matrix[wi] = np.abs(A)

    # --- Run omega Kalman filter per wave ---
    kf_all = []
    for wi, (wave, A) in enumerate(zip(waves_detected, complex_amplitudes)):
        amp_obs = np.abs(A)

        # ω_obs(t) = |angle(A_cross(t))| / dt  — always non-negative.
        omega_obs = np.full(n_time, np.nan)
        nonzero   = amp_obs > 0.0
        omega_obs[nonzero] = np.abs(np.angle(A[nonzero])) / dt   # ≥ 0 always

        # Initialise at the ω-k phase speed (magnitude only).
        ps        = float(wave.get('phase_speed', 0.0))
        lam_m     = float(wave['wavelength_km']) * 1000.0
        omega_init = abs(2.0 * np.pi * ps / lam_m) if lam_m > 0 else 0.0

        if verbose:
            print(f'  [{wi + 1}/{n_waves}] {wave["label"]}: '
                  f'λ={wave["wavelength_km"]:.1f} km, '
                  f'orient={wave["orientation_deg"]:.1f}°  '
                  f'ω_init={omega_init:.4e} rad/s  '
                  f'(mean|A|={amp_obs.mean():.3g}, max|A|={amp_obs.max():.3g})')

        kf_out = _kalman_omega_tracker(
            omega_obs=omega_obs,
            amp_obs=amp_obs,
            dt=dt,
            q_omega=q_omega,
            q_domega=q_domega,
            r_sigma_base=r_sigma_base,
            snr_min_amplitude=snr_min_amplitude,
            chi2_gate=chi2_gate,
            hard_chi2_gate=hard_chi2_gate,
            omega_init=omega_init,
        )
        kf_all.append(kf_out)

    # --- Run orientation Kalman filter per wave (Feature A) ---
    have_gradient = (kx_obs_list is not None and ky_obs_list is not None)
    _btheta = btheta_rad if btheta_rad is not None else np.deg2rad(15.0)
    r_theta_base = np.deg2rad(kf_r_theta_deg)

    kf_theta_all = []
    for wi, (wave, A) in enumerate(zip(waves_detected, complex_amplitudes)):
        amp_obs = np.abs(A)
        theta_init = np.deg2rad(float(wave['azimuth']))   # full-circle, radians

        if have_gradient:
            kx = np.asarray(kx_obs_list[wi], dtype=float)
            ky = np.asarray(ky_obs_list[wi], dtype=float)
        else:
            kx = np.full(n_time, np.nan)
            ky = np.full(n_time, np.nan)

        # Use omega-filter validity mask to gate the orientation filter too.
        innov_valid_w = kf_all[wi]['innov_valid']
        amp_ref_w = float(np.max(amp_obs)) if amp_obs.max() > 0 else 1.0
        snr_ok_w  = (amp_obs / amp_ref_w) >= snr_min_amplitude
        combined_mask = innov_valid_w & snr_ok_w

        # In adaptive Gabor mode the per-frame kernel centre is stored in
        # orientation_deg_series (half-circle, [0°, 180°)).  Pass it through
        # so kernel_mismatch is evaluated against the actual kernel, not the
        # fixed median orientation.
        _orient_series = wave.get('orientation_deg_series', None)
        _ks_deg = (np.asarray(_orient_series, dtype=float)
                   if _orient_series is not None else None)

        th_out = _kalman_theta_tracker(
            kx_obs=kx,
            ky_obs=ky,
            amp_obs=amp_obs,
            dt=dt,
            theta_init_rad=theta_init,
            btheta_rad=_btheta,
            q_theta=kf_q_theta,
            q_dtheta=kf_q_dtheta,
            r_theta_base_rad=r_theta_base,
            snr_min_amplitude=snr_min_amplitude,
            chi2_gate=chi2_gate,
            innov_valid_mask=combined_mask,
            kernel_center_series_deg=_ks_deg,
        )
        kf_theta_all.append(th_out)

    # --- Run wavenumber Kalman filter per wave (Feature C) ---
    k_h_min = 2.0 * np.pi / (lambda_h_max_km * 1000.0)
    k_h_max = 2.0 * np.pi / (lambda_h_min_km * 1000.0)

    kf_kh_all = []
    for wi, (wave, A) in enumerate(zip(waves_detected, complex_amplitudes)):
        amp_obs = np.abs(A)
        lam_m   = float(wave['wavelength_km']) * 1000.0
        k_h_init = (2.0 * np.pi / lam_m) if lam_m > 0 else k_h_min
        k_h_init = float(np.clip(k_h_init, k_h_min, k_h_max))

        r_kh_base = kf_r_kh_frac * k_h_init

        # Derive kh_obs from the same kx/ky gradient arrays used by theta tracker
        if have_gradient:
            kx_w = np.asarray(kx_obs_list[wi], dtype=float)
            ky_w = np.asarray(ky_obs_list[wi], dtype=float)
        else:
            kx_w = np.full(n_time, np.nan)
            ky_w = np.full(n_time, np.nan)

        kh_obs_w = np.full(n_time, np.nan)
        for t in range(n_time):
            kxt, kyt = kx_w[t], ky_w[t]
            if np.isfinite(kxt) and np.isfinite(kyt):
                k = np.sqrt(kxt ** 2 + kyt ** 2)
                if k > 1e-6:
                    kh_obs_w[t] = k

        # All-NaN guard: fall back to ω-k reference
        n_valid_kh = np.sum(np.isfinite(kh_obs_w))
        if n_valid_kh == 0:
            warnings.warn(
                f'Wave {wave["label"]}: all k_h_obs are NaN — '
                f'kf_lambda_km filled with ω-k reference ({wave["wavelength_km"]:.1f} km).',
                stacklevel=2,
            )
            kf_kh_all.append(dict(kh_est=np.full(n_time, k_h_init),
                                  kh_std=np.full(n_time, np.nan)))
            continue

        # Use the omega-filter validity mask (same as theta tracker)
        innov_valid_w = kf_all[wi]['innov_valid']

        kh_out = _kalman_kh_tracker(
            kh_obs=kh_obs_w,
            amp_obs=amp_obs,
            dt=dt,
            k_h_init=k_h_init,
            q_kh=kf_q_kh,
            q_dkh=kf_q_dkh,
            r_kh_base=r_kh_base,
            k_h_min=k_h_min,
            k_h_max=k_h_max,
            innovation_valid=innov_valid_w,
        )
        kf_kh_all.append(kh_out)

    # --- Dominant filter selection ---
    dominant = _select_dominant_filter(amp_matrix, dwell_time, dominant_window)

    if verbose:
        dom_counts = {wi: int(np.sum(dominant == wi)) for wi in range(n_waves)}
        print(f'\n  Dominant-filter dwell counts (frames):')
        for wi, wave in enumerate(waves_detected):
            print(f'    {wave["label"]}: {dom_counts[wi]} frames '
                  f'({100*dom_counts[wi]/n_time:.0f}%)')
        print()

    have_activity = (rms_omegak is not None and rms_wave_cubes is not None)

    # --- Build output DataFrame ---
    rows = []
    for wi, (wave, kf, A, th_kf, kh_kf) in enumerate(
            zip(waves_detected, kf_all, complex_amplitudes, kf_theta_all, kf_kh_all)):
        lam_km  = float(wave['wavelength_km'])
        omk_az  = float(wave['azimuth'])   # fixed propagation direction from ω-k

        amp_cross = np.abs(A)
        amp_ref_w = float(np.max(amp_cross)) if amp_cross.max() > 0 else 1.0

        rms_wave_t = rms_wave_cubes[wi] if rms_wave_cubes is not None else None

        for t in range(n_time):
            omega_t = float(kf['omega'][t])
            speed_ms   = np.nan
            prop_az    = np.nan
            period_min = np.nan

            if np.isfinite(omega_t):
                speed_ms   = abs(omega_t) * lam_km / (2.0 * np.pi) * 1000.0
                prop_az    = omk_az
                if abs(omega_t) > 1e-8:
                    period_min = 2.0 * np.pi / abs(omega_t) / 60.0

            amp_t  = float(amp_cross[t])
            snr_ok = (amp_t / amp_ref_w) >= snr_min_amplitude
            innov_ok = bool(kf['innov_valid'][t]) and snr_ok

            # Activity metric (Feature B)
            act_frac = np.nan
            wave_act = False
            if have_activity:
                rms_ok_t = float(rms_omegak[t])
                rms_w_t  = float(rms_wave_t[t])
                if np.isfinite(rms_ok_t) and rms_ok_t > 0 and np.isfinite(rms_w_t):
                    act_frac = rms_w_t / rms_ok_t
                    wave_act = bool((act_frac > activity_threshold) and innov_ok)
                # else: act_frac stays NaN, wave_act stays False

            rows.append({
                'wave_id':                wi,
                'time':                   time_vals[t],
                'frame_idx':              t,
                'gabor_wavelength_km':    lam_km,
                'gabor_orientation_deg':  float(wave['orientation_deg']),
                'kf_phase_rad':           np.nan,
                'kf_omega_rads':          omega_t,
                'kf_wavespeed_ms':        speed_ms,
                'kf_prop_azimuth_deg':    prop_az,
                'kf_period_min':          period_min,
                'amplitude':              amp_t,
                'innovation_residual':    kf['innovation'][t],
                'innovation_valid':       innov_ok,
                'is_dominant':            bool(dominant[t] == wi),
                # Feature A
                'kf_orient_deg':          float(th_kf['theta_est_deg'][t]),
                'kf_orient_std_deg':      float(th_kf['theta_std_deg'][t]),
                'kf_lambda_obs_km':       float(th_kf['lambda_obs_km'][t]),
                'kernel_mismatch':        bool(th_kf['kernel_mismatch'][t]),
                # Feature B
                'activity_fraction':      act_frac,
                'wave_active':            wave_act,
                # Feature C — wavelength KF
                'kf_lambda_km':           _kh_to_lambda_km(kh_kf['kh_est'][t]),
                'kf_lambda_std_km':       _kh_std_to_lambda_std_km(
                                              kh_kf['kh_est'][t],
                                              kh_kf['kh_std'][t]),
            })

    df = pd.DataFrame(rows)

    if verbose and len(df) > 0:
        print('KALMAN ANALYSIS SUMMARY (valid frames only)')
        print('-' * 70)
        for wi, wave in enumerate(waves_detected):
            wdf = df[(df['wave_id'] == wi) & df['innovation_valid']]
            if len(wdf) == 0:
                print(f'  {wave["label"]}: no valid frames')
                continue
            speed_abs = wdf['kf_wavespeed_ms'].abs()
            n_act = int(df[(df['wave_id'] == wi)]['wave_active'].sum())
            n_all = int(len(df[df['wave_id'] == wi]))
            print(f'  {wave["label"]}: '
                  f'az={wdf["kf_prop_azimuth_deg"].mean():.0f}±'
                  f'{wdf["kf_prop_azimuth_deg"].std():.0f}°  '
                  f'speed={speed_abs.mean():.0f}±{speed_abs.std():.0f} m/s  '
                  f'T={wdf["kf_period_min"].mean():.1f} min  '
                  f'({len(wdf)} valid, {n_act}/{n_all} active)')
        print('=' * 70)

    return df


# ---------------------------------------------------------------------------
# Binned wave statistics (Feature B)
# ---------------------------------------------------------------------------

def compute_binned_wave_stats(
    kf_results: pd.DataFrame,
    bin_width_min: int = 60,
    min_valid_frames: int = 5,
) -> pd.DataFrame:
    """
    Compute temporal bin statistics per wave from Kalman results.

    Bins are aligned to whole hours (or multiples of ``bin_width_min``).

    Parameters
    ----------
    kf_results : pd.DataFrame
        Output of ``analyze_all_waves_kalman``.  Must contain columns
        ``wave_id``, ``time``, ``wave_active``, ``kf_wavespeed_ms``,
        ``kf_orient_deg``, ``activity_fraction``.
    bin_width_min : int
        Temporal bin width in minutes.  Default 60.
    min_valid_frames : int
        Minimum active frames required to report statistics for a bin.
        Default 5.

    Returns
    -------
    pd.DataFrame with columns: wave_id, bin_start, bin_end,
    n_frames_total, n_frames_active, duty_cycle, mean_speed_ms,
    std_speed_ms, mean_orient_deg, std_orient_deg, mean_activity,
    insufficient_data.
    """
    if len(kf_results) == 0:
        return pd.DataFrame()

    bin_width = pd.Timedelta(minutes=bin_width_min)

    t_min = pd.to_datetime(kf_results['time'].min())
    t_max = pd.to_datetime(kf_results['time'].max())

    # Align bin start to nearest whole bin boundary (e.g. whole hour)
    epoch = pd.Timestamp('2000-01-01')
    n_bins_offset = int((t_min - epoch) / bin_width)
    bin_origin = epoch + n_bins_offset * bin_width

    # Build bin starts covering [t_min, t_max]
    bin_starts = []
    t = bin_origin
    while t <= t_max:
        bin_starts.append(t)
        t += bin_width

    wave_ids = sorted(kf_results['wave_id'].unique())
    rows = []

    for wave_id in wave_ids:
        wdf = kf_results[kf_results['wave_id'] == wave_id].copy()
        wdf['time_pd'] = pd.to_datetime(wdf['time'])

        for bin_start in bin_starts:
            bin_end = bin_start + bin_width
            mask = (wdf['time_pd'] >= bin_start) & (wdf['time_pd'] < bin_end)
            bin_frames = wdf[mask]
            n_total = len(bin_frames)

            if n_total == 0:
                continue

            n_active = int(bin_frames['wave_active'].sum())
            duty_cycle = n_active / n_total
            active_frames = bin_frames[bin_frames['wave_active']]
            insufficient = n_active < min_valid_frames

            if insufficient or len(active_frames) == 0:
                rows.append({
                    'wave_id':         wave_id,
                    'bin_start':       bin_start,
                    'bin_end':         bin_end,
                    'n_frames_total':  n_total,
                    'n_frames_active': n_active,
                    'duty_cycle':      duty_cycle,
                    'mean_speed_ms':   np.nan,
                    'std_speed_ms':    np.nan,
                    'mean_orient_deg': np.nan,
                    'std_orient_deg':  np.nan,
                    'mean_activity':   np.nan,
                    'mean_lambda_km':  np.nan,
                    'std_lambda_km':   np.nan,
                    'insufficient_data': True,
                })
            else:
                spd    = active_frames['kf_wavespeed_ms'].values
                act    = active_frames['activity_fraction'].values
                orient = active_frames['kf_orient_deg'].values \
                    if 'kf_orient_deg' in active_frames.columns \
                    else np.full(len(active_frames), np.nan)

                orient_finite = orient[np.isfinite(orient)]
                circ_m = circular_mean(orient_finite) if len(orient_finite) > 0 else np.nan
                circ_s = circular_std(orient_finite)  if len(orient_finite) > 1 else np.nan

                lam_vals = active_frames['kf_lambda_km'].values \
                    if 'kf_lambda_km' in active_frames.columns \
                    else np.full(len(active_frames), np.nan)
                lam_finite = lam_vals[np.isfinite(lam_vals)]
                lam_mean = float(np.mean(lam_finite))   if len(lam_finite) > 0 else np.nan
                lam_std  = float(np.std(lam_finite))    if len(lam_finite) > 1 else np.nan

                rows.append({
                    'wave_id':         wave_id,
                    'bin_start':       bin_start,
                    'bin_end':         bin_end,
                    'n_frames_total':  n_total,
                    'n_frames_active': n_active,
                    'duty_cycle':      duty_cycle,
                    'mean_speed_ms':   float(np.nanmean(spd)),
                    'std_speed_ms':    float(np.nanstd(spd)),
                    'mean_orient_deg': circ_m,
                    'std_orient_deg':  circ_s,
                    'mean_activity':   float(np.nanmean(act)),
                    'mean_lambda_km':  lam_mean,
                    'std_lambda_km':   lam_std,
                    'insufficient_data': False,
                })

    if len(rows) == 0:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Regression test — run with:  python phase_speed_kalman.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    print("=" * 60)
    print("kernel_mismatch regression test")
    print("=" * 60)

    n = 30
    btheta_rad      = np.pi / 4          # 45° bandwidth (typical default)
    btheta_half_deg = np.degrees(btheta_rad) / 2.0   # 22.5°

    def _half_diff(theta_full, orient_half):
        """Half-circle distance between full-circle azimuth and [0°,180°) orientation."""
        theta_half = theta_full % 180.0
        return abs((theta_half - orient_half + 90.0) % 180.0 - 90.0)

    # ── Test 1: wide ramp where fixed-median bias is clear ────────────────
    # Ramp spans 70°, so endpoints deviate ±35° from the median.
    # 35° > btheta_half_deg=22.5°, so ~half the frames are wrongly flagged
    # by the old fixed-median comparison.
    orient_series = np.linspace(10.0, 80.0, n)       # [0°, 180°) adaptive kernel
    fixed_median  = float(np.median(orient_series))   # ≈ 45°
    kf_orient_full = orient_series.copy()              # KF perfectly tracks the ramp

    km_adaptive = np.array([
        _half_diff(kf_orient_full[t], orient_series[t]) > btheta_half_deg
        for t in range(n)
    ], dtype=bool)

    km_old = np.array([
        abs(circular_diff_deg(kf_orient_full[t], fixed_median)) > btheta_half_deg
        for t in range(n)
    ], dtype=bool)

    n_new = int(km_adaptive.sum())
    n_old = int(km_old.sum())
    print(f"  Wide ramp (10°→80°, median={fixed_median:.0f}°, btheta/2={btheta_half_deg:.1f}°):")
    print(f"    Adaptive (per-frame half-circle): {n_new}/{n} mismatches — expect 0")
    print(f"    Fixed-median (old buggy):         {n_old}/{n} mismatches — expect >0")

    assert n_new == 0, f"FAIL: adaptive mode produced {n_new} spurious mismatches"
    assert n_old > 0,  "FAIL: fixed-median mode should show mismatches on 70° ramp"
    print("  PASS: per-frame comparison eliminates spurious mismatches on wide ramp.")

    # ── Test 2: NaN frame → no mismatch ──────────────────────────────────
    orient_with_nan = orient_series.copy()
    orient_with_nan[5] = np.nan
    km_nan = np.array([
        False if np.isnan(orient_with_nan[t])
        else _half_diff(kf_orient_full[t], orient_with_nan[t]) > btheta_half_deg
        for t in range(n)
    ], dtype=bool)
    assert not km_nan[5], "FAIL: NaN frame should not be flagged as mismatch"
    print("  PASS: NaN frame treated as no-mismatch.")

    # ── Test 3: opposite-lobe — azimuth 210° with kernel centre 30° ──────
    # 210° and 30° represent the SAME physical orientation (210 % 180 = 30).
    # Half-circle diff should be 0°; old full-circle diff gives 180°.
    az_opposite = 210.0   # full-circle KF azimuth
    kc_half     = 30.0    # half-circle kernel centre  (210 % 180 = 30)

    half_d   = _half_diff(az_opposite, kc_half)
    full_d   = abs(circular_diff_deg(az_opposite, kc_half))
    km_new_opp = half_d > btheta_half_deg
    km_old_opp = full_d > btheta_half_deg

    print(f"  Opposite-lobe (az=210°, kernel=30°):")
    print(f"    Half-circle diff={half_d:.1f}° → mismatch={km_new_opp} (expect False)")
    print(f"    Full-circle diff={full_d:.1f}° → mismatch={km_old_opp} (expect True — old bug)")
    assert not km_new_opp, "FAIL: opposite-lobe should NOT be flagged (same physical orientation)"
    assert km_old_opp,     "FAIL: old code should flag opposite-lobe (confirming the bug exists)"
    print("  PASS: opposite-lobe case handled correctly.")

    # ── Test 4: genuine mismatch is still detected ────────────────────────
    # KF drifts 40° away from its kernel centre — should fire.
    orient_fixed = np.full(n, 30.0)
    kf_drifted   = np.full(n, 70.0)   # 40° away, > 22.5° threshold
    km_real = np.array([
        _half_diff(kf_drifted[t], orient_fixed[t]) > btheta_half_deg
        for t in range(n)
    ], dtype=bool)
    assert km_real.all(), "FAIL: genuine 40° drift should be flagged as mismatch"
    print(f"  PASS: genuine drift ({_half_diff(70.0, 30.0):.0f}° > {btheta_half_deg:.1f}°) "
          f"correctly flagged.")

    print("=" * 60)
    print("All regression tests passed.")
    sys.exit(0)
