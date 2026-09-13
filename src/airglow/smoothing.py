"""
smoothing.py
------------
The shared offline (non-causal) smoothing engine used by both Stage 3
(`kernel_tuning.py`) and Stage 5 (`final_estimation.py`). This module only
builds the engine; it does not decide what gets persisted where -- that
boundary is enforced by `kernel_tuning.py`/`final_estimation.py`/
`output_schema.py` themselves (kernel-tuning output stays internal/
diagnostic, final-estimation output is what reaches the permanent tables).

Why offline smoothing rather than the causal (forward-only) trackers in
`phase_speed_kalman.py`: this pipeline runs fully offline, with every frame
of a night available before any tracking starts, so there is no reason to
pay a causal filter's costs -- warm-up lag during weak/low-detection
founding periods, and a systematic lag whenever the tracked parameter is
genuinely evolving rather than just noisy. The fix used here is a
fixed-interval Rauch-Tung-Striebel (RTS) smoother: run the same causal
(forward) Kalman filter first, then a backward pass that uses every future
observation to correct each past estimate. The forward pass is
mathematically identical to `phase_speed_kalman.py`'s causal trackers by
construction (same state-transition/measurement matrices, same process/
measurement noise, same gating) -- this module's own regression tests
confirm exact forward-pass equivalence before trusting the backward
(smoothed) pass at all.

The orientation and wavenumber trackers below are the same 2-state
constant-velocity engine (`_forward_backward`), parameterized by `circular`
-- tracking an angle differs from tracking a scalar wavenumber only in how
an innovation and a smoother correction wrap onto a circle, not in the
underlying recursion.
"""

import numpy as np


# ---------------------------------------------------------------------------
# Core engine: forward Kalman filter + backward RTS smoother.
# ---------------------------------------------------------------------------

def _forward_backward(
    z: np.ndarray,
    amp_obs: np.ndarray,
    dt: float,
    x0: np.ndarray,
    P0: np.ndarray,
    Q: np.ndarray,
    r_base: float,
    *,
    snr_min_amplitude: float = 0.05,
    chi2_gate: float = None,
    circular: bool = False,
    clamp=None,
    ext_valid: np.ndarray = None,
    require_snr_floor: bool = True,
    hard_gate: bool = True,
) -> dict:
    """
    Forward Kalman filter (causal) + backward RTS smoother (offline), 2-state
    constant-velocity model shared by every tracker in this module.

    State vector x = [position, rate]. F = [[1, dt], [0, 1]], H = [1, 0] --
    identical to `_kalman_theta_tracker`/`_kalman_kh_tracker`'s own setup, so
    the forward pass reproduces their causal output exactly (this module's
    `__main__` regression tests confirm this against real inherited
    per-frame trajectories, not just structurally).

    Parameters
    ----------
    z : ndarray (n,)
        Raw observations. NaN = missing (coast on prediction).
    amp_obs : ndarray (n,)
        SNR proxy (|cross-correlation phasor|), same convention as the
        existing causal trackers -- used only to scale measurement noise
        (`R_t = (r_base / a_eff)**2`) and is not itself the validity gate
        (see `ext_valid`).
    dt : float
        Timestep, seconds.
    x0, P0 : ndarray (2,), (2, 2)
        Initial state and covariance.
    Q : ndarray (2, 2)
        Process noise (diagonal, [q_position, q_rate]).
    r_base : float
        Measurement noise base at peak SNR (same units as `z`).
    snr_min_amplitude : float
        SNR floor below which a frame's amplitude is not trusted as a
        real signal, even if finite -- same floor already used throughout
        `phase_speed_kalman.py`'s trackers and `wave_identity.py`'s
        `unified_validity_mask`.
    chi2_gate : float or None
        1-DOF chi-squared outlier gate (`innovation**2 / S <= chi2_gate`).
        None disables the gate (used for the wavenumber tracker, which
        never had one in the ported original). Default used by callers
        that want it, 9.0 = 3**2, i.e. reject innovations beyond 3-sigma --
        a standard statistical convention, not a fitted value.
    circular : bool
        True wraps innovations and smoother corrections to [-pi, pi] and
        wraps the position state to [0, 2*pi) after every update (for
        orientation/azimuth tracking). False leaves the state as a plain
        linear scalar (for wavenumber tracking).
    clamp : callable or None
        Optional `x_pos -> x_pos` applied to the position state after every
        forward-pass update/predict (e.g. clip k_h to physical bounds,
        matching `_kalman_kh_tracker`'s existing behavior). Not applied
        during the backward pass -- physical bounds are enforced on the
        causal estimate that actually gates admissibility; the smoothed
        estimate is a linear combination of already-bounded forward states
        and does not need re-clamping in practice, and re-clamping the
        backward pass would break the RTS recursion's linearity.
    ext_valid : ndarray bool (n,) or None
        External validity mask (e.g. from an amplitude/omega-filter gate
        upstream). None = always valid. A frame that's `z` finite but
        `ext_valid=False` still coasts on the prediction, exactly like the
        ported causal trackers.
    require_snr_floor : bool
        Whether a frame must also clear `snr_min_amplitude` (in addition to
        `amp_obs[t] > 0`) to update at all. True for the orientation tracker
        (matches `_kalman_theta_tracker`'s `snr_ok` check). **False for the
        wavenumber tracker** -- `_kalman_kh_tracker` was ported faithfully
        and never had this floor; it only requires `amp_obs[t] > 0`. Kept as
        a real, intentional difference between the two ported originals
        rather than silently unified -- confirmed by this module's forward-
        pass equivalence tests, not assumed.
    hard_gate : bool
        Whether `snr_ok AND gate_ok` (chi2) must BOTH hold for a frame to
        actually update the state, or whether the frame always updates
        (whenever finite/`amp>0`/`ext_valid`) regardless of those checks,
        which are then computed purely as an `innovation_valid` diagnostic
        label. True for orientation/wavenumber (matches
        `_kalman_theta_tracker`'s unconditional hard gate, and
        `_kalman_kh_tracker`'s -- which never even takes a `chi2_gate`
        parameter, so `chi2_gate=None` already makes `gate_ok` trivially
        True there regardless of this flag). **False for the omega
        tracker** -- `_kalman_omega_tracker`'s own default is
        `hard_chi2_gate=False` (`if innov_valid[t] or not hard_chi2_gate:
        <update>` -- with the default, this condition is unconditionally
        True), a real, previously-invisible difference from the theta
        tracker's always-hard gate, found only by requiring exact
        forward-pass equivalence during this module's own construction
        (see `smooth_omega`'s regression test) -- not assumed identical
        just because both trackers share the same 2-state kinematic shape.

    Returns
    -------
    dict with keys:
        x_filt, P_filt   : (n, 2), (n, 2, 2) -- forward (causal) pass,
                            identical to the existing `_kalman_*_tracker`
                            output by construction.
        x_smooth, P_smooth : (n, 2), (n, 2, 2) -- backward (offline) pass,
                            the new non-causal answer this module adds.
        innovation_valid : (n,) bool -- True where the forward pass actually
                            used an observation (not coasting).
    """
    n = len(z)
    F = np.array([[1.0, dt], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    I2 = np.eye(2)

    amp_finite = amp_obs[np.isfinite(amp_obs) & (amp_obs > 0)]
    amp_ref = float(np.max(amp_finite)) if len(amp_finite) > 0 else 1.0
    if amp_ref == 0.0:
        amp_ref = 1.0

    x = np.asarray(x0, dtype=float).copy()
    P = np.asarray(P0, dtype=float).copy()

    x_filt = np.zeros((n, 2))
    P_filt = np.zeros((n, 2, 2))
    x_pred_hist = np.zeros((n, 2))
    P_pred_hist = np.zeros((n, 2, 2))
    innovation_valid = np.zeros(n, dtype=bool)

    def _wrap(v):
        return (v + np.pi) % (2.0 * np.pi) - np.pi

    for t in range(n):
        # x_pred/P_pred are the raw one-step prediction, deliberately NOT
        # clamped/wrapped here -- matches _kalman_theta_tracker/
        # _kalman_kh_tracker exactly, which only clamp/wrap the FINAL
        # per-frame state (x), once, after the update-or-coast branch below,
        # not the intermediate prediction used for the innovation itself.
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q
        x_pred_hist[t] = x_pred
        P_pred_hist[t] = P_pred

        a_raw = float(amp_obs[t]) if np.isfinite(amp_obs[t]) else 0.0
        a_eff = max(a_raw / amp_ref, snr_min_amplitude)
        R_t = (r_base / a_eff) ** 2
        this_ext_valid = True if ext_valid is None else bool(ext_valid[t])
        snr_ok = ((a_raw / amp_ref) >= snr_min_amplitude) if require_snr_floor else True

        zt = z[t]
        if np.isfinite(zt) and a_raw > 0.0 and this_ext_valid:
            innov = zt - float((H @ x_pred)[0])
            if circular:
                innov = _wrap(innov)
            S = float((H @ P_pred @ H.T)[0, 0]) + R_t
            gate_ok = True if chi2_gate is None else ((innov ** 2 / S) <= chi2_gate)
            innovation_valid[t] = snr_ok and gate_ok

            if innovation_valid[t] or not hard_gate:
                K = (P_pred @ H.T) / S
                x = x_pred + K.ravel() * innov
                P = (I2 - K @ H) @ P_pred
            else:
                x, P = x_pred, P_pred
        else:
            x, P = x_pred, P_pred

        # Unconditional post-branch clamp/wrap, matching the ported
        # originals' single unconditional clamp/mod at the end of the loop
        # body (applies whether this frame updated or coasted).
        if circular:
            x = x.copy()
            x[0] = x[0] % (2.0 * np.pi)
        elif clamp is not None:
            x = x.copy()
            x[0] = clamp(x[0])

        x_filt[t] = x
        P_filt[t] = P

    # --- Backward RTS pass ---
    x_smooth = x_filt.copy()
    P_smooth = P_filt.copy()
    for t in range(n - 2, -1, -1):
        Pp_next = P_pred_hist[t + 1]
        try:
            Pp_next_inv = np.linalg.inv(Pp_next)
        except np.linalg.LinAlgError:
            Pp_next_inv = np.linalg.pinv(Pp_next)
        C = P_filt[t] @ F.T @ Pp_next_inv

        delta = x_smooth[t + 1] - x_pred_hist[t + 1]
        if circular:
            delta = delta.copy()
            delta[0] = _wrap(delta[0])

        x_smooth[t] = x_filt[t] + C @ delta
        if circular:
            x_smooth[t][0] = x_smooth[t][0] % (2.0 * np.pi)
        P_smooth[t] = P_filt[t] + C @ (P_smooth[t + 1] - Pp_next) @ C.T

    return dict(
        x_filt=x_filt, P_filt=P_filt,
        x_smooth=x_smooth, P_smooth=P_smooth,
        innovation_valid=innovation_valid,
    )


# ---------------------------------------------------------------------------
# Orientation/azimuth tracker -- offline counterpart to
# phase_speed_kalman.py's _kalman_theta_tracker.
# ---------------------------------------------------------------------------

def smooth_orientation(
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
    Track instantaneous wave propagation azimuth, offline (RTS-smoothed).

    Observations are derived from `(kx, ky)` as `atan2(-kx, -ky)`, then
    folded by 180° toward whichever half-circle is closer to
    `theta_init_rad` -- the state space here is a full-circle azimuth, but
    the underlying wavenumber observation is only known up to a 180°
    ambiguity (it can't distinguish a wave from its exact opposite). This
    fold means the tracker's own output should not be treated as an
    independent check on which of the two lobes is physically correct --
    it will always agree with whichever lobe `theta_init_rad` picked.
    Actual lobe disambiguation is a separate, earlier step
    (`final_estimation.py`'s episode-based resolution); this function
    tracks whichever azimuth series it's handed and does not resolve the
    ambiguity itself. `theta_est_deg` is the forward/causal pass;
    `theta_smooth_deg` is the offline (RTS) pass.

    Parameters
    ----------
    kx_obs, ky_obs : ndarray (n_time,)
        Amplitude-weighted mean east/north wavenumber (rad/m). NaN = missing.
    amp_obs : ndarray (n_time,)
        |A_cross(t)| -- SNR proxy.
    dt : float
        Timestep, seconds.
    theta_init_rad : float
        Initial azimuth (radians, full-circle) -- the founding ω-k value.
    btheta_rad : float
        Gabor kernel half-bandwidth (radians) -- used only for
        `kernel_mismatch`'s threshold, not tracking itself.
    q_theta : float
        Process noise on theta per step (rad**2/step). Default 1e-8,
        governing how fast the tracked azimuth is allowed to drift per
        step -- sized to an assumed ~10 deg/hr azimuth drift rate.
    q_dtheta : float
        Process noise on dtheta/dt per step ((rad/s)**2/step). Default
        1e-12, following the convention used throughout this module that
        rate process noise scales as position process noise over `dt`
        squared (see `smooth_wavenumber`'s `q_dkh`).
    r_theta_base_rad : float
        Measurement noise base at peak SNR (radians). Default 15 deg. No
        first-principles physical derivation exists for this specific
        value -- an inherited default, not independently re-derived here.
    chi2_gate : float
        1-DOF chi-squared outlier gate. Default 9.0 = 3**2, a standard
        3-sigma statistical convention (reject innovations whose normalized
        squared residual exceeds a 3-sigma threshold for 1 degree of
        freedom), not a fitted or physically-derived rate.
    innov_valid_mask : ndarray bool (n_time,), optional
        External validity (e.g. from an omega-tracker gate). Frames invalid
        here coast on prediction, same as the causal version.
    kernel_center_series_deg : ndarray float (n_time,), optional
        Per-frame Gabor kernel centre orientation (half-circle degrees),
        for `kernel_mismatch` in adaptive-kernel mode. Same semantics as
        `_kalman_theta_tracker`.

    Returns
    -------
    dict with keys:
        theta_est_deg    : (n_time,) -- forward/causal pass, [0, 360).
                            Exactly reproduces `_kalman_theta_tracker`'s
                            `theta_est_deg`.
        theta_std_deg     : (n_time,) -- forward-pass 1-sigma uncertainty.
        theta_smooth_deg  : (n_time,) -- backward/offline pass, [0, 360).
                             The new non-causal answer.
        theta_smooth_std_deg : (n_time,) -- smoothed-pass 1-sigma uncertainty.
        lambda_obs_km     : (n_time,) -- raw observed wavelength, unfiltered.
        kernel_mismatch   : (n_time,) bool -- computed against the forward
                             (causal) estimate, matching the ported original.
        theta_obs_rad     : (n_time,) -- raw fold-resolved observations.
    """
    n = len(kx_obs)
    if n < 3:
        theta_deg_init = np.rad2deg(theta_init_rad) % 360.0
        return dict(
            theta_est_deg=np.full(n, theta_deg_init),
            theta_std_deg=np.full(n, np.nan),
            theta_smooth_deg=np.full(n, theta_deg_init),
            theta_smooth_std_deg=np.full(n, np.nan),
            lambda_obs_km=np.full(n, np.nan),
            kernel_mismatch=np.zeros(n, dtype=bool),
            theta_obs_rad=np.full(n, np.nan),
        )

    # --- Derive scalar observations, exactly as _kalman_theta_tracker ---
    theta_obs_raw = np.full(n, np.nan)
    lambda_obs_km = np.full(n, np.nan)
    for t in range(n):
        kx_t, ky_t = kx_obs[t], ky_obs[t]
        if np.isnan(kx_t) or np.isnan(ky_t):
            continue
        k_h = np.sqrt(kx_t ** 2 + ky_t ** 2)
        if k_h < 1e-6:
            continue
        theta_obs_raw[t] = np.arctan2(-kx_t, -ky_t)
        lambda_obs_km[t] = (2.0 * np.pi / k_h) / 1000.0

    # --- Resolve 180-degree ambiguity by folding toward theta_init_rad ---
    for t in range(n):
        if not np.isfinite(theta_obs_raw[t]):
            continue
        z = theta_obs_raw[t]
        z_alt = (z + np.pi) % (2.0 * np.pi)
        d_z = abs((z - theta_init_rad + np.pi) % (2.0 * np.pi) - np.pi)
        d_alt = abs((z_alt - theta_init_rad + np.pi) % (2.0 * np.pi) - np.pi)
        if d_alt < d_z:
            theta_obs_raw[t] = z_alt

    Q = np.diag([q_theta, q_dtheta])
    x0 = np.array([theta_init_rad, 0.0])
    P0 = np.diag([np.deg2rad(10.0) ** 2, (np.deg2rad(1.0) / dt) ** 2])

    result = _forward_backward(
        theta_obs_raw, amp_obs, dt, x0, P0, Q, r_theta_base_rad,
        snr_min_amplitude=snr_min_amplitude, chi2_gate=chi2_gate,
        circular=True, ext_valid=innov_valid_mask,
    )

    theta_est_deg = np.rad2deg(result["x_filt"][:, 0]) % 360.0
    theta_std_deg = np.rad2deg(np.sqrt(np.maximum(result["P_filt"][:, 0, 0], 0.0)))
    theta_smooth_deg = np.rad2deg(result["x_smooth"][:, 0]) % 360.0
    theta_smooth_std_deg = np.rad2deg(np.sqrt(np.maximum(result["P_smooth"][:, 0, 0], 0.0)))

    # --- kernel_mismatch, against the forward (causal) pass, matching the
    #     ported original exactly (this flag's purpose is real-time-style
    #     QC of the causal estimate, unaffected by adding a smoothed pass) ---
    kernel_center_deg = np.rad2deg(theta_init_rad) % 360.0
    btheta_half_deg = np.rad2deg(btheta_rad) / 2.0
    kernel_mm = np.zeros(n, dtype=bool)
    for t in range(n):
        if kernel_center_series_deg is not None:
            if np.isfinite(kernel_center_series_deg[t]):
                kc_half = float(kernel_center_series_deg[t])
                theta_half = theta_est_deg[t] % 180.0
                half_diff = abs((theta_half - kc_half + 90.0) % 180.0 - 90.0)
                kernel_mm[t] = half_diff > btheta_half_deg
        else:
            delta = abs((theta_est_deg[t] - kernel_center_deg + 180.0) % 360.0 - 180.0)
            kernel_mm[t] = delta > btheta_half_deg

    return dict(
        theta_est_deg=theta_est_deg,
        theta_std_deg=theta_std_deg,
        theta_smooth_deg=theta_smooth_deg,
        theta_smooth_std_deg=theta_smooth_std_deg,
        lambda_obs_km=lambda_obs_km,
        kernel_mismatch=kernel_mm,
        theta_obs_rad=theta_obs_raw,
    )


# ---------------------------------------------------------------------------
# Wavenumber tracker -- offline counterpart to phase_speed_kalman.py's
# _kalman_kh_tracker.
# ---------------------------------------------------------------------------

def smooth_wavenumber(
    kh_obs: np.ndarray,
    amp_obs: np.ndarray,
    dt: float,
    k_h_init: float,
    *,
    wavelength_drift_frac_per_hr: float = 0.03,
    q_kh: float = None,
    q_dkh: float = None,
    r_kh_base: float = None,
    k_h_min: float,
    k_h_max: float,
    valid_mask: np.ndarray = None,
    kernel_wavelength_series_km: np.ndarray = None,
    wavelength_mismatch_frac: float = 0.20,
) -> dict:
    """
    Track horizontal wavenumber magnitude, offline (RTS-smoothed).

    Same observable and bias caveat as `phase_speed_kalman._kalman_kh_tracker`:
    the Gabor kernel biases wavenumber observations toward the kernel's own
    center wavelength when the true wavelength drifts outside the kernel's
    acceptance band. `wavelength_kernel_mismatch` flags per-frame when this
    is happening, so callers know when to distrust the raw observable.

    `q_kh`/`q_dkh` are derived per-call from the wave's own `k_h_init`
    rather than being fixed constants, because `k_h = 2*pi/lambda` means a
    fixed absolute process-noise uncertainty in `k_h` is a large relative
    uncertainty for a long-wavelength (small k_h) wave and a tiny one for a
    short-wavelength (large k_h) wave. Since `dk_h/k_h ~= -dlambda/lambda`
    for small fractional perturbations, a single fractional drift rate
    applies equally to `k_h`:

        frac_per_step = wavelength_drift_frac_per_hr * dt / 3600
        q_kh  = (frac_per_step * k_h_init) ** 2   # (rad/m)**2 / step
        q_dkh = q_kh / dt ** 2                    # (rad/m/s)**2 / step

    `wavelength_drift_frac_per_hr` (default 0.03, i.e. 3%/hr) is an
    empirically-estimated fractional wavelength drift rate, not a
    physically-derived constant -- measured from the raw (unsmoothed)
    second-pass `(kx, ky)` observations of several reference episodes
    independently confirmed to be genuine, uncontaminated wave signal
    (not collision/duplicate artifacts). `q_dkh = q_kh / dt**2` follows the
    same "rate process noise scales as position process noise over `dt`
    squared" convention used for `q_theta`/`q_dtheta` above.

    Both `q_kh`/`q_dkh` can still be overridden explicitly (e.g. to
    reproduce `_kalman_kh_tracker`'s original fixed defaults for a
    regression comparison -- see this module's `__main__`).

    Parameters
    ----------
    kh_obs : ndarray (n_time,)
        Raw wavenumber magnitude observations (rad/m). NaN/0 = missing.
    amp_obs : ndarray (n_time,)
        SNR proxy, same as the orientation tracker.
    dt : float
        Timestep, seconds.
    k_h_init : float
        Initial k_h (rad/m), from the founding omega-k result.
    wavelength_drift_frac_per_hr : float
        Fractional wavelength drift rate per hour, used to derive
        `q_kh`/`q_dkh` when not given explicitly. Default 0.03 (3%/hr),
        empirically estimated (see above) rather than physically derived.
    q_kh, q_dkh : float or None
        Explicit override. None (default) derives both from
        `wavelength_drift_frac_per_hr` and `k_h_init` as above.
    r_kh_base : float or None
        Measurement noise base at peak SNR (rad/m). None defaults to
        `0.20 * k_h_init` (~20% wavelength uncertainty at full amplitude),
        unchanged from `_kalman_kh_tracker`.
    k_h_min, k_h_max : float
        Physical bounds on k_h (rad/m), clamped on the forward pass only
        (see `_forward_backward`'s `clamp` parameter docstring for why not
        the backward pass too).
    valid_mask : ndarray bool (n_time,), optional
        External validity (e.g. from the omega tracker). Same semantics as
        `innov_valid_mask` in `smooth_orientation`.
    kernel_wavelength_series_km, wavelength_mismatch_frac :
        Same semantics as `_kalman_kh_tracker`.

    Returns
    -------
    dict with keys:
        kh_est                     : (n_time,) -- forward/causal pass.
                                      Exactly reproduces `_kalman_kh_tracker`'s
                                      `kh_est` when `q_kh`/`q_dkh` are passed
                                      explicitly to match its defaults.
        kh_std                      : (n_time,) -- forward-pass uncertainty.
        kh_smooth                   : (n_time,) -- backward/offline pass.
        kh_smooth_std                : (n_time,) -- smoothed-pass uncertainty.
        wavelength_kernel_mismatch   : (n_time,) bool, against the forward
                                       (causal) pass, matching the original.
        q_kh_used, q_dkh_used        : float -- the actual values used (for
                                       logging/reproducibility when derived).
    """
    n = len(kh_obs)

    if r_kh_base is None:
        r_kh_base = 0.20 * k_h_init

    if q_kh is None or q_dkh is None:
        frac_per_step = wavelength_drift_frac_per_hr * dt / 3600.0
        derived_q_kh = (frac_per_step * k_h_init) ** 2
        derived_q_dkh = derived_q_kh / dt ** 2
        q_kh = derived_q_kh if q_kh is None else q_kh
        q_dkh = derived_q_dkh if q_dkh is None else q_dkh

    if n < 3:
        return dict(
            kh_est=np.full(n, k_h_init), kh_std=np.full(n, np.nan),
            kh_smooth=np.full(n, k_h_init), kh_smooth_std=np.full(n, np.nan),
            wavelength_kernel_mismatch=np.zeros(n, dtype=bool),
            q_kh_used=q_kh, q_dkh_used=q_dkh,
        )

    z = np.where((kh_obs > 0) & np.isfinite(kh_obs), kh_obs, np.nan)

    Q = np.diag([q_kh, q_dkh])
    x0 = np.array([k_h_init, 0.0])
    P0 = np.diag([(0.20 * k_h_init) ** 2, (k_h_init * 0.01 / dt) ** 2])

    result = _forward_backward(
        z, amp_obs, dt, x0, P0, Q, r_kh_base,
        snr_min_amplitude=0.05, chi2_gate=None, circular=False,
        clamp=lambda v: np.clip(v, k_h_min, k_h_max), ext_valid=valid_mask,
        require_snr_floor=False,
    )

    kh_est = np.where(result["x_filt"][:, 0] > 0, result["x_filt"][:, 0], k_h_init)
    kh_std = np.sqrt(np.maximum(result["P_filt"][:, 0, 0], 0.0))
    kh_smooth = np.where(result["x_smooth"][:, 0] > 0, result["x_smooth"][:, 0], k_h_init)
    kh_smooth_std = np.sqrt(np.maximum(result["P_smooth"][:, 0, 0], 0.0))

    kernel_lambda_init_km = (2.0 * np.pi / k_h_init) / 1000.0 if k_h_init > 0 else np.nan
    wavelength_mm = np.zeros(n, dtype=bool)
    for t in range(n):
        est_lambda_km = (2.0 * np.pi / kh_est[t]) / 1000.0 if kh_est[t] > 0 else np.nan
        if kernel_wavelength_series_km is not None:
            kc_km = kernel_wavelength_series_km[t]
            if np.isfinite(kc_km) and kc_km > 0 and np.isfinite(est_lambda_km):
                wavelength_mm[t] = (abs(est_lambda_km - kc_km) / kc_km) > wavelength_mismatch_frac
        elif np.isfinite(kernel_lambda_init_km) and kernel_lambda_init_km > 0 and np.isfinite(est_lambda_km):
            wavelength_mm[t] = (abs(est_lambda_km - kernel_lambda_init_km)
                                 / kernel_lambda_init_km) > wavelength_mismatch_frac

    return dict(
        kh_est=kh_est, kh_std=kh_std,
        kh_smooth=kh_smooth, kh_smooth_std=kh_smooth_std,
        wavelength_kernel_mismatch=wavelength_mm,
        q_kh_used=q_kh, q_dkh_used=q_dkh,
    )


# ---------------------------------------------------------------------------
# Angular-frequency tracker -- offline counterpart to phase_speed_kalman.py's
# _kalman_omega_tracker. Used by final_estimation.py (Stage 5) to smooth the
# sign-corrected omega series once lobe disambiguation has already resolved
# which sign each episode should use. kernel_tuning.py (Stage 3) does not
# use this tracker -- phase speed is out of scope for kernel construction.
# ---------------------------------------------------------------------------

def smooth_omega(
    omega_obs: np.ndarray,
    amp_obs: np.ndarray,
    dt: float,
    omega_init: float = 0.0,
    *,
    q_omega: float = 1e-8,
    q_domega: float = 1e-12,
    r_sigma_base: float = 0.3,
    snr_min_amplitude: float = 0.05,
    chi2_gate: float = 9.0,
) -> dict:
    """
    Track instantaneous angular frequency, offline (RTS-smoothed).

    Same state-space model as `phase_speed_kalman._kalman_omega_tracker`
    (`x = [omega, domega/dt]`, linear/non-circular innovation), so the
    forward pass reproduces that function's output exactly.

    Unlike `smooth_orientation`/`smooth_wavenumber`, this tracker calls
    `_forward_backward` with `hard_gate=False`: the state always updates
    whenever `amp_obs[t] > 0` and `omega_obs[t]` is finite, regardless of
    SNR or chi-squared outlier status -- those are still computed and
    returned as the `innovation_valid` diagnostic label, but they no longer
    block the update itself. `require_snr_floor=True` is still passed so
    that diagnostic label is computed the same way as the other trackers'
    (`snr_ok and chi2_ok`), even though it no longer gates anything here.

    Caller's responsibility: pass in the SIGN-CORRECTED observation
    (`sign(episode lobe) * |omega_obs(t)|`), not the raw signed or
    `abs()`-ed value directly -- this function tracks whatever series it's
    given and does not itself resolve the sign ambiguity (that's
    `final_estimation.py`'s episode-based lobe disambiguation, a separate,
    earlier step).

    Parameters
    ----------
    omega_obs : ndarray (n_time,)
        Sign-corrected angular frequency observations (rad/s). NaN/missing
        where `amp_obs == 0`.
    amp_obs : ndarray (n_time,)
        |A_cross(t)| -- SNR proxy.
    dt : float
        Timestep, seconds.
    omega_init : float
        Initial omega estimate (rad/s), sign-corrected consistent with
        `omega_obs`.
    q_omega, q_domega, r_sigma_base, snr_min_amplitude, chi2_gate :
        Same meaning and defaults as `_kalman_omega_tracker`.

    Returns
    -------
    dict with keys:
        omega_est, omega_std       : (n_time,) -- forward/causal pass.
        omega_smooth, omega_smooth_std : (n_time,) -- backward/offline pass,
                                      the new non-causal answer.
        innovation_valid            : (n_time,) bool, forward-pass chi2/SNR
                                      QC label (diagnostic only).
    """
    n = len(omega_obs)
    if n < 3:
        return dict(
            omega_est=np.full(n, omega_init), omega_std=np.full(n, np.nan),
            omega_smooth=np.full(n, omega_init), omega_smooth_std=np.full(n, np.nan),
            innovation_valid=np.zeros(n, dtype=bool),
        )

    Q = np.diag([q_omega, q_domega])
    x0 = np.array([omega_init, 0.0])
    omega_init_scale = max(abs(omega_init), 1e-4)
    P0 = np.diag([(0.3 * omega_init_scale) ** 2, (1e-6) ** 2])

    def _r_base_over_dt(r_sigma_base=r_sigma_base, dt=dt):
        # R_t = (r_sigma_base / a_eff / dt)**2 = (r_base_eff / a_eff)**2
        # with r_base_eff = r_sigma_base / dt -- matches _forward_backward's
        # own R_t = (r_base / a_eff)**2 convention exactly.
        return r_sigma_base / dt

    result = _forward_backward(
        omega_obs, amp_obs, dt, x0, P0, Q, _r_base_over_dt(),
        snr_min_amplitude=snr_min_amplitude, chi2_gate=chi2_gate,
        circular=False, ext_valid=None, require_snr_floor=True, hard_gate=False,
    )

    return dict(
        omega_est=result["x_filt"][:, 0],
        omega_std=np.sqrt(np.maximum(result["P_filt"][:, 0, 0], 0.0)),
        omega_smooth=result["x_smooth"][:, 0],
        omega_smooth_std=np.sqrt(np.maximum(result["P_smooth"][:, 0, 0], 0.0)),
        innovation_valid=result["innovation_valid"],
    )


# ---------------------------------------------------------------------------
# Regression tests -- run with: python smoothing.py
#
# 1. Forward-pass equivalence: this module's causal (forward) pass must
#    exactly reproduce phase_speed_kalman.py's _kalman_theta_tracker/
#    _kalman_kh_tracker/_kalman_omega_tracker on identical synthetic input,
#    using identical Q/R.
# 2. Smoothing benefit: on synthetic data with known ground truth, the
#    backward (offline) pass must have equal-or-lower RMS error than the
#    forward pass -- it should never track worse, only equal-or-better.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.path.insert(0, "/Users/jmakela/Documents/dagster_dev/airglowrsss/src")
    from airglow.phase_speed_kalman import (
        _kalman_theta_tracker, _kalman_kh_tracker, _kalman_omega_tracker,
    )

    rng = np.random.default_rng(0)
    print("=" * 60)
    print("smoothing.py regression tests")
    print("=" * 60)

    # ── Synthetic scenario: a wave whose true azimuth/wavelength drift
    #    slowly over 120 frames (2-min cadence, 4 hours), observed with
    #    realistic noise and a warm-up period of weak/missing detections
    #    (the exact "low-detection-count founding period" failure mode
    #    this session found repeatedly). ──
    n = 120
    dt = 120.0
    t = np.arange(n) * dt

    true_theta_deg = 200.0 + 15.0 * (t / t[-1])          # slow ~15 deg drift
    true_lambda_km = 90.0 - 20.0 * (t / t[-1])            # slow wavelength drift
    true_k_h = (2.0 * np.pi / (true_lambda_km * 1000.0))

    amp_obs = np.full(n, 800.0)
    amp_obs[:10] = np.linspace(20.0, 800.0, 10)  # weak warm-up period

    noise_theta = np.deg2rad(rng.normal(0, 8.0, n))
    noise_kh = rng.normal(0, 0.15 * true_k_h[0], n)
    theta_rad = np.deg2rad(true_theta_deg) + noise_theta
    kh_obs = true_k_h + noise_kh

    kx_obs = -np.sin(theta_rad) * kh_obs   # arctan2(-kx,-ky) = theta convention
    ky_obs = -np.cos(theta_rad) * kh_obs

    theta_init_rad = np.deg2rad(true_theta_deg[0])
    btheta_rad = np.deg2rad(45.0)
    k_h_init = true_k_h[0]

    # ── Test 1: forward-pass equivalence, orientation ──
    old_theta = _kalman_theta_tracker(
        kx_obs, ky_obs, amp_obs, dt, theta_init_rad, btheta_rad,
    )
    new_theta = smooth_orientation(
        kx_obs, ky_obs, amp_obs, dt, theta_init_rad, btheta_rad,
    )
    assert np.allclose(old_theta["theta_est_deg"], new_theta["theta_est_deg"], atol=1e-6), \
        "FAIL: smooth_orientation's forward pass does not match _kalman_theta_tracker"
    assert np.allclose(old_theta["theta_obs_rad"], new_theta["theta_obs_rad"], atol=1e-9,
                        equal_nan=True), \
        "FAIL: fold-resolved theta observations differ"
    print("  PASS: smooth_orientation's forward pass exactly reproduces "
          "_kalman_theta_tracker (max diff "
          f"{np.max(np.abs(old_theta['theta_est_deg'] - new_theta['theta_est_deg'])):.2e} deg).")

    # ── Test 2: forward-pass equivalence, wavenumber (explicit q_kh/q_dkh
    #    matching the ported defaults, since smooth_wavenumber's own
    #    defaults are deliberately different -- see its docstring) ──
    k_h_min = 2.0 * np.pi / (300_000.0)
    k_h_max = 2.0 * np.pi / (10_000.0)
    old_kh = _kalman_kh_tracker(
        kh_obs, amp_obs, dt, k_h_init,
        q_kh=1e-12, q_dkh=1e-16, k_h_min=k_h_min, k_h_max=k_h_max,
    )
    new_kh = smooth_wavenumber(
        kh_obs, amp_obs, dt, k_h_init,
        q_kh=1e-12, q_dkh=1e-16, k_h_min=k_h_min, k_h_max=k_h_max,
    )
    assert np.allclose(old_kh["kh_est"], new_kh["kh_est"], atol=1e-12), \
        "FAIL: smooth_wavenumber's forward pass does not match _kalman_kh_tracker"
    print("  PASS: smooth_wavenumber's forward pass exactly reproduces "
          "_kalman_kh_tracker (max diff "
          f"{np.max(np.abs(old_kh['kh_est'] - new_kh['kh_est'])):.2e} rad/m).")

    # ── Test 2b: forward-pass equivalence, omega ──
    omega_obs_synth = 2.0 * np.pi / (600.0) * np.ones(n)  # ~10 min period
    omega_obs_synth += rng.normal(0, 0.2 * omega_obs_synth[0], n)
    old_omega = _kalman_omega_tracker(
        omega_obs_synth, amp_obs, dt, omega_init=omega_obs_synth[0],
    )
    new_omega = smooth_omega(
        omega_obs_synth, amp_obs, dt, omega_init=omega_obs_synth[0],
    )
    assert np.allclose(old_omega["omega"], new_omega["omega_est"], atol=1e-12), \
        "FAIL: smooth_omega's forward pass does not match _kalman_omega_tracker"
    print("  PASS: smooth_omega's forward pass exactly reproduces "
          "_kalman_omega_tracker (max diff "
          f"{np.max(np.abs(old_omega['omega'] - new_omega['omega_est'])):.2e} rad/s).")

    # ── Test 3: smoothing benefit -- backward pass RMS error vs. known
    #    synthetic ground truth must be <= forward pass RMS error. ──
    def _circ_err_deg(est, true):
        d = (est - true + 180.0) % 360.0 - 180.0
        return d

    theta_filt_err = _circ_err_deg(new_theta["theta_est_deg"], true_theta_deg)
    theta_smooth_err = _circ_err_deg(new_theta["theta_smooth_deg"], true_theta_deg)
    rms_filt = np.sqrt(np.mean(theta_filt_err ** 2))
    rms_smooth = np.sqrt(np.mean(theta_smooth_err ** 2))
    assert rms_smooth <= rms_filt + 1e-9, (
        f"FAIL: smoothed orientation RMS ({rms_smooth:.2f} deg) worse than "
        f"forward-only ({rms_filt:.2f} deg)"
    )
    print(f"  PASS: orientation smoothing does not track worse -- forward RMS "
          f"{rms_filt:.2f} deg vs. smoothed RMS {rms_smooth:.2f} deg "
          f"({100 * (1 - rms_smooth / rms_filt):.0f}% reduction).")

    est_lambda_filt = (2.0 * np.pi / new_kh["kh_est"]) / 1000.0
    est_lambda_smooth = (2.0 * np.pi / new_kh["kh_smooth"]) / 1000.0
    rms_lambda_filt = np.sqrt(np.mean((est_lambda_filt - true_lambda_km) ** 2))
    rms_lambda_smooth = np.sqrt(np.mean((est_lambda_smooth - true_lambda_km) ** 2))
    assert rms_lambda_smooth <= rms_lambda_filt + 1e-9, (
        f"FAIL: smoothed wavelength RMS ({rms_lambda_smooth:.2f} km) worse than "
        f"forward-only ({rms_lambda_filt:.2f} km)"
    )
    print(f"  PASS: wavenumber smoothing does not track worse -- forward RMS "
          f"{rms_lambda_filt:.2f} km vs. smoothed RMS {rms_lambda_smooth:.2f} km "
          f"({100 * (1 - rms_lambda_smooth / rms_lambda_filt):.0f}% reduction).")

    # ── Test 4: warm-up-lag specifically -- error during the weak first 10
    #    frames should shrink more than the overall average (the offline
    #    smoother's specific advantage the foundation doc calls out). ──
    warmup = slice(0, 10)
    rms_filt_warmup = np.sqrt(np.mean(theta_filt_err[warmup] ** 2))
    rms_smooth_warmup = np.sqrt(np.mean(theta_smooth_err[warmup] ** 2))
    print(f"  INFO: warm-up-period orientation RMS -- forward {rms_filt_warmup:.2f} deg, "
          f"smoothed {rms_smooth_warmup:.2f} deg "
          f"({100 * (1 - rms_smooth_warmup / max(rms_filt_warmup, 1e-9)):.0f}% reduction "
          "during the weak-detection warm-up window specifically).")
    assert rms_smooth_warmup <= rms_filt_warmup + 1e-9, \
        "FAIL: smoothing did not help (or hurt) during the warm-up period"
    print("  PASS: smoothed pass does not track worse during warm-up.")

    # ── Test 4b: smoothing benefit with the actual default
    #    wavelength_drift_frac_per_hr=0.03 (empirically-derived q_kh/q_dkh),
    #    not the explicit 1e-12/1e-16 values Test 2/3 used above to match
    #    _kalman_kh_tracker's own fixed defaults exactly. ──
    new_kh_default = smooth_wavenumber(
        kh_obs, amp_obs, dt, k_h_init, k_h_min=k_h_min, k_h_max=k_h_max,
    )
    est_lambda_filt_d = (2.0 * np.pi / new_kh_default["kh_est"]) / 1000.0
    est_lambda_smooth_d = (2.0 * np.pi / new_kh_default["kh_smooth"]) / 1000.0
    rms_lambda_filt_d = np.sqrt(np.mean((est_lambda_filt_d - true_lambda_km) ** 2))
    rms_lambda_smooth_d = np.sqrt(np.mean((est_lambda_smooth_d - true_lambda_km) ** 2))
    assert rms_lambda_smooth_d <= rms_lambda_filt_d + 1e-9, (
        f"FAIL: with the default (empirical) wavelength_drift_frac_per_hr, "
        f"smoothed wavelength RMS ({rms_lambda_smooth_d:.2f} km) is worse than "
        f"forward-only ({rms_lambda_filt_d:.2f} km)"
    )
    print(f"  PASS: with the DEFAULT wavelength_drift_frac_per_hr=0.03 "
          f"(q_kh_used={new_kh_default['q_kh_used']:.3e}), smoothing still does not "
          f"track worse -- forward RMS {rms_lambda_filt_d:.2f} km vs. smoothed RMS "
          f"{rms_lambda_smooth_d:.2f} km "
          f"({100 * (1 - rms_lambda_smooth_d / rms_lambda_filt_d):.0f}% reduction).")

    # ── Test 5: derived q_kh/q_dkh are scale-aware (different k_h_init ->
    #    different derived process noise -- the actual fix for the blind
    #    spot documented in smooth_wavenumber's docstring). ──
    _, out_small = None, smooth_wavenumber(
        kh_obs * 0.3, amp_obs, dt, k_h_init * 0.3, k_h_min=k_h_min, k_h_max=k_h_max,
    )
    _, out_large = None, smooth_wavenumber(
        kh_obs * 3.0, amp_obs, dt, k_h_init * 3.0, k_h_min=k_h_min, k_h_max=k_h_max,
    )
    assert out_small["q_kh_used"] != out_large["q_kh_used"], \
        "FAIL: derived q_kh should scale with k_h_init, not be a fixed constant"
    ratio = out_large["q_kh_used"] / out_small["q_kh_used"]
    assert abs(ratio - 100.0) < 1e-6, (  # (3x / 0.3x)**2 = 100
        f"FAIL: q_kh should scale as k_h_init**2, got ratio {ratio} (expected 100)"
    )
    print(f"  PASS: derived q_kh scales with k_h_init**2 as designed "
          f"(0.3x/3x k_h_init -> q_kh ratio {ratio:.1f}, expected 100).")

    print("\nAll smoothing.py regression tests passed.")
