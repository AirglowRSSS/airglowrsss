"""
smoothing.py
------------
The one shared offline (non-causal) smoothing engine for step 3
(kernel_tuning.py) and step 5 (final_estimation.py), per
`refactor_foundation.md`'s revised architecture:

    "Since step 3 doesn't need to be causal either ... both steps 3 and 5
    can reasonably share one underlying offline-smoothing engine ... Demand:
    kernel-tuning output stays internal/diagnostic ... Final-estimation
    output is the only thing that reaches the persisted final tables.
    Enforce this as a persistence/API boundary (what gets written where),
    not by duplicating the smoothing machinery itself."

This module builds that engine. It does NOT decide what's persisted where --
that's `kernel_tuning.py`/`final_estimation.py`/`output_schema.py`'s job.

Why offline smoothing instead of the causal (forward-only) Kalman trackers
already in `phase_speed_kalman.py`: the causal framing there
(`_kalman_theta_tracker`, `_kalman_kh_tracker`) was inherited from Kalman
filters being the default tool, not from any real-time requirement -- this
pipeline is fully offline, every frame of every night is available before
tracking ever starts. A causal filter pays two costs that don't apply here:
early-episode warm-up lag (this session's recurring finding: weak,
low-detection-count founding periods are exactly where things go wrong most)
and a systematic lag whenever the tracked parameter is genuinely evolving,
not just noisy. The standard fix for "offline access to the whole
sequence, want the best estimate at every point" is a fixed-interval
Rauch-Tung-Striebel (RTS) smoother: run the causal (forward) Kalman filter
first, exactly as before, then run a backward pass that uses every future
observation to correct each past estimate. The forward pass is mathematically
identical to the existing causal trackers by construction (same F/H/Q/R,
same gating) -- this module's own regression tests confirm exact forward-pass
equivalence with `_kalman_theta_tracker`/`_kalman_kh_tracker` before ever
trusting the backward (smoothed) pass.

Both trackers below (orientation, wavenumber) are literally the same 2-state
constant-velocity-model engine (`_forward_backward`), parameterized by
`circular` -- tracking an angle and tracking a scalar wavenumber differ only
in how an innovation and a smoother correction wrap onto a circle, not in
the underlying recursion. Building this as one function, not two, is the
actual point of this module per the foundation document -- a single engine
shared by both quantities (and, per its docstring above, by both steps 3
and 5), rather than the parameter-tuning duplication `_kalman_theta_tracker`/
`_kalman_kh_tracker` already show (near-identical loops, independently
written).
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

    Observation derivation, 180-degree fold-to-`theta_init_rad`, and
    `kernel_mismatch` computation are a faithful port of
    `_kalman_theta_tracker` (`phase_speed_kalman.py:273`) -- unchanged, so
    this function's `theta_est_deg` (the forward/causal pass) reproduces
    that function's output exactly; see this module's regression tests.
    `theta_smooth_deg` is the new backward-pass (offline) answer.

    The fold toward `theta_init_rad` is carried forward as-is: it is a
    real, known limitation for lobe disambiguation specifically
    (`REFACTOR_DESIGN.md` Task on the causal/kernel-tuning blurring --
    `kf_orient_deg` is "actively wrong as an independent check on the
    founding value itself," `SECOND_PASS_INDEPENDENT_ESTIMATION.md`), which
    is why `refactor_foundation.md` scopes lobe disambiguation to happen
    BEFORE `final_estimation.py` calls this function on the second pass
    ("calls smoothing.py on second-pass observations, after lobe
    disambiguation") -- not something this offline engine fixes on its own,
    same as the causal version never did. Fine for step 3's purpose
    (kernel-tuning signal, never persisted to the final schema).

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
        unchanged from `_kalman_theta_tracker` -- see `KF_Q_THETA`'s
        existing derivation (`CLAUDE_original_2026-07-02.md` Section 3):
        sized to an assumed ~10 deg/hr azimuth drift rate,
        Q ~ (rate_deg_per_step * pi/180)**2. Carried forward, not
        re-derived here (already had a stated rationale, unlike q_kh below).
    q_dtheta : float
        Process noise on dtheta/dt per step ((rad/s)**2/step). Default
        1e-12, unchanged -- empirically consistent with
        `q_theta / dt**2 ~ 7e-13` at dt=120s, i.e. the same
        "rate process noise scales as position process noise over dt
        squared" relationship this module states explicitly and uses for
        `q_dkh` below (see `smooth_wavenumber`), not independently derived
        for theta.
    r_theta_base_rad : float
        Measurement noise base at peak SNR (radians). Default 15 deg,
        unchanged from `_kalman_theta_tracker`. **No physical derivation
        found for this specific value** (checked alongside `q_kh`/`q_dkh`
        during this module's construction) -- carried forward as an
        inherited, unjustified default, same status as `smoothing_window`
        in `wave_identity.py`'s `resolve_collisions`. Flagged, not silently
        treated as validated.
    chi2_gate : float
        1-DOF chi-squared outlier gate. Default 9.0 = 3**2 -- a standard
        3-sigma statistical convention (reject innovations whose normalized
        squared residual exceeds a 3-sigma threshold for 1 degree of
        freedom), not a fitted or assumed physical rate. Unchanged from
        `_kalman_theta_tracker`.
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

    Offline counterpart to `_kalman_kh_tracker` (`phase_speed_kalman.py:538`)
    -- same observable, same bias caveat (see that function's module-level
    comment: the observable is pulled toward the Gabor kernel's own centre
    wavelength when the true wavelength drifts outside the kernel's
    passband; `wavelength_kernel_mismatch` flags exactly when to distrust
    this).

    `q_kh`/`q_dkh` derivation (this module's answer to
    `stage1_closeout_task.md`'s "finally derive one for whatever succeeds
    kf_q_kh/kf_q_dkh, which was found to have none either"):

    The ported originals (`kf_q_kh=1e-12`, `kf_q_dkh=1e-16`) are fixed
    absolute constants with no stated derivation anywhere in the codebase
    or its git history (`FAMILY6_WAVELENGTH_JUMP_INVESTIGATION.md` Task 5).
    A fixed absolute `q_kh` has the same scale-awareness blind spot this
    session already found and documented for `lambda_tol_km` in
    `wave_identity.py`'s duplicate-detection gate: `k_h = 2*pi/lambda`, so a
    fixed absolute uncertainty in `k_h` is a huge relative uncertainty for a
    long-wavelength (small k_h) wave and a tiny one for a short-wavelength
    (large k_h) wave -- the exact same blind spot named in
    `flag_kf_wave_collisions_by_correlation`'s own docstring for a
    *different* check.

    Instead of a fixed constant, `q_kh` is derived per-call from the wave's
    own `k_h_init` and `wavelength_drift_frac_per_hr` (default 3%/hr, see
    empirical derivation below). Since `dk_h/k_h ~= -dlambda/lambda` for
    small fractional perturbations, this fractional rate applies equally to
    `k_h`:

        frac_per_step = wavelength_drift_frac_per_hr * dt / 3600
        q_kh  = (frac_per_step * k_h_init) ** 2   # (rad/m)**2 / step
        q_dkh = q_kh / dt ** 2                    # (rad/m/s)**2 / step

    The `q_dkh = q_kh / dt**2` relationship is stated explicitly as the
    modeling convention this module uses (rate process noise scales as
    position process noise over `dt` squared) -- checked against the
    existing `q_theta`/`q_dtheta` pair (1e-8, 1e-12 at dt=120s) and found
    empirically consistent (`1e-8 / 120**2 ~= 6.9e-13`, same order of
    magnitude as `1e-12`), suggesting this was the (undocumented) rule of
    thumb already implicitly in use -- now applied deliberately and stated,
    rather than left as an unexplained ratio. This basis is unaffected by
    the `wavelength_drift_frac_per_hr` value below -- whatever `q_kh` comes
    out to, `q_dkh` follows it via this same, separately-justified
    convention.

    `wavelength_drift_frac_per_hr` (default 0.03, i.e. **3%/hr**) --
    **empirically estimated, not physically derived, exactly like
    `KF_Q_THETA`'s own ~10 deg/hr number was** (`empirical_wavelength_drift_task.md`;
    `KF_Q_THETA`'s derivation was itself empirical, not theoretical, so this
    number is grounded the same honest way, not dressed up as more
    principled than it is). Measured directly from the second pass's RAW
    (unsmoothed) `(kx, ky)` observations -- never `kf_lambda_km`, which is
    already smoothed by the very causal filter this parameter feeds, and
    would make the estimate circular.

    **Episodes used, and why each was trusted as genuinely wave-not-
    artifact** (per-episode fractional drift measured as both a ln(lambda)
    vs. time least-squares slope over the whole episode, and a simple
    endpoint-to-endpoint rate -- reported together since raw single-frame
    k_h observations are noisy enough that the two can disagree
    substantially, itself informative about how much of any one number is
    real signal vs. frame noise):

    - **DOY173 Family 13 (wave_id 12), frames 50-142 of 142** (the
      legitimately-owned portion only, per its own `_owned_mask` --
      `FAMILY13_INVESTIGATION.md` established this family is frozen and
      unowned for the night's first ~90 minutes, so only the owned tail is
      real signal). Direction independently confirmed correct
      (`SECOND_PASS_INDEPENDENT_ESTIMATION.md` Task 2 sanity check). 86
      valid frames, 2.83 hr: fit -6.33%/hr, endpoint +0.45%/hr.
    - **DOY150 wave_id 1 (Family 2) and wave_id 10 (Family 11), the
      confirmed-genuine-duplicate `w1v10` episode** (frames 140-183) --
      confirmed via a full episode-wide axis/wavelength/correlation check
      (`DECOUPLE_AND_FIX_SMOOTHING.md` Part 1: axis diff a tight 18.7-19.4
      deg, corr 0.74-0.88, stable across all 44 frames), the most
      rigorously validated pair this session has. Each wave's OWN raw
      trajectory used (not the pair's difference -- the duplicate
      confirmation just establishes both series are real-wave signal, not
      that they should agree with each other). wave_id1: 42 valid frames,
      1.37 hr, fit +1.63%/hr, endpoint +1.13%/hr. wave_id10: 42 frames,
      1.37 hr, fit -1.92%/hr, endpoint +1.24%/hr.
    - **DOY150 wave_id 0 (Family 1) and wave_id 7 (Family 8), the
      confirmed-genuine-duplicate `w0v7` episode** (two contiguous runs,
      frames 35-92 and 139-183) -- confirmed via near-identical
      reconstructed fields at r=0.994 (`FIRSTPASS_GATE_ROLLOUT.md`), though
      without `w1v10`'s full episode-wide numeric check. wave_id7 (the
      steadier of the pair): run1 58 frames/1.90 hr, fit -0.91%/hr,
      endpoint +1.85%/hr; run2 43 frames/1.40 hr, fit -2.82%/hr, endpoint
      +0.65%/hr. wave_id0's run2 is consistent (43 frames, fit -3.76%/hr,
      endpoint -5.09%/hr) but **its run1 is a clear outlier**: fit
      +13.12%/hr, endpoint +9.52%/hr -- roughly 5-10x every other episode.
      Since wave_id0 and wave_id7 are confirmed to be the SAME physical
      wave over this identical window, and wave_id7's own estimate stays
      small and stable while wave_id0's spikes, this reads as raw
      per-frame observation noise in that specific segment (a known,
      expected property of the unsmoothed observable -- the entire reason
      this module's offline smoother exists), not a real 13%/hr physical
      event. Kept in the reported distribution rather than discarded
      (nothing here proves it isn't real), but not allowed to dominate the
      choice below -- see why median, not mean, is used.

      **This pair is also direct, demonstrated evidence of this
      measurement technique's own noise floor, not just general
      small-sample-spread caution**: two waves, CONFIRMED identical (same
      physical structure, r=0.994), measured over the SAME episode window,
      give +13.12%/hr (wave_id0) vs. -0.91%/hr (wave_id7) -- a ~14
      percentage-point spread on a quantity that, if the raw observable
      were noise-free, should read identically for both, since they're the
      same wave. That gap IS the technique's noise floor, measured
      directly rather than inferred. It's the concrete reason the median
      (not the mean, and not any single episode's number) is used below --
      not a hedge against small-sample spread in the abstract, but a
      response to a specific, paired, same-wave measurement showing how
      much of any one episode's number can be pure observation noise.

    **Family 6 (DOY173) deliberately excluded**: independently confirmed to
    have a real, non-physical wavelength jump correlated with a collision
    episode (`FAMILY6_WAVELENGTH_JUMP_INVESTIGATION.md`) -- exactly the
    kind of contamination this measurement needs to avoid, so it does not
    qualify as "confirmed clean" despite its direction being independently
    validated correct.

    **Distribution across the 7 episodes**: fit-based |rate|: 6.33, 1.63,
    1.92, 13.12, 3.76, 0.91, 2.82 (%/hr) -- mean 4.36%, **median 2.82%**.
    Endpoint-based |rate|: 0.45, 1.13, 1.24, 9.52, 5.09, 1.85, 0.65 (%/hr)
    -- mean 2.85%, median 1.24%. Both medians sit roughly an order of
    magnitude below the previously-assumed 15%/hr, and the median (not
    mean) is used deliberately as the summary statistic specifically
    because it's insensitive to the single wave_id0/run1 outlier discussed
    above without requiring a judgment call to discard that data point
    outright.

    **Value used: `wavelength_drift_frac_per_hr = 0.03` (3%/hr)** -- the
    fit-based median (2.82%/hr), rounded to a clean value slightly above it
    (matching `KF_Q_THETA`'s own "~10 deg/hr," a rounded representative
    figure, not a raw unrounded statistic quoted as if it were exact). The
    fit-based (not endpoint-based) median is used as the reference value
    since it draws on every frame of each episode rather than just two
    potentially-noisy endpoints.

    Both `q_kh`/`q_dkh` can still be overridden explicitly (e.g. to
    reproduce the exact ported defaults for a regression test against
    `_kalman_kh_tracker` -- see this module's `__main__`).

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
        `q_kh`/`q_dkh` when not given explicitly. Default 0.03 (3%/hr) --
        empirically estimated (median fit-based rate across 7 confirmed-
        clean reference episodes), not a physically-derived constant; see
        the full derivation and named episodes above.
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
# _kalman_omega_tracker. Added for final_estimation.py (step 5) per
# STEP5_DESIGN_SCOPING.md Task 4: "Speed: derived per-frame from the smoothed
# omega ... propose smoothing the unfolded, sign-corrected omega directly,
# i.e. sign(episode lobe) x |omega_obs(t)|, through the same RTS treatment as
# orientation." kernel_tuning.py (step 3) deliberately does not use this --
# omega/phase-speed is out of scope for kernel construction -- but it belongs
# in this shared engine module regardless, per the module's own stated
# purpose (the one shared offline engine for both steps 3 and 5, not just
# whichever pieces step 3 happened to need first).
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

    Offline counterpart to `_kalman_omega_tracker` (`phase_speed_kalman.py:83`)
    -- identical state-space model (`x = [omega, domega/dt]`, linear/non-
    circular innovation, same Q/R construction), so the forward pass
    reproduces that function's output exactly (see this module's regression
    tests). All defaults (`q_omega=1e-8`, `q_domega=1e-12`,
    `r_sigma_base=0.3`) are unchanged from the ported original -- these
    already have stated rationales there (see that function's own
    docstring), not re-derived here.

    Unlike `smooth_orientation`/`smooth_wavenumber`, this tracker uses
    `_forward_backward`'s `hard_gate=False` -- `_kalman_omega_tracker`'s own
    default is `hard_chi2_gate=False`, which makes its update condition
    (`if innov_valid[t] or not hard_chi2_gate:`) unconditionally True: the
    state always updates whenever `amp_obs[t] > 0` and `omega_obs[t]` is
    finite, regardless of SNR or chi2 outlier status -- those are computed
    only as an `innovation_valid` diagnostic label, never used to block the
    update. This was NOT the assumption this module started with: an
    earlier version reused the theta/kh trackers' always-hard gate for
    omega too and failed its own forward-pass-equivalence regression test
    against real (non-trivial) synthetic noise -- exact equivalence testing
    is what caught this, not code inspection alone. `require_snr_floor=True`
    is still passed (unlike `smooth_wavenumber`'s `False`) so the
    `innovation_valid` label itself is computed the same way the ported
    original computes it (`snr_ok and chi2_ok`), even though that label no
    longer gates anything when `hard_gate=False`.

    Caller's responsibility, per `STEP5_DESIGN_SCOPING.md` Task 4: pass in
    the SIGN-CORRECTED observation (`sign(episode lobe) * |omega_obs(t)|`),
    not the raw signed or `abs()`-ed value directly -- this function tracks
    whatever series it's given, it does not itself resolve the sign
    ambiguity (that's `final_estimation.py`'s episode-based lobe
    disambiguation, a separate, earlier step).

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
#    _kalman_kh_tracker on identical synthetic input, using identical Q/R.
# 2. Smoothing benefit: on synthetic data with known ground truth, the
#    backward (offline) pass must have equal-or-lower RMS error than the
#    forward pass -- REFACTOR_DESIGN.md's stated Stage 2 acceptance bar
#    ("should never track worse, only equal-or-better").
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

    # ── Test 4b: smoothing benefit with the actual DEFAULT
    #    wavelength_drift_frac_per_hr=0.03 (empirically-derived q_kh/q_dkh,
    #    not the explicit 1e-12/1e-16 values Test 2/3 used to match the
    #    ported original exactly) -- empirical_wavelength_drift_task.md's
    #    explicit re-verification requirement: confirm the smoothing
    #    benefit still holds with the new empirically-grounded value, not
    #    just with the old ported constants. ──
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
