"""
final_estimation.py
--------------------
Produces the pipeline's authoritative final per-wave parameters (azimuth,
wavelength, speed) from the second-pass reconstruction, after lobe
disambiguation. This is the only module whose output reaches the permanent
output schema -- `kernel_tuning.py`'s smoothed series are diagnostic-only
kernel-construction inputs, never persisted.

Pipeline within this module, per wave:

    1. `valid[t]` = `wave_identity.unified_validity_mask(...)`.
    2. Episodes = maximal contiguous runs of `valid[t] == True`, then
       merged across short (<=2 frame) invalid gaps
       (`merge_close_episodes`) -- a brief validity dropout should not
       split one continuous physical presence into separate episodes --
       then split where the propagation sign reverses for a sustained run
       (`split_at_reversals`), so a wave that turns around without a
       validity gap gets one direction per leg.
    3. Per merged episode: lobe (propagation-direction) resolution via
       `_averaged_sign_lobe` -- an amplitude-weighted mean axis (with a
       wrap-aware unwrap to avoid a spurious 0/180° boundary artifact; see
       that function's docstring) plus the sign of the amplitude-weighted
       mean cross-correlation phase step, each frame's step referred to
       the axis (`_aligned_phase_step`), with its significance `sign_z`.
    4. Each episode's own resolved azimuth/sign is its smoothing
       fold-reference when its evidence is significant (`SIGN_Z_DECISIVE`);
       otherwise the wave-level pooled value. Frames are flagged
       `direction_ambiguous` when that deciding `sign_z` is below
       `SIGN_Z_DECISIVE` or the smoothed speed is below
       `AMBIGUOUS_SPEED_MS`.
       `lobe_disagreement`/`lobe_majority_frac` are reported per wave as a
       QC signal of how much its episodes vary, not as a gate on which
       episode's evidence is trusted.
    5. Per episode: offline-smooth orientation (raw per-frame `atan2`,
       folded toward that episode's own resolved azimuth, unwrapped, then
       RTS-smoothed via `smoothing._forward_backward` with
       `circular=False` -- deliberately not `smoothing.smooth_orientation`,
       whose fold-toward-a-single-founding-value behavior is specific to
       `kernel_tuning.py`'s needs), wavelength
       (`smoothing.smooth_wavenumber`, unchanged -- no sign ambiguity, no
       episode-level disambiguation needed), and speed (the sign-corrected
       omega, via `smoothing.smooth_omega`, divided by the smoothed `k_h`).

Known limitation, not addressed here: a genuine physical reversal can span
a temporal gap between two separately-clustered wave/family identities
(matching axis, continuous extrapolated rotation, adjacent-but-
non-overlapping ownership windows). This module only ever sees one wave's
own episodes and has no mechanism to connect two different wave identities
as one continuous physical wave -- that would require revisiting family
identity itself (`wave_identity.py`), a separately-scoped question.

Output is per-frame (one row per wave per frame, valid or not); an hourly
aggregate is built downstream from the same per-frame trajectory
(`output_schema.py`). A genuinely reversing wave can produce a real,
accurate discontinuity between adjacent hourly bins -- see
`output_schema.build_wave_hourly_table`'s docstring.
"""

import numpy as np
import pandas as pd

from airglow.angles import full_circle_diff_deg
from airglow.wave_identity import unified_validity_mask
from airglow.smoothing import _forward_backward, smooth_wavenumber, smooth_omega


# ---------------------------------------------------------------------------
# Episode definition (Task 3's first half)
# ---------------------------------------------------------------------------

def find_episodes(valid_mask: np.ndarray, min_episode_frames: int = 1) -> list:
    """
    Maximal contiguous runs of True in `valid_mask`.

    Returns
    -------
    list of (t_start, t_end), inclusive, sorted by t_start. Runs shorter
    than `min_episode_frames` are dropped -- default 1 keeps everything;
    a caller wanting to discard isolated single-frame noise can raise it.
    """
    episodes = []
    n = len(valid_mask)
    t = 0
    while t < n:
        if valid_mask[t]:
            t0 = t
            while t < n and valid_mask[t]:
                t += 1
            t1 = t - 1
            if (t1 - t0 + 1) >= min_episode_frames:
                episodes.append((t0, t1))
        else:
            t += 1
    return episodes


def merge_close_episodes(episodes: list, max_gap: int = 2) -> list:
    """
    Bridge episodes separated by a short invalid gap (<= max_gap frames)
    into one episode, before lobe resolution runs.

    This is a definitional fix, not a substitute for frame-count-weighted
    voting (see `resolve_wave_lobe`): a single transient validity dropout
    (a momentary SNR dip or a collision-check hiccup) splitting one
    continuous physical presence into fragments is a "what counts as one
    episode" problem, not a vote-weighting problem -- down-weighting a
    fragment still treats it as a separate, potentially-disagreeing
    episode, while merging correctly treats it as part of the episode it
    was always physically part of.

    `max_gap=2` is a judgment call (not an empirically-fitted threshold).

    A merged episode's returned span still includes the short invalid gap
    frame(s) in its `(t_start, t_end)` range -- downstream lobe resolution
    and smoothing already skip non-finite/zero-amplitude frames wherever
    they occur (the same handling any interior missing frame gets), so
    this does not require separately tracking a non-contiguous index set.

    Parameters
    ----------
    episodes : list of (t_start, t_end)
        `find_episodes`'s own output, sorted by t_start (guaranteed by
        that function).
    max_gap : int
        Maximum invalid-frame gap (in frames) to bridge.

    Returns
    -------
    list of (t_start, t_end), merged, still sorted by t_start.
    """
    if not episodes:
        return []
    merged = [episodes[0]]
    for (t0, t1) in episodes[1:]:
        prev_t0, prev_t1 = merged[-1]
        gap = t0 - prev_t1 - 1
        if gap <= max_gap:
            merged[-1] = (prev_t0, t1)
        else:
            merged.append((t0, t1))
    return merged


# ---------------------------------------------------------------------------
# Lobe disambiguation (Task 3's second half)
# ---------------------------------------------------------------------------

def _aligned_phase_step(kx, ky, complex_amp, ref_deg):
    """
    Per-frame cross-correlation phase step, signed relative to `ref_deg`:
    positive when the frame says the wave travels toward `ref_deg`.

    A frame's phase step is relative to its own phase-gradient direction
    (lobe = atan2(-kx, -ky)), not to any fixed axis: a positive step means
    travel toward lobe + 180. The second-pass kernel's lobe can flip by 180
    deg between frames (its orientation is an axis, tuned per frame), most
    often when the axis is near 0/180, so steps from different frames are
    comparable only after referring each to a common direction. `ref_deg`
    may be a scalar or per-frame array.
    """
    lobe = np.arctan2(-kx, -ky)
    toward = np.cos(lobe - np.deg2rad(ref_deg)) < 0.0      # lobe ~ ref + 180
    return np.where(toward, 1.0, -1.0) * np.angle(complex_amp)


def _averaged_sign_lobe(kx, ky, complex_amp, idx) -> dict:
    """
    Averaged-sign lobe resolution over an arbitrary set of frame indices
    `idx` (an episode's own contiguous range, or the pooled union of all of
    a wave's episodes).

    Averaging a raw mod-180 axis directly can manufacture a spurious ~180°
    jump for frames whose true axis sits near the 0/180° boundary -- mod-180
    reduction is still necessary (a spatial gradient can't distinguish a
    wave from its 180°-opposite), but naively averaging the reduced values
    treats a wrap-boundary artifact as a real change. The fix: double the
    mod-180 axis (mapping its 180°-periodic wrap onto a full 360° period),
    `np.unwrap` that doubled series over the frames in time order (removing
    only near-full-period jumps, i.e. the wrap artifact, while leaving a
    genuine gradual change in axis alone), then halve back before
    averaging. The propagation sign is resolved separately, as the
    amplitude-weighted mean of the cross-correlation phase step, each
    frame's step referred to the unwrapped axis (`_aligned_phase_step`);
    `sign_z` is that mean over its standard error.

    Returns None if fewer than 1 usable frame.
    """
    kx_s, ky_s, A_s = kx[idx], ky[idx], complex_amp[idx]
    idx_s = np.asarray(idx)
    amp_s = np.abs(A_s)
    m = np.isfinite(kx_s) & np.isfinite(ky_s) & (amp_s > 0)
    if m.sum() < 1:
        return None

    # Time-ordered, valid-only view -- unwrap needs frames in their natural
    # sequence to tell a genuine axis drift from a boundary-noise artifact.
    order = np.argsort(idx_s[m])
    kx_m, ky_m, A_m = kx_s[m][order], ky_s[m][order], A_s[m][order]
    amp_w = amp_s[m][order]

    axis_deg_mod180 = np.degrees(np.arctan2(-kx_m, -ky_m)) % 180.0

    # Doubling maps the mod-180 period to a full mod-360 (2*pi in radians)
    # period so np.unwrap's default pi discontinuity threshold correctly
    # detects and removes spurious near-180-degree (post-doubling,
    # near-360-degree) jumps between consecutive frames -- see docstring.
    doubled_rad_unwrapped = np.unwrap(np.deg2rad(axis_deg_mod180) * 2.0)
    axis_deg_unwrapped = np.degrees(doubled_rad_unwrapped) / 2.0

    mean_axis_unwrapped = np.average(axis_deg_unwrapped, weights=amp_w)
    mean_axis = mean_axis_unwrapped % 180.0   # canonical [0,180) label, for reporting only

    # Phase steps referred to each frame's position on the unwrapped axis
    # (positive = travel toward that axis value, negative = toward +180).
    step = _aligned_phase_step(kx_m, ky_m, A_m, axis_deg_unwrapped)
    overall_sign_val = np.average(step, weights=amp_w)
    overall_sign = 1 if overall_sign_val >= 0 else -1
    agree = (np.sign(step) == overall_sign)
    sign_frac = float(np.sum(amp_w[agree]) / np.sum(amp_w))
    # Significance of the mean step (amplitude-weighted mean / its standard
    # error): per-frame steps are noisy (std ~1 rad at low SNR) while the
    # true step is omega*dt (0.25 rad for a 50-min period at 2-min cadence),
    # so many frames can carry a clear mean with sign_frac barely above 0.5.
    var = np.average((step - overall_sign_val) ** 2, weights=amp_w)
    se = np.sqrt(var * np.sum(amp_w ** 2)) / np.sum(amp_w)
    sign_z = (float(abs(overall_sign_val) / se) if se > 0
              else (np.inf if overall_sign_val != 0 else 0.0))

    # Sign convention: positive overall sign -> axis as-is; negative ->
    # axis + 180. Combined using the UNWRAPPED mean (mean_axis_unwrapped),
    # not the canonical [0,180) one (mean_axis) -- re-applying %180 first
    # would reintroduce the exact ambiguity the unwrap fix above removes,
    # right before the +180 adjustment needs a stable value to work from.
    azimuth_deg = (mean_axis_unwrapped if overall_sign > 0 else mean_axis_unwrapped + 180.0) % 360.0

    return dict(axis_deg=mean_axis, azimuth_deg=azimuth_deg, sign=overall_sign,
                sign_frac=sign_frac, sign_z=sign_z, n_frames=int(m.sum()))


MINORITY_DISAGREEMENT_FRAC = 0.30
"""
Governs `resolve_wave_lobe`'s `disagreement` flag: True when the minority
sign's share of a wave's total valid frames (across all its episodes)
exceeds this fraction. Empirically positioned at a low-density point in
the observed distribution of per-wave minority fractions, separating
confident near-unanimous waves from genuinely near-even splits.

Purely informational -- does not gate which episode's own resolved
azimuth/sign is used in the final trajectory (see `SIGN_Z_DECISIVE` for
the condition that actually governs that). `disagreement`/`majority_frac` are reported as a QC signal of how
much a wave's episodes vary.
"""

SIGN_Z_DECISIVE = 3.0
"""
Sign evidence bar: a set of frames (one episode, or a wave's pooled
episodes) decides the propagation sign only when its mean aligned phase
step is at least this many standard errors from zero (`sign_z`). Below it,
an episode takes the pooled wave's sign. A genuine reversal (e.g. DOY144
Family 7, episode sign_frac 1.0 over 12 frames) clears 3 easily, while a
1-3 frame fragment's own sign does not decide (the case the former
sign_frac/min-frames rule covered).

The Stage-1 omega-k direction is deliberately not a fallback: it was never
flipped in the single-wave Monte Carlo, but on real nights it was opposite
to the motion measured directly from the second-pass wave movie for whole
episodes (40-65 frames on 3 of 5 nights; 291 of 4968 frames reversed vs 95
for this rule, 2026-10-21) -- the same lesson as the August decision not
to override the reconstruction's evidence with another estimator.
"""

MIN_REVERSAL_FRAMES = 5
"""
Shortest run of frames on either side of a mid-episode reversal for
`split_at_reversals` to split there (10 min at 2-min cadence).
"""

AMBIGUOUS_SPEED_MS = 20.0
"""
Below this |final_speed_ms| a frame's direction is flagged ambiguous
(`direction_ambiguous`): the speed and the propagation sign come from the
same phase step, so as the speed tends to zero the sign is set by noise.
On DOY144 2022 / DOY173 2023 the reported direction opposed the motion in
the reconstruction movie for 56% of frames below 10 m/s, 32% at 10-20 m/s,
20% at 20-40 and 4-5% above 40 (2026-10-22).
"""


def _signed_z_cumulative(step, w):
    """Weighted mean and sign_z (as in `_averaged_sign_lobe`) of step[:s] for
    every s = 1..n; returns (mean[s-1], z[s-1])."""
    S0, S1, S2, Q = (np.cumsum(w), np.cumsum(w * step), np.cumsum(w * step ** 2),
                     np.cumsum(w ** 2))
    mean = S1 / S0
    var = np.maximum(S2 / S0 - mean ** 2, 0.0)
    se = np.sqrt(var * Q) / S0
    with np.errstate(divide="ignore", invalid="ignore"):
        z = np.where(se > 0, np.abs(mean) / se, np.where(mean != 0, np.inf, 0.0))
    return mean, z


def split_at_reversals(kx, ky, complex_amp, episodes: list,
                       min_frames: int = MIN_REVERSAL_FRAMES,
                       z_min: float = SIGN_Z_DECISIVE) -> list:
    """
    Split episodes where the propagation sign reverses for a sustained run.

    Episodes come from validity alone, so a wave that turns around without
    a validity gap stays one episode and gets one direction: the frames on
    the minority side of the turn are reported backwards (DOY144 2022
    Family 2, frames 91-149: ~315 deg then ~132 deg, reported 316 deg
    throughout). Within each episode, each frame's phase step is referred
    to the episode's unwrapped axis (as in `_averaged_sign_lobe`), and the
    split point is the one maximising the smaller of the two sides'
    `sign_z`. The split is kept when both sides have >= `min_frames`
    usable frames, opposite mean signs and `sign_z` >= `z_min`; each side
    is then searched again (binary segmentation). Returns the new
    (t_start, t_end) list, time-ordered.
    """
    out = []
    stack = list(episodes)[::-1]
    while stack:
        t0, t1 = stack.pop()
        idx = np.arange(t0, t1 + 1)
        amp = np.abs(complex_amp[idx])
        m = np.isfinite(kx[idx]) & np.isfinite(ky[idx]) & (amp > 0)
        n = int(m.sum())
        if n < 2 * min_frames:
            out.append((t0, t1))
            continue
        t_m, w = idx[m], amp[m]
        axis = np.degrees(np.unwrap(np.deg2rad(np.degrees(np.arctan2(-kx[t_m], -ky[t_m])) % 180.0) * 2.0)) / 2.0
        step = _aligned_phase_step(kx[t_m], ky[t_m], complex_amp[t_m], axis)
        mean_l, z_l = _signed_z_cumulative(step, w)
        mean_r, z_r = _signed_z_cumulative(step[::-1], w[::-1])
        mean_r, z_r = mean_r[::-1], z_r[::-1]        # [s] = stats of step[s:]
        s = np.arange(min_frames, n - min_frames + 1)  # left = step[:s], right = step[s:]
        score = np.minimum(z_l[s - 1], z_r[s])
        score[np.sign(mean_l[s - 1]) == np.sign(mean_r[s])] = -np.inf
        best = int(np.argmax(score))
        if score[best] < z_min:
            out.append((t0, t1))
            continue
        t_split = int(t_m[s[best]])                   # first frame of the right side
        stack.extend([(t_split, t1), (t0, t_split - 1)])
    return out


def resolve_wave_lobe(kx, ky, complex_amp, episodes: list,
                       minority_disagreement_frac: float = MINORITY_DISAGREEMENT_FRAC) -> dict:
    """
    Resolve one wave's propagation lobe across all its episodes, via a
    frame-count-weighted majority vote across episodes (not a flat
    per-episode unanimous check, which would let a handful of tiny,
    low-confidence episodes carry the same weight as one long, confident
    one).

    `wave_azimuth_deg`/`wave_sign_frac`/`wave_sign_z` (the pooled result)
    are always computed and returned, even when `disagreement` is True --
    this function does not decide whether the pooled value is trustworthy
    for a given caller's purpose; see `SIGN_Z_DECISIVE` for the condition
    `estimate_wave_final` uses to decide, per episode, between its own
    resolution, this pooled value and the Stage-1 direction. Each episode also gets its own independently-resolved
    azimuth/sign (`ep_results`) regardless of the pooled outcome.

    Returns
    -------
    dict with:
        episodes       : list of per-episode dicts (t_start, t_end, plus
                         `_averaged_sign_lobe`'s own keys), one per input
                         episode with >=1 usable frame.
        majority_sign  : int, +1 or -1 -- the frame-weighted majority of
                         episode signs (QC only).
        pooled_sign    : int -- the sign of the pooled (all-episodes) mean
                         aligned step, the sign of `wave_azimuth_deg`.
        majority_frac  : float -- majority sign's share of total valid
                         frames across all episodes ([0.5, 1.0]).
        disagreement   : bool -- True when the minority sign's share
                         exceeds `minority_disagreement_frac`. Informational
                         only -- see module docstring.
        wave_azimuth_deg : float -- the pooled, majority-consistent
                         life-of-the-wave azimuth, computed unconditionally.
                         NaN only when no usable frames exist anywhere.
        wave_sign_frac, wave_sign_z : float -- `_averaged_sign_lobe`'s
                         sign_frac / sign_z for the pooled (all-episodes)
                         frame set, same NaN condition as `wave_azimuth_deg`.
    """
    ep_results = []
    for (t0, t1) in episodes:
        idx = np.arange(t0, t1 + 1)
        r = _averaged_sign_lobe(kx, ky, complex_amp, idx)
        if r is not None:
            r["t_start"] = t0
            r["t_end"] = t1
            ep_results.append(r)

    if not ep_results:
        return dict(episodes=[], majority_sign=None, majority_frac=np.nan,
                     disagreement=False, wave_azimuth_deg=np.nan, wave_sign_frac=np.nan,
                     wave_sign_z=np.nan)

    frames_by_sign = {1: 0, -1: 0}
    for r in ep_results:
        frames_by_sign[r["sign"]] += r["n_frames"]
    total_frames = frames_by_sign[1] + frames_by_sign[-1]
    majority_sign = 1 if frames_by_sign[1] >= frames_by_sign[-1] else -1
    majority_frac = frames_by_sign[majority_sign] / total_frames
    minority_frac = 1.0 - majority_frac
    disagreement = minority_frac > minority_disagreement_frac

    # Computed unconditionally -- used by estimate_wave_final as the
    # fallback for an episode whose own sign evidence isn't decisive, not
    # gated on `disagreement` (see docstring).
    all_idx = np.concatenate([np.arange(r["t_start"], r["t_end"] + 1) for r in ep_results])
    pooled = _averaged_sign_lobe(kx, ky, complex_amp, all_idx)
    # The pooled sign is the amplitude-weighted mean step over all frames;
    # it can differ from the frame-count majority of episode signs.
    wave_azimuth_deg = pooled["azimuth_deg"]
    wave_sign_frac = pooled["sign_frac"]

    return dict(episodes=ep_results, majority_sign=majority_sign, majority_frac=majority_frac,
                disagreement=disagreement, wave_azimuth_deg=wave_azimuth_deg,
                wave_sign_frac=wave_sign_frac, wave_sign_z=pooled["sign_z"],
                pooled_sign=pooled["sign"])


# ---------------------------------------------------------------------------
# Per-episode offline smoothing (Task 4)
# ---------------------------------------------------------------------------

def _smooth_episode(
    kx, ky, complex_amp, dt, t0, t1, episode_azimuth_deg, episode_sign,
    *, lambda_h_min_km=5.0, lambda_h_max_km=150.0,
    wavelength_drift_frac_per_hr=0.03,
    q_theta=1e-8, q_dtheta=1e-12, r_theta_base_rad=np.deg2rad(15.0),
    q_omega=1e-8, q_domega=1e-12, r_sigma_omega_base=0.3,
    snr_min_amplitude=0.05, chi2_gate=9.0,
) -> dict:
    """
    Offline-smooth orientation, wavelength, and speed for ONE episode,
    using its own already-resolved lobe -- never smoothing across an
    episode boundary (a fresh gap is real information, not noise to
    interpolate through, per the same "NaN frames produce zero
    reconstruction, not interpolation" principle already established
    elsewhere in this codebase).

    Returns per-frame arrays covering exactly [t0, t1] (length t1-t0+1).
    """
    n_ep = t1 - t0 + 1
    seg = slice(t0, t1 + 1)
    kx_s, ky_s = kx[seg], ky[seg]
    A_s = complex_amp[seg]
    amp_s = np.abs(A_s)

    # --- Orientation: raw per-frame arctan2, folded toward the EPISODE's
    #     own resolved azimuth (not a founding value), then unwrapped. ---
    theta_raw = np.full(n_ep, np.nan)
    valid_kxky = np.isfinite(kx_s) & np.isfinite(ky_s) & (amp_s > 0)
    theta_raw[valid_kxky] = np.arctan2(-kx_s[valid_kxky], -ky_s[valid_kxky])

    ref_rad = np.deg2rad(episode_azimuth_deg)
    for i in range(n_ep):
        if not np.isfinite(theta_raw[i]):
            continue
        z = theta_raw[i]
        z_alt = (z + np.pi) % (2.0 * np.pi)
        d_z = abs((z - ref_rad + np.pi) % (2.0 * np.pi) - np.pi)
        d_alt = abs((z_alt - ref_rad + np.pi) % (2.0 * np.pi) - np.pi)
        theta_raw[i] = z_alt if d_alt < d_z else z

    # np.unwrap requires finite input; unwrap only the finite run, leaving
    # NaN gaps (within-episode NaNs shouldn't occur given unified_validity_
    # mask already restricted to amp>0 frames feeding the episode, but
    # guarded regardless).
    theta_for_unwrap = theta_raw.copy()
    finite = np.isfinite(theta_for_unwrap)
    if np.any(finite):
        theta_for_unwrap[finite] = np.unwrap(theta_for_unwrap[finite])

    theta_init = theta_for_unwrap[finite][0] if np.any(finite) else ref_rad
    Q_theta = np.diag([q_theta, q_dtheta])
    x0_theta = np.array([theta_init, 0.0])
    P0_theta = np.diag([np.deg2rad(10.0) ** 2, (np.deg2rad(1.0) / dt) ** 2])
    theta_result = _forward_backward(
        theta_for_unwrap, amp_s, dt, x0_theta, P0_theta, Q_theta, r_theta_base_rad,
        snr_min_amplitude=snr_min_amplitude, chi2_gate=chi2_gate,
        circular=False, ext_valid=None, require_snr_floor=True, hard_gate=True,
    )
    azimuth_smooth_deg = np.degrees(theta_result["x_smooth"][:, 0]) % 360.0
    azimuth_smooth_std_deg = np.degrees(np.sqrt(np.maximum(theta_result["P_smooth"][:, 0, 0], 0.0)))

    # --- Wavelength: smooth_wavenumber, reused directly, unchanged. ---
    kh_obs = np.sqrt(kx_s ** 2 + ky_s ** 2)
    kh_obs = np.where((kh_obs > 1e-6) & np.isfinite(kh_obs), kh_obs, np.nan)
    k_h_min = 2.0 * np.pi / (lambda_h_max_km * 1000.0)
    k_h_max = 2.0 * np.pi / (lambda_h_min_km * 1000.0)
    finite_kh = np.isfinite(kh_obs)
    k_h_init = float(np.clip(kh_obs[finite_kh][0], k_h_min, k_h_max)) if np.any(finite_kh) else k_h_min

    kh_result = smooth_wavenumber(
        kh_obs, amp_s, dt, k_h_init,
        wavelength_drift_frac_per_hr=wavelength_drift_frac_per_hr,
        k_h_min=k_h_min, k_h_max=k_h_max,
    )
    wavelength_smooth_km = (2.0 * np.pi / kh_result["kh_smooth"]) / 1000.0
    kh_smooth_std = kh_result["kh_smooth_std"]
    # d(lambda)/d(kh) = -2*pi/kh**2 -> std propagation
    wavelength_smooth_std_km = np.abs(2.0 * np.pi / kh_result["kh_smooth"] ** 2 * kh_smooth_std) / 1000.0

    # --- Speed: sign-corrected omega through smooth_omega, / smoothed k_h. ---
    # Each frame's phase step referred to the episode's direction (positive
    # = travel toward it), times episode_sign for smooth_omega's convention.
    # Signed, not abs(): folding noisy steps of the wrong sign back positive
    # biased speed high wherever omega*dt is small against the phase noise
    # (long periods, low SNR).
    omega_obs_raw = np.full(n_ep, np.nan)
    nz = (amp_s > 0) & valid_kxky
    omega_obs_raw[nz] = episode_sign * _aligned_phase_step(
        kx_s[nz], ky_s[nz], A_s[nz], episode_azimuth_deg) / dt

    omega_init = episode_sign * (2.0 * np.pi * 0.0) if not np.any(nz) else float(omega_obs_raw[nz][0])
    omega_result = smooth_omega(
        omega_obs_raw, amp_s, dt, omega_init=omega_init,
        q_omega=q_omega, q_domega=q_domega, r_sigma_base=r_sigma_omega_base,
        snr_min_amplitude=snr_min_amplitude, chi2_gate=chi2_gate,
    )
    speed_smooth_ms = np.abs(omega_result["omega_smooth"]) / kh_result["kh_smooth"]
    # First-order independent-error propagation: relative variance sums.
    omega_rel_std = np.where(
        np.abs(omega_result["omega_smooth"]) > 1e-12,
        omega_result["omega_smooth_std"] / np.abs(omega_result["omega_smooth"]),
        np.nan,
    )
    kh_rel_std = np.where(
        kh_result["kh_smooth"] > 1e-12,
        kh_smooth_std / kh_result["kh_smooth"],
        np.nan,
    )
    speed_smooth_std_ms = speed_smooth_ms * np.sqrt(
        np.nan_to_num(omega_rel_std, nan=0.0) ** 2 + np.nan_to_num(kh_rel_std, nan=0.0) ** 2
    )

    return dict(
        azimuth_deg=azimuth_smooth_deg, azimuth_std_deg=azimuth_smooth_std_deg,
        wavelength_km=wavelength_smooth_km, wavelength_std_km=wavelength_smooth_std_km,
        speed_ms=speed_smooth_ms, speed_std_ms=speed_smooth_std_ms,
    )


# ---------------------------------------------------------------------------
# Per-wave orchestration
# ---------------------------------------------------------------------------

def estimate_wave_final(
    wave: dict, kx, ky, complex_amp, time_vals, dt,
    owned_mask, collision_primary_full, *,
    snr_min_amplitude=0.05, min_episode_frames=1,
    **smoothing_kwargs,
) -> pd.DataFrame:
    """
    Full step-5 pipeline for one wave: validity -> episodes -> lobe
    disambiguation -> per-episode offline smoothing -> one per-frame table.

    Parameters
    ----------
    wave : dict
        Must have 'label' (for diagnostics/logging).
    kx, ky : ndarray (n_time,)
        Second-pass raw amplitude-weighted spatial phase gradient.
    complex_amp : ndarray (n_time,) complex
        Second-pass cross-correlation phasor.
    time_vals : ndarray datetime64 (n_time,)
    dt : float
        Timestep, seconds.
    owned_mask : ndarray bool (n_time,)
        Stage B's per-frame ownership answer.
    collision_primary_full : ndarray bool (n_time,)
        Stage C's per-frame collision outcome, aligned to the SAME full
        night grid as everything else -- alignment is the caller's
        responsibility (`kf_results`, if used as a source, is on a
        different, edge-trimmed grid and needs a timestamp-based
        realignment first).
    snr_min_amplitude : float
        Passed to `unified_validity_mask` and the smoothing calls.
    min_episode_frames : int
        Passed to `find_episodes`.

    Returns
    -------
    pd.DataFrame, one row per frame (n_time rows), columns:
        time, frame_idx, episode_id, valid,
        final_azimuth_deg, final_wavelength_km, final_speed_ms,
        final_azimuth_std_deg, final_wavelength_std_km, final_speed_std_ms,
        lobe_sign_frac, lobe_disagreement, lobe_majority_frac,
        direction_sign_z (the sign_z that decided the frame's episode
        direction: its own, or the pooled wave's), direction_ambiguous
    """
    n_time = len(time_vals)
    amp = np.abs(complex_amp)
    valid = unified_validity_mask(owned_mask, collision_primary_full, amp,
                                   snr_min_amplitude=snr_min_amplitude)
    raw_episodes = find_episodes(valid, min_episode_frames=min_episode_frames)
    episodes = split_at_reversals(kx, ky, complex_amp, merge_close_episodes(raw_episodes, max_gap=2))

    out = pd.DataFrame({
        "time": time_vals, "frame_idx": np.arange(n_time),
        "episode_id": -1, "valid": False,
        "final_azimuth_deg": np.nan, "final_wavelength_km": np.nan, "final_speed_ms": np.nan,
        "final_azimuth_std_deg": np.nan, "final_wavelength_std_km": np.nan, "final_speed_std_ms": np.nan,
        "lobe_sign_frac": np.nan, "lobe_disagreement": False, "lobe_majority_frac": np.nan,
        "direction_sign_z": np.nan, "direction_ambiguous": False,
    })

    if not episodes:
        return out

    lobe = resolve_wave_lobe(kx, ky, complex_amp, episodes)
    out["lobe_disagreement"] = bool(lobe["disagreement"])
    out["lobe_majority_frac"] = lobe["majority_frac"]

    for eid, ep in enumerate(lobe["episodes"]):
        t0, t1 = ep["t_start"], ep["t_end"]
        # Sign: the episode's own evidence when significant (SIGN_Z_DECISIVE),
        # else the pooled wave's. The direction always comes from the
        # second-pass reconstruction's own phase evidence, never from Stage 1
        # (see SIGN_Z_DECISIVE).
        if ep["sign_z"] >= SIGN_Z_DECISIVE:
            ep_azimuth, ep_sign, ep_sign_frac = ep["azimuth_deg"], ep["sign"], ep["sign_frac"]
            ep_sign_z = ep["sign_z"]
        else:
            ep_azimuth = lobe["wave_azimuth_deg"]
            ep_sign_frac = lobe["wave_sign_frac"]
            ep_sign = lobe["pooled_sign"]
            ep_sign_z = lobe["wave_sign_z"]

        sm = _smooth_episode(
            kx, ky, complex_amp, dt, t0, t1, ep_azimuth, ep_sign,
            snr_min_amplitude=snr_min_amplitude, **smoothing_kwargs,
        )

        rows = slice(t0, t1 + 1)
        out.loc[out.index[rows], "episode_id"] = eid
        out.loc[out.index[rows], "valid"] = True
        out.loc[out.index[rows], "final_azimuth_deg"] = sm["azimuth_deg"]
        out.loc[out.index[rows], "final_wavelength_km"] = sm["wavelength_km"]
        out.loc[out.index[rows], "final_speed_ms"] = sm["speed_ms"]
        out.loc[out.index[rows], "final_azimuth_std_deg"] = sm["azimuth_std_deg"]
        out.loc[out.index[rows], "final_wavelength_std_km"] = sm["wavelength_std_km"]
        out.loc[out.index[rows], "final_speed_std_ms"] = sm["speed_std_ms"]
        out.loc[out.index[rows], "lobe_sign_frac"] = ep_sign_frac
        # Ambiguous: the sign that decided this episode is weak, or the wave
        # is too slow for its phase step to carry a sign (AMBIGUOUS_SPEED_MS).
        out.loc[out.index[rows], "direction_sign_z"] = ep_sign_z
        out.loc[out.index[rows], "direction_ambiguous"] = (
            (ep_sign_z < SIGN_Z_DECISIVE) | ~(np.abs(sm["speed_ms"]) >= AMBIGUOUS_SPEED_MS))

    return out
