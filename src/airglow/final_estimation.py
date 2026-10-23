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
       split one continuous physical presence into separate episodes.
    3. Per merged episode: lobe (propagation-direction) resolution via
       `_averaged_sign_lobe` -- an amplitude-weighted mean axis (with a
       wrap-aware unwrap to avoid a spurious 0/180° boundary artifact; see
       that function's docstring) plus an amplitude-weighted mean sign of
       the raw cross-correlation phase.
    4. Each episode's own resolved azimuth/sign is its default smoothing
       fold-reference. An episode falls back to the wave-level pooled
       value only when its OWN evidence isn't decisive on its own terms --
       see `SIGN_FRAC_DECISIVE_THRESHOLD`/`SIGN_FRAC_DECISIVE_MIN_FRAMES`.
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
    amplitude-weighted mean sign of the raw (not `abs()`-ed) cross-
    correlation phase.

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

    angle_signed = np.angle(A_m)
    overall_sign_val = np.average(angle_signed, weights=amp_w)
    overall_sign = 1 if overall_sign_val >= 0 else -1
    agree = (np.sign(angle_signed) == overall_sign)
    sign_frac = float(np.sum(amp_w[agree]) / np.sum(amp_w))

    # Sign convention: positive overall sign -> axis as-is; negative ->
    # axis + 180. Combined using the UNWRAPPED mean (mean_axis_unwrapped),
    # not the canonical [0,180) one (mean_axis) -- re-applying %180 first
    # would reintroduce the exact ambiguity the unwrap fix above removes,
    # right before the +180 adjustment needs a stable value to work from.
    azimuth_deg = (mean_axis_unwrapped if overall_sign > 0 else mean_axis_unwrapped + 180.0) % 360.0

    return dict(axis_deg=mean_axis, azimuth_deg=azimuth_deg, sign=overall_sign,
                sign_frac=sign_frac, n_frames=int(m.sum()))


MINORITY_DISAGREEMENT_FRAC = 0.30
"""
Governs `resolve_wave_lobe`'s `disagreement` flag: True when the minority
sign's share of a wave's total valid frames (across all its episodes)
exceeds this fraction. Empirically positioned at a low-density point in
the observed distribution of per-wave minority fractions, separating
confident near-unanimous waves from genuinely near-even splits.

Purely informational -- does not gate which episode's own resolved
azimuth/sign is used in the final trajectory (see
`SIGN_FRAC_DECISIVE_THRESHOLD` for the condition that actually governs
that). `disagreement`/`majority_frac` are reported as a QC signal of how
much a wave's episodes vary.
"""

SIGN_FRAC_DECISIVE_THRESHOLD = 1.0 - MINORITY_DISAGREEMENT_FRAC
"""
Per-episode confidence bar: an episode's OWN `sign_frac` (from
`_averaged_sign_lobe` -- amplitude-weighted agreement with its own
resolved sign) must clear this to stand alone as its own smoothing
fold-reference in `estimate_wave_final`. Below it, the episode falls back
to the pooled wave-level value instead -- not because it disagrees with
the wave's majority (that alone is not disqualifying), but because its
own evidence isn't decisive enough to trust independent of what any other
episode says.

Reuses `1 - MINORITY_DISAGREEMENT_FRAC` (0.70) rather than an independent
constant, since both thresholds answer "is this evidence decisive,"
just measured on different populations (frame-count share of the whole
wave vs. amplitude-weighted sign agreement within one episode).

`sign_frac` alone is not sufficient to decide this reliably: it's a
fragile statistic on a short episode (a 3-4 frame episode can clear a
0.70 bar by chance alone) -- see `SIGN_FRAC_DECISIVE_MIN_FRAMES`, which
this threshold requires alongside a minimum frame count.
"""

SIGN_FRAC_DECISIVE_MIN_FRAMES = 5
"""
Companion to `SIGN_FRAC_DECISIVE_THRESHOLD`: an episode must ALSO have at
least this many frames to stand alone. `sign_frac` computed on very few
frames is too easily pushed above a fixed threshold by chance to trust on
its own -- a 1-frame episode's `sign_frac` is trivially 1.000 (it agrees
with itself by construction) and carries no real statistical weight. 5 is
a judgment call, not a value independently re-derived from a full-batch
sweep the way `MINORITY_DISAGREEMENT_FRAC` was.
"""


def resolve_wave_lobe(kx, ky, complex_amp, episodes: list,
                       minority_disagreement_frac: float = MINORITY_DISAGREEMENT_FRAC) -> dict:
    """
    Resolve one wave's propagation lobe across all its episodes, via a
    frame-count-weighted majority vote across episodes (not a flat
    per-episode unanimous check, which would let a handful of tiny,
    low-confidence episodes carry the same weight as one long, confident
    one).

    `wave_azimuth_deg`/`wave_sign_frac` (the frame-weighted pooled result)
    are always computed and returned, even when `disagreement` is True --
    this function does not decide whether the pooled value is trustworthy
    for a given caller's purpose; see `SIGN_FRAC_DECISIVE_THRESHOLD` for
    the condition `estimate_wave_final` actually uses to decide, per
    episode, whether to use its own resolution or fall back to this pooled
    value. Each episode also gets its own independently-resolved
    azimuth/sign (`ep_results`) regardless of the pooled outcome.

    Returns
    -------
    dict with:
        episodes       : list of per-episode dicts (t_start, t_end, plus
                         `_averaged_sign_lobe`'s own keys), one per input
                         episode with >=1 usable frame.
        majority_sign  : int, +1 or -1 -- the frame-weighted majority.
        majority_frac  : float -- majority sign's share of total valid
                         frames across all episodes ([0.5, 1.0]).
        disagreement   : bool -- True when the minority sign's share
                         exceeds `minority_disagreement_frac`. Informational
                         only -- see module docstring.
        wave_azimuth_deg : float -- the pooled, majority-consistent
                         life-of-the-wave azimuth, computed unconditionally.
                         NaN only when no usable frames exist anywhere.
        wave_sign_frac : float -- `_averaged_sign_lobe`'s own sign_frac
                         for the pooled (all-episodes) frame set, same NaN
                         condition as `wave_azimuth_deg`.
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
                     disagreement=False, wave_azimuth_deg=np.nan, wave_sign_frac=np.nan)

    frames_by_sign = {1: 0, -1: 0}
    for r in ep_results:
        frames_by_sign[r["sign"]] += r["n_frames"]
    total_frames = frames_by_sign[1] + frames_by_sign[-1]
    majority_sign = 1 if frames_by_sign[1] >= frames_by_sign[-1] else -1
    majority_frac = frames_by_sign[majority_sign] / total_frames
    minority_frac = 1.0 - majority_frac
    disagreement = minority_frac > minority_disagreement_frac

    # Computed unconditionally now -- used by estimate_wave_final as the
    # fallback for an episode whose own sign_frac isn't decisive, not
    # gated on `disagreement` (see docstring).
    all_idx = np.concatenate([np.arange(r["t_start"], r["t_end"] + 1) for r in ep_results])
    pooled = _averaged_sign_lobe(kx, ky, complex_amp, all_idx)
    # Pooled resolution's own sign should always match the frame-weighted
    # majority by construction (same underlying frames, same weighting
    # logic) -- not re-derived independently here, used directly.
    wave_azimuth_deg = pooled["azimuth_deg"]
    wave_sign_frac = pooled["sign_frac"]

    return dict(episodes=ep_results, majority_sign=majority_sign, majority_frac=majority_frac,
                disagreement=disagreement, wave_azimuth_deg=wave_azimuth_deg,
                wave_sign_frac=wave_sign_frac)


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
    omega_obs_raw = np.full(n_ep, np.nan)
    nz = amp_s > 0
    omega_obs_raw[nz] = episode_sign * np.abs(np.angle(A_s[nz])) / dt

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
        lobe_sign_frac, lobe_disagreement, lobe_majority_frac
    """
    n_time = len(time_vals)
    amp = np.abs(complex_amp)
    valid = unified_validity_mask(owned_mask, collision_primary_full, amp,
                                   snr_min_amplitude=snr_min_amplitude)
    raw_episodes = find_episodes(valid, min_episode_frames=min_episode_frames)
    episodes = merge_close_episodes(raw_episodes, max_gap=2)

    out = pd.DataFrame({
        "time": time_vals, "frame_idx": np.arange(n_time),
        "episode_id": -1, "valid": False,
        "final_azimuth_deg": np.nan, "final_wavelength_km": np.nan, "final_speed_ms": np.nan,
        "final_azimuth_std_deg": np.nan, "final_wavelength_std_km": np.nan, "final_speed_std_ms": np.nan,
        "lobe_sign_frac": np.nan, "lobe_disagreement": False, "lobe_majority_frac": np.nan,
    })

    if not episodes:
        return out

    lobe = resolve_wave_lobe(kx, ky, complex_amp, episodes)
    out["lobe_disagreement"] = bool(lobe["disagreement"])
    out["lobe_majority_frac"] = lobe["majority_frac"]

    for eid, ep in enumerate(lobe["episodes"]):
        t0, t1 = ep["t_start"], ep["t_end"]
        # Each episode's own independently-resolved azimuth/sign is used by
        # default (not the wave-level pooled value, and not gated on
        # lobe["disagreement"]). Falls back to the pooled wave-level value
        # only when this episode's own evidence isn't both decisive and
        # substantial enough to trust alone -- SIGN_FRAC_DECISIVE_THRESHOLD
        # + SIGN_FRAC_DECISIVE_MIN_FRAMES, both required (see their own
        # docstrings for why a confidence bar alone isn't sufficient).
        if (ep["sign_frac"] >= SIGN_FRAC_DECISIVE_THRESHOLD
                and ep["n_frames"] >= SIGN_FRAC_DECISIVE_MIN_FRAMES):
            ep_azimuth = ep["azimuth_deg"]
            ep_sign_frac = ep["sign_frac"]
            ep_sign = ep["sign"]
        else:
            ep_azimuth = lobe["wave_azimuth_deg"]
            ep_sign_frac = lobe["wave_sign_frac"]
            ep_sign = lobe["majority_sign"]

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

    return out
