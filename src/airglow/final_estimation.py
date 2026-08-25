"""
final_estimation.py
--------------------
Step 5 of the five-step architecture (`refactor_foundation.md`): second-pass
reconstruction -> final estimation. The step that was never actually built
before this refactor -- "the actual deliverable," per `REFACTOR_DESIGN.md`'s
own staging table, and the step whose output is the ONLY thing that reaches
the permanent final schema (`refactor_foundation.md`'s persistence-boundary
demand; `kernel_tuning.py`'s output stays diagnostic-only, never persisted).

Design source: `STEP5_DESIGN_SCOPING.md` Tasks 2-4, built as specified
there for the initial version. Revised once, per `stage4_fix_and_revalidate_task.md`
Task 2: the initial version's episode-agreement logic (flat per-episode
unanimous vote) let a single tiny, low-confidence episode -- as short as 3
frames, split off from a much longer real presence by a 1-frame validity
dropout -- flip a whole wave's disagreement flag, diagnosed concretely
against DOY173 Family13 and three other cases
(`STAGE4_FINAL_ESTIMATION_KICKOFF.md`, `stage4_fix_and_revalidate_task.md`'s
own diagnosis). Replaced with episode merging across short gaps
(`merge_close_episodes`) plus a frame-count-weighted majority vote
(`resolve_wave_lobe`) -- see both functions' own docstrings for the full
reasoning and the diagnosed cases each one resolves.

Pipeline within this module, per wave:

    1. valid[t] = wave_identity.unified_validity_mask(...)   -- Task 2,
       already built there, reused as-is.
    2. episode = maximal contiguous run of valid[t] == True, THEN merged
       across gaps <= 2 frames (`merge_close_episodes`)       -- Task 3,
       revised per the fix above.
    3. Per (merged) episode: averaged-sign lobe resolution (amplitude-
       weighted mean axis via angles.circular_mean_axis_deg + amplitude-
       weighted mean sign of the raw, un-abs()-ed cross-correlation phase)
       -- the exact method validated in SECOND_PASS_INDEPENDENT_ESTIMATION.md
       against Family 6/13 (DOY173) and recalibrated/reconfirmed against
       the same two cases in FULL_BATCH_PREVALENCE_BASELINE.md before this
       module was built.
    4. The wave ALWAYS resolves to the frame-count-weighted MAJORITY sign's
       pooled azimuth (pooling every valid frame across every episode
       together) -- the single best available estimate. `lobe_disagreement`
       is set when the minority sign's frame share exceeds
       `MINORITY_DISAGREEMENT_FRAC` -- a confidence/QC annotation on that
       estimate, not a gate on whether one gets computed (revised from the
       initial version's NaN-on-disagreement behavior, which produced a
       misleading circular mean when a caller averaged per-frame values
       spanning genuinely different resolved hemispheres -- see
       `resolve_wave_lobe`'s own docstring).
    5. Per episode, using the wave-level resolved lobe: offline-smooth
       orientation (raw per-frame arctan2, folded toward the WAVE-level
       resolved azimuth -- not any founding value -- then np.unwrap()'d,
       then RTS via smoothing.py's _forward_backward with circular=False
       -- the fold-toward-founding behavior smooth_orientation() carries
       for kernel_tuning.py's purposes is deliberately NOT reused here,
       per Task 4's explicit instruction), wavelength (smooth_wavenumber(),
       reused directly and unchanged -- Task 4: "no sign issue, no
       episode-level disambiguation needed here"), and speed (the sign-
       corrected omega -- majority_sign * |angle(A_cross(t))| -- via the
       new smooth_omega(), divided by the smoothed k_h).

Output is per-frame (one row per wave per frame, valid or not) plus an
hourly aggregate built from the same per-frame trajectory -- both from one
underlying computation, per Task 4's own framing.
"""

import numpy as np
import pandas as pd

from airglow.angles import circular_mean_axis_deg, full_circle_diff_deg
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

    Added per `stage4_fix_and_revalidate_task.md` Task 2, alongside the
    frame-weighted vote in `resolve_wave_lobe` below -- diagnosed as
    genuinely necessary, not redundant with weighting: a single transient
    1-frame validity dropout (a momentary SNR dip or a collision-check
    hiccup) splitting one continuous 100+-frame physical presence into
    fragments looks like a definitional problem with what counts as "one
    episode," not a voting-weight problem -- weighting a 3-frame fragment
    down still treats it as a *separate, disagreeing* episode; merging
    treats it correctly as part of the SAME episode it was always
    physically part of. Confirmed on DOY173 Family13
    (`STAGE4_FINAL_ESTIMATION_KICKOFF.md`'s worked example): its 3
    episodes `(64,107),(109,111),(113,135)` are separated by exactly
    1-frame gaps (108, 112) and merge into one `(64,135)` episode here,
    resolving its spurious disagreement flag by construction, without
    needing the vote-weighting fix at all for this specific case.

    `max_gap=2` matches the task's own suggested "1-2 frames" range --
    not independently re-derived here; a genuine judgment call, not an
    empirically-fitted threshold (this session's own standing lesson from
    `FULL_BATCH_PREVALENCE_BASELINE.md`'s `sign_frac` finding: don't dress
    up a chosen cutoff as more validated than it is).

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
    `idx` (an episode's own contiguous range, or the pooled union of all
    of a wave's episodes when they agree). Method validated in
    SECOND_PASS_INDEPENDENT_ESTIMATION.md / recalibrated in
    FULL_BATCH_PREVALENCE_BASELINE.md against DOY173 Family13/Family6.

    Returns None if fewer than 1 usable frame.
    """
    kx_s, ky_s, A_s = kx[idx], ky[idx], complex_amp[idx]
    amp_s = np.abs(A_s)
    m = np.isfinite(kx_s) & np.isfinite(ky_s) & (amp_s > 0)
    if m.sum() < 1:
        return None

    axis_deg = np.degrees(np.arctan2(-kx_s[m], -ky_s[m])) % 180.0
    mean_axis = circular_mean_axis_deg(axis_deg, weights=amp_s[m])

    angle_signed = np.angle(A_s[m])
    amp_w = amp_s[m]
    overall_sign_val = np.average(angle_signed, weights=amp_w)
    overall_sign = 1 if overall_sign_val >= 0 else -1
    agree = (np.sign(angle_signed) == overall_sign)
    sign_frac = float(np.sum(amp_w[agree]) / np.sum(amp_w))

    # CALIBRATED convention (FULL_BATCH_PREVALENCE_BASELINE.md, confirmed
    # against both DOY173 sanity cases independently): positive overall
    # sign -> axis as-is; negative -> axis + 180.
    azimuth_deg = mean_axis if overall_sign > 0 else (mean_axis + 180.0) % 360.0

    return dict(axis_deg=mean_axis, azimuth_deg=azimuth_deg, sign=overall_sign,
                sign_frac=sign_frac, n_frames=int(m.sum()))


MINORITY_DISAGREEMENT_FRAC = 0.20
"""
Disagreement threshold for `resolve_wave_lobe`: flag when the minority
sign's share of total valid frames exceeds this. A stated judgment call,
not an empirically re-derived cutoff (that would need its own full-batch
analysis, out of scope for `stage4_fix_and_revalidate_task.md`) -- chosen
and reported transparently as such, the same discipline
`FULL_BATCH_PREVALENCE_BASELINE.md` demanded of `sign_frac`'s old 0.7 cutoff.
Checked against the diagnosed cases before adopting: cleanly separates
DOY150 Family4 (5% minority after merging -> resolves, matches its
established ~13 deg direction) from DOY144 Family7 (43%), DOY154 Family5
(34%), and DOY154 Family14 (45%) (all stay flagged -- genuinely close
splits, not fragmentation artifacts; Family5 additionally reproduces
FULL_BATCH_PREVALENCE_BASELINE.md's own flagged=True finding for that wave).
"""


def resolve_wave_lobe(kx, ky, complex_amp, episodes: list,
                       minority_disagreement_frac: float = MINORITY_DISAGREEMENT_FRAC) -> dict:
    """
    Resolve one wave's propagation lobe across all its episodes, via a
    frame-count-weighted majority vote -- NOT a flat per-episode unanimous
    check (`stage4_fix_and_revalidate_task.md` Task 2: the original flat
    vote let a handful of tiny, low-confidence episodes carry the same
    weight as one long, confident episode, flagging DOY173 Family13 --
    two long flanking episodes both landing within ~9 deg of the correct
    direction -- purely because a 3-frame middle fragment disagreed).

    The wave ALWAYS resolves to the majority sign's pooled azimuth
    (computed by pooling every valid frame across every episode together,
    not by averaging independent per-episode azimuths) -- this is the
    single best available estimate regardless of the disagreement flag's
    state. `disagreement` is a confidence/QC annotation on top of that
    estimate, not a gate on whether one gets computed: per
    `STEP5_DESIGN_SCOPING.md` Task 3's own flagged judgment call, a
    disagreeing wave is not auto-split into synthetic sub-waves, but this
    module's reading of "flag for manual review" is "compute the best
    single answer AND flag it," not "withhold an answer" -- the earlier
    NaN-on-disagreement behavior actively produced a misleading circular
    mean when a caller naively averaged per-frame values spanning
    genuinely different resolved hemispheres across episodes; always
    resolving via the majority avoids reintroducing that failure mode.

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
                         exceeds `minority_disagreement_frac`.
        wave_azimuth_deg : float -- the pooled, majority-consistent
                         life-of-the-wave azimuth. NaN only if no usable
                         frames exist anywhere.
        wave_sign_frac : float -- `_averaged_sign_lobe`'s own sign_frac
                         for the pooled (all-episodes) frame set.
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
        night grid as everything else (caller's responsibility -- see
        this module's verification script for the timestamp-based
        alignment pattern required to build this correctly from
        kf_results, which uses a different, edge-trimmed grid).
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
        # ALWAYS use the wave-level, frame-weighted-majority pooled
        # azimuth/sign as every episode's smoothing fold-reference --
        # the single best available estimate regardless of the
        # disagreement flag (see resolve_wave_lobe's own docstring for
        # why this replaced the old per-episode-own-value fallback).
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
