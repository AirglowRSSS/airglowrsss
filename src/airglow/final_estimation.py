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
there for the initial version, then revised several times:

    - `stage4_fix_and_revalidate_task.md` Task 2: the initial version's
      episode-agreement logic (flat per-episode unanimous vote) let a
      single tiny, low-confidence episode -- as short as 3 frames, split
      off from a much longer real presence by a 1-frame validity dropout
      -- flip a whole wave's disagreement flag, diagnosed concretely
      against DOY173 Family13 and three other cases
      (`STAGE4_FINAL_ESTIMATION_KICKOFF.md`,
      `stage4_fix_and_revalidate_task.md`'s own diagnosis). Replaced with
      episode merging across short gaps (`merge_close_episodes`) plus a
      frame-count-weighted majority vote (`resolve_wave_lobe`) -- this
      part stands, see both functions' own docstrings for the full
      reasoning.
    - `stage4_reconsider_autoresolve_task.md` Task 1: that same round also
      changed a wave to ALWAYS resolve to the majority-pooled azimuth
      regardless of the disagreement flag. Reverted -- `STEP5_DESIGN_SCOPING.md`
      Task 3 and an earlier explicit decision in this thread were both
      clear that a genuinely-disagreeing wave gets flagged, not silently
      resolved to a value presented as equally trustworthy. The actual bug
      that had motivated the auto-resolve change (a validation script
      naively averaging per-frame values across genuinely different
      resolved hemispheres) was fixed in the script instead, where it
      belonged.
    - `doy144_wave7_reversal_investigation.md` /
      `implement_per_episode_defer_crossfamily.md`: the pooled-by-default
      policy the previous point reverted TO turned out to have its own,
      more consequential failure mode -- DOY144 Family7, checked with
      direct per-frame evidence, is a wave whose direction genuinely
      reverses mid-life (two high-confidence episodes independently
      resolving to opposite signs, ~132 deg and ~314 deg, not a noisy
      fragment). Pooling silently forced the whole wave's earlier,
      100%-confident episode to the OPPOSITE of its own resolved
      direction, matching whichever episode had more frames -- exactly
      the failure mode a genuine physical reversal produces, and neither
      prior revision was built to distinguish it from ordinary noise
      fragmentation. Each episode NOW keeps its own independently-resolved
      azimuth/sign by default (see `resolve_wave_lobe`'s own docstring for
      the confidence-based exception), and `lobe_disagreement`/
      `lobe_majority_frac` become informational (how much a wave's
      episodes vary) rather than a gate on whether per-episode truth
      survives. `resolve_wave_lobe`'s pooled wave-level value is retained
      as the fallback for an episode whose OWN evidence isn't decisive
      *and* substantial enough to stand alone (see
      `SIGN_FRAC_DECISIVE_THRESHOLD`/`SIGN_FRAC_DECISIVE_MIN_FRAMES` --
      the first version of this fix used a confidence bar alone, found
      wrong against DOY173 Family13's own 3-frame fragment before being
      committed, see `SIGN_FRAC_DECISIVE_THRESHOLD`'s own docstring) --
      not removed, repurposed.

Pipeline within this module, per wave:

    1. valid[t] = wave_identity.unified_validity_mask(...)   -- Task 2,
       already built there, reused as-is.
    2. episode = maximal contiguous run of valid[t] == True, THEN merged
       across gaps <= 2 frames (`merge_close_episodes`)       -- Task 3,
       revised per the fix above.
    3. Per (merged) episode: averaged-sign lobe resolution (amplitude-
       weighted mean axis, computed via a mod-180-aware unwrap across the
       episode's time-ordered frames before averaging, to remove spurious
       0/180-boundary jumps rather than let them corrupt the mean -- see
       `_averaged_sign_lobe`'s own docstring for why -- + amplitude-
       weighted mean sign of the raw, un-abs()-ed cross-correlation phase)
       -- the exact method validated in SECOND_PASS_INDEPENDENT_ESTIMATION.md
       against Family 6/13 (DOY173) and recalibrated/reconfirmed against
       the same two cases in FULL_BATCH_PREVALENCE_BASELINE.md before this
       module was built.
    4. Each episode's smoothing fold-reference is its OWN resolved
       azimuth/sign by default -- UNLESS that episode's own `sign_frac`
       fails to clear `SIGN_FRAC_DECISIVE_THRESHOLD` OR it has fewer than
       `SIGN_FRAC_DECISIVE_MIN_FRAMES` frames (both required -- a thin
       episode's `sign_frac` is too easily pushed above a fixed bar by
       chance to trust alone), in which case it falls back to the
       frame-count-weighted pooled wave-level value (still computed
       unconditionally by `resolve_wave_lobe`, no longer NaN'd out on
       disagreement). `lobe_disagreement`/`lobe_majority_frac` are still
       computed and reported per frame, now purely as a QC signal of how
       much a wave's episodes vary -- not as the gate deciding whose
       evidence survives into the final trajectory.
    5. Per episode: offline-smooth orientation (raw per-frame arctan2,
       folded toward whichever azimuth Step 4 assigned that episode --
       that episode's own resolution in the normal case, the pooled
       wave-level one only when that episode's own confidence is
       indecisive -- never any founding value -- then np.unwrap()'d, then
       RTS via smoothing.py's _forward_backward with circular=False -- the
       fold-toward-founding behavior smooth_orientation() carries for
       kernel_tuning.py's purposes is deliberately NOT reused here, per
       Task 4's explicit instruction), wavelength (smooth_wavenumber(),
       reused directly and unchanged -- Task 4: "no sign issue, no
       episode-level disambiguation needed here"), and speed (the sign-
       corrected omega -- via the new smooth_omega(), divided by the
       smoothed k_h).

    A real, previously-unaddressed limitation, deliberately NOT tackled
    here (`implement_per_episode_defer_crossfamily.md`): a genuine
    physical reversal can apparently also span a temporal gap between TWO
    separately-clustered family identities (DOY144 Family7/Family8 --
    matching axis, continuous extrapolated rotation, adjacent-but-
    non-overlapping ownership windows, no frame ever owned by both). This
    module only ever sees one wave_id's own episodes; it has no mechanism
    to connect two different families as one continuous physical wave --
    that would require revisiting family identity itself (Stage A/B,
    upstream of this module), a materially larger, separately-scoped
    question. What's implemented above only fixes the WITHIN-one-wave_id
    case.

Output is per-frame (one row per wave per frame, valid or not) plus an
hourly aggregate built from the same per-frame trajectory -- both from one
underlying computation, per Task 4's own framing. An hourly bin can now
show a real, accurate discontinuity for a genuinely-reversing wave (e.g.
DOY144 Family7's ~314 deg early bin, ~132 deg later bins) rather than a
smoothed-over single value -- this is the fix working as intended, not a
data-quality problem, and should not be mistaken for one.
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

    **Wrap-boundary fix** (`stage4_synthetic_sweep_and_movie_status.md`
    Task 1's own synthetic full-circle sweep, az_true=0 case -- caught by
    the sweep this task exists to run, not by any real-data check). Two
    earlier fix attempts were tried and rejected, each with direct evidence,
    not assumption:

    1. "Fold every frame toward the highest-amplitude frame's own axis"
       failed the identical synthetic case, because the reference frame it
       folded toward could ITSELF be on the wrong side of the wrap -- the
       ambiguity just moved, it didn't go away.
    2. "Never take `% 180` before averaging -- average the RAW arctan2
       output directly via a full-360-degree circular mean, then reduce to
       [0,180) once at the end" fixed the synthetic case (44/44 sweep) but
       broke real data catastrophically: re-validating against DOY173
       Family6/Family13 (the two independently-established ground-truth
       cases this whole module was calibrated against) showed the result
       flipped by ~172/170 degrees from the already-verified-correct
       values, and the same near-exact 180-degree flip showed up in 43 of
       46 checked waves across the full named-target validation set. Root
       cause, confirmed by direct inspection rather than assumed: real
       per-frame k-vectors genuinely DO straddle the mod-180 axis
       ambiguity across a episode's frames (that's the whole reason axis
       values need a mod-180 reduction at all -- a spatial gradient can't
       tell a wave from its 180-degree opposite). The synthetic test's
       tight, single-cluster raw theta was an artifact of how the test
       constructed kx/ky (always from one small perturbation of a fixed
       true axis, never flipping) -- not representative of real data,
       where treating raw, un-reduced theta as safe to average directly is
       simply wrong in general.

    **Actual fix**: keep the mod-180 axis reduction (still necessary and
    correct for real per-frame data), but eliminate the ARTIFICIAL 0/180
    boundary jump it manufactures between frames whose true axis sits near
    that boundary -- via `np.unwrap` on the DOUBLED mod-180 axis, over the
    frames in their natural time order. Doubling maps the mod-180 (mod-pi)
    period up to a full mod-360 (2*pi) period, which is what `np.unwrap`'s
    default discontinuity threshold expects; it then walks the time-ordered
    sequence and removes any jump close to a full period, exactly the
    boundary-noise artifact this bug is about, while correctly leaving a
    genuine large change in axis over time alone (real per-frame data can
    still have both). Halving back afterward recovers the axis on its
    true mod-180 scale, free of the spurious jump, safe to average
    directly. Verified against both the synthetic full-circle sweep (44/44,
    including the az_true=0 case this was written for) and reproduces the
    OLD, already-validated values for DOY173 Family6 (208.60 vs 208.70 deg)
    and Family13 (9.15 vs 9.20 deg) to within 0.1 degree -- unlike the
    rejected full-circle-average attempt, which put both ~170 degrees off.

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

    # CALIBRATED convention (FULL_BATCH_PREVALENCE_BASELINE.md, confirmed
    # against both DOY173 sanity cases independently): positive overall
    # sign -> axis as-is; negative -> axis + 180. Combined using the
    # UNWRAPPED mean (mean_axis_unwrapped), not the canonical [0,180) one
    # (mean_axis) -- re-applying %180 first would reintroduce the exact
    # ambiguity this fix removes, right before the +180 adjustment needs
    # a stable value to work from.
    azimuth_deg = (mean_axis_unwrapped if overall_sign > 0 else mean_axis_unwrapped + 180.0) % 360.0

    return dict(axis_deg=mean_axis, azimuth_deg=azimuth_deg, sign=overall_sign,
                sign_frac=sign_frac, n_frames=int(m.sum()))


MINORITY_DISAGREEMENT_FRAC = 0.30
"""
Disagreement threshold for `resolve_wave_lobe`: flag when the minority
sign's share of total valid frames exceeds this.

Empirically positioned, not just a small-sample-fitted guess --
`stage4_reconsider_autoresolve_task.md` Task 2 computed `minority_frac`
for all 227 waves in the full 2022 LOW batch (DOY143-159) and found a real
bimodal distribution: 74.4% of waves cluster below 0.05 minority
(confident, near-unanimous), a second mode (9.7%) sits at 0.35-0.50
(genuinely near-even splits), and 0.20-0.35 is a real low-density valley
between them (10 of 227 waves), with the single sparsest bin at exactly
0.30-0.35 (1 wave) -- a genuine natural separation, unlike `sign_frac`'s
smooth, non-separating distribution at any cutoff from 0.5-0.9
(`FULL_BATCH_PREVALENCE_BASELINE.md`). An initial value of 0.20 (chosen
from 4 diagnosed cases before the full-batch check existed) sat at the
valley's edge rather than its center; moved to 0.30 here, matching the
distribution's actual sparsest point
(`stage4_final_threshold_closeout.md` Task 1) -- affects ~9 of 227 waves
relative to 0.20, all within the low-density valley itself, not a
wholesale reclassification.

Checked against the diagnosed cases before either value was adopted:
cleanly separates DOY150 Family4 (5% minority after merging -> resolves,
matches its established ~13 deg direction) from DOY144 Family7 (43%),
DOY154 Family5 (34%), and DOY154 Family14 (45%) (all stay flagged --
genuinely close splits, not fragmentation artifacts; Family5 additionally
reproduces FULL_BATCH_PREVALENCE_BASELINE.md's own flagged=True finding
for that wave).

No longer gates whether a wave-level answer is asserted (see
`SIGN_FRAC_DECISIVE_THRESHOLD` and `resolve_wave_lobe`'s own docstring,
`implement_per_episode_defer_crossfamily.md`) -- `disagreement`/
`majority_frac` are still computed and reported exactly as before, now
purely as a QC signal of how much a wave's episodes vary.
"""

SIGN_FRAC_DECISIVE_THRESHOLD = 1.0 - MINORITY_DISAGREEMENT_FRAC
"""
Per-episode confidence bar: an episode's OWN `sign_frac` (from
`_averaged_sign_lobe`, amplitude-weighted agreement with its own resolved
sign) must clear this to stand alone as its own smoothing fold-reference
in `estimate_wave_final`. Below it, the episode falls back to the pooled
wave-level value instead -- not because it disagrees with the wave's
majority (that alone is no longer disqualifying, see
`doy144_wave7_reversal_investigation.md` Task 3), but because ITS OWN
evidence isn't decisive enough to trust on its own terms, independent of
what any other episode says.

Deliberately reuses `1 - MINORITY_DISAGREEMENT_FRAC` (0.70) rather than
inventing a second, unrelated constant -- both thresholds answer "is this
evidence decisive," just measured on different populations (frame-count
share of the whole wave, vs. amplitude-weighted sign agreement within one
episode); anchoring them to the same number keeps the module's notion of
"decisive" consistent rather than accidentally introducing two
independently-tunable, uncalibrated bars.

**`sign_frac` alone is not sufficient -- found directly, not assumed,
exactly the "sanity check against available cases" the design doc called
for.** The initial single-condition version of this rule was checked
against DOY173 Family13 -- the same real named wave this whole module's
lobe-sign convention was calibrated against -- and found wrong: a genuine
3-frame minority sub-episode (frames 51-53, `sign_frac=0.813`) cleared
0.70 and asserted its own ~194 deg value against the dominant 70-frame
episode's ~8 deg, a real, wrong-looking discontinuity for one of the two
ground-truth anchor cases. `sign_frac` on a 3-frame sample is a fragile
statistic -- easily pushed above a fixed bar by chance, not because the
evidence is genuinely strong. See `SIGN_FRAC_DECISIVE_MIN_FRAMES` for the
fix: this threshold and a frame-count floor are BOTH required.
"""

SIGN_FRAC_DECISIVE_MIN_FRAMES = 5
"""
Companion to `SIGN_FRAC_DECISIVE_THRESHOLD`: an episode must ALSO have at
least this many frames to stand alone -- `sign_frac` on too few frames is
too easily pushed above a fixed threshold by chance to trust on its own
(found directly against DOY173 Family13, see `SIGN_FRAC_DECISIVE_THRESHOLD`'s
own docstring). Matches this module's own pre-existing precedent for what
counts as "too small to trust": `resolve_wave_lobe`'s docstring already
cites DOY173 Family13's original diagnosis -- "purely because a 3-frame
middle fragment disagreed" -- as the motivating case for frame-count-weighted
voting in the first place. 5 is chosen to exclude that case and the several
1-2-frame degenerate episodes found in the same audit (a 1-frame episode's
`sign_frac` is trivially 1.000, agreeing with itself, and carries no real
statistical weight at all) while still comfortably including DOY144
Family7's real motivating case (12 and 50 frames respectively).

Checked against the available named cases (`implement_per_episode_defer_
crossfamily.md`, re-verified after adding this floor, not assumed correct
on the first try): DOY144 Family7's both episodes (12, 50 frames) clear
both bars and correctly preserve their own genuine reversal; DOY173
Family13's 3-frame fragment now correctly falls back to the pooled value,
restoring its established ~9 deg behavior. Like `SIGN_FRAC_DECISIVE_THRESHOLD`,
not independently re-derived from a full-batch sweep the way
`MINORITY_DISAGREEMENT_FRAC` was -- flagged as the shape of the rule, not a
finally calibrated threshold; a case with a genuinely short (4-5 frame)
but real reversal, if one exists in the batch, would be the next thing to
check this against.
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

    **Revised again per `implement_per_episode_defer_crossfamily.md`**: an
    earlier version of this function asserted the pooled wave-level azimuth
    only when NOT `disagreement`, reasoning that a genuinely-disagreeing
    wave should get no wave-level answer at all rather than one "presented
    as if it were as trustworthy as any other result." That reasoning
    still holds for what it was built to prevent (silently averaging over
    a real disagreement) -- but direct evidence
    (`doy144_wave7_reversal_investigation.md`) found it had its own,
    larger failure mode: DOY144 Family7's `majority_frac` (0.8065) sat just
    under the flagging bar, so `disagreement` was `False` and the pooled
    azimuth WAS asserted and used -- silently overwriting an entire
    100%-confident, independently-resolved episode with the OPPOSITE of
    its own evidence, precisely because a genuine physical reversal
    produces exactly the frame-count pattern (one longer episode
    outweighing one shorter, oppositely-signed one) this majority vote was
    built to resolve. The pooled value is no longer used to overwrite an
    episode's own resolution based on `disagreement` alone -- see
    `SIGN_FRAC_DECISIVE_THRESHOLD` for the narrower condition
    `estimate_wave_final` now actually uses it for (an episode whose own
    evidence isn't decisive on its own terms).

    `wave_azimuth_deg`/`wave_sign_frac` are therefore now computed and
    returned unconditionally (no longer NaN'd out when `disagreement` is
    True) -- callers that want "only give me a number when the wave agrees
    with itself" should check `disagreement` explicitly rather than relying
    on NaN, since this function no longer makes that decision for them.
    Each episode still gets its own independently-resolved azimuth/sign
    (`ep_results`) regardless.

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
        # Per-episode azimuth/sign for smoothing's fold reference: each
        # episode's OWN independently-resolved azimuth/sign BY DEFAULT --
        # not the wave-level pooled value, and not gated on
        # `lobe["disagreement"]` -- per `implement_per_episode_defer_
        # crossfamily.md`, replacing the pool-when-agreeing policy that
        # silently overwrote DOY144 Family7's genuinely-reversing early
        # episode with the opposite of its own 100%-confident evidence
        # (see resolve_wave_lobe's own docstring). Falls back to the
        # pooled wave-level value when this episode's own sign_frac isn't
        # decisive AND substantial enough to trust on its own terms
        # (SIGN_FRAC_DECISIVE_THRESHOLD + SIGN_FRAC_DECISIVE_MIN_FRAMES --
        # both required, see their own docstrings for why a sign_frac bar
        # alone isn't sufficient) -- a narrower, per-episode condition, not
        # the old wave-wide disagreement gate.
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
