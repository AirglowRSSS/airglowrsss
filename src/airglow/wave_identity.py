"""
wave_identity.py
-----------------
The unified wave-identity / frame-ownership module — one place that answers
"is this the same physical wave" and "who owns this frame," replacing three
previously-uncoordinated mechanisms that each answered a related but distinct
version of that question independently (`REFACTOR_DESIGN.md` Task 1/2):

  Stage A — cluster_detections_into_families()
      Sparse per-window ω-k detections -> discrete families. Ports
      wave_validation.py's build_wave_families(). Operates before any Gabor
      reconstruction exists.

  Stage B — resolve_frame_ownership()
      Discrete families -> dense per-frame ownership on the full night grid.
      Ports gabor_reconstruction.py's build_waves_for_gabor() (Option 1
      half-window extension + Option 2 Voronoi tie-break).

  Stage C — resolve_collisions()
      Actual reconstructed fields -> episode-resolved duplicate/collision
      decisions via direct cross-correlation. Ports and generalizes
      phase_speed_kalman.py's flag_kf_wave_collisions_by_correlation() to be
      pass-agnostic: it used to run only on the second-pass reconstruction,
      with gabor_reconstruction.py's check_duplicate_waves() providing a
      separate, weaker (whole-night-averaged, informational-only) answer for
      the first pass. Per the refactor kickoff decision, check_duplicate_waves
      folds into this stage -- the same per-frame/episode mechanism now runs,
      operationally, on both passes. This is a real behavior change on the
      first pass, not a pure port -- see this module's own verification
      script for what it found on the reference nights.

  unified_validity_mask()
      Combines Stage B's ownership with Stage C's collision outcome and a
      fresh signal-presence check into ONE authoritative per-frame validity
      array -- the single answer both kernel_tuning.py and
      final_estimation.py should consume, rather than each stage combining
      masks itself (STEP5_DESIGN_SCOPING.md had final_estimation.py do this
      ad hoc; that combining logic now lives here instead).

These three stages are NOT collapsed into one function -- they operate on
three different kinds of evidence (sparse discrete detections, dense
per-frame kinematic continuity, dense per-frame field similarity),
available at three genuinely different points in the pipeline, in an
unavoidable sequential dependency. See REFACTOR_DESIGN.md Task 2 for the
full reasoning.

Every angular comparison below imports from angles.py rather than
reimplementing the arithmetic -- this module is itself the primary
consolidation point the angles.py module was built for.
"""

import numpy as np
import pandas as pd
from scipy.stats import pearsonr

from airglow.angles import (
    axis_diff_deg, full_circle_diff_deg, circular_mean_axis_deg, circular_mean_full_deg,
)


# ---------------------------------------------------------------------------
# Stage A -- cluster raw per-window detections into families
# ---------------------------------------------------------------------------

def cluster_detections_into_families(
    window_results: list,
    dir_tol_deg: float = 15.0,
    lambda_tol_km: float = 20.0,
    min_detections: int = 1,
    verbose: bool = True,
) -> list:
    """
    Cluster per-window detections into temporally coherent wave families.

    Faithful port of wave_validation.py's build_wave_families(), using
    angles.py's axis_diff_deg/full_circle_diff_deg in place of that
    function's local _angular_diff/_full_circle_diff (identical formulas --
    see angles.py's own regression tests for the equivalence check). No
    behavior change from the current, already-fixed mechanism: this is a
    consolidation port, not a redesign.

    Grouping is based only on direction and wavelength (not speed), so a
    family can track a wave whose speed evolves over time. Requires BOTH
    axis (mod-180) AND full-circle azimuth proximity to merge a detection
    into an existing group -- the full-circle check is what stops two
    counter-propagating, same-axis detections from silently merging (the
    bug this mechanism already had fixed this session).

    Parameters
    ----------
    window_results  : output of sliding_window_analysis()
    dir_tol_deg     : max azimuth difference (degrees) to merge into same family
    lambda_tol_km   : max wavelength difference (km) to merge into same family
    min_detections  : minimum number of windows a family must appear in to be kept
    verbose : bool
        Print per-family summary (matches the original's unconditional prints,
        now gateable).

    Returns
    -------
    families : list of dicts, each containing:
        family_id         : int
        direction_center  : mean axis (degrees, [0,180)) of the FOUNDING detection
        azimuth_full_center : mean azimuth (degrees, [0,360)) of the FOUNDING detection
        wavelength_center : wavelength (km) of the FOUNDING detection
        members           : list of per-detection dicts with keys:
                            time, direction, azimuth_full, wavelength, speed,
                            period, power, confidence
    """
    all_waves = []
    for r in window_results:
        for w in r["physical_waves"]:
            full_az = float(w["azimuth"])
            all_waves.append({
                "time":         r["center_time"],
                "direction":    full_az % 180,          # [0, 180) for Gabor orientation
                "azimuth_full": full_az,                # [0, 360) for full-circle propagation
                "wavelength":   float(w["wavelength"]),
                "speed":        float(w["phase_speed"]),
                "period":       float(w["period"]),
                "power":        float(w.get("power",      np.nan)),
                "confidence":   float(w.get("confidence", np.nan)),
            })

    if verbose:
        print(f"Total detections across all windows: {len(all_waves)}")

    # Greedy grouping -- centroid is FIXED at the founding detection (not
    # updated to a running mean) so total parameter spread per family stays
    # bounded to <= dir_tol_deg / <= lambda_tol_km, rather than letting slow
    # drift chain detections arbitrarily far via many small incremental
    # steps. See wave_validation.py:318-341 for the full original reasoning,
    # including why axis-only proximity is not sufficient on its own.
    groups = []
    for w in all_waves:
        matched = False
        for g in groups:
            if (
                axis_diff_deg(w["direction"], g["direction_center"]) < dir_tol_deg
                and full_circle_diff_deg(w["azimuth_full"], g["azimuth_full_center"]) < dir_tol_deg
                and abs(w["wavelength"] - g["wavelength_center"]) < lambda_tol_km
            ):
                g["members"].append(w)
                matched = True
                break
        if not matched:
            groups.append({
                "direction_center":    w["direction"],
                "azimuth_full_center": w["azimuth_full"],
                "wavelength_center":   w["wavelength"],
                "members":             [w],
            })

    # Per-window deduplication: keep only the strongest-power detection per
    # (family, window-time) pair, in case the peak-finder split one wave's
    # power across two nearby k-bins.
    for g in groups:
        seen_times = {}
        for m in g["members"]:
            t = m["time"]
            if t not in seen_times or m["power"] > seen_times[t]["power"]:
                seen_times[t] = m
        n_before = len(g["members"])
        g["members"] = list(seen_times.values())
        n_after = len(g["members"])
        if verbose and n_after < n_before:
            print(f"  Dedup: removed {n_before - n_after} weaker co-temporal "
                  f"peak(s) in family "
                  f"(dir~{g['direction_center']:.0f} deg, "
                  f"lam~{g['wavelength_center']:.0f} km)")

    families = [g for g in groups if len(g["members"]) >= min_detections]
    families = sorted(families, key=lambda g: -len(g["members"]))
    for i, f in enumerate(families):
        f["family_id"] = i + 1

    if verbose:
        print(f"Wave families found (>={min_detections} detections): {len(families)}")
        for f in families:
            speeds = [m["speed"] for m in f["members"]]
            print(f"  Family {f['family_id']}: dir={f['direction_center']:.1f} deg  "
                  f"lam={f['wavelength_center']:.1f} km  "
                  f"n={len(f['members'])}  "
                  f"speed {min(speeds):.0f}-{max(speeds):.0f} m/s")

    return families


# ---------------------------------------------------------------------------
# Stage B -- resolve dense per-frame ownership from discrete families
# ---------------------------------------------------------------------------

def interpolate_family_to_time_grid(
    family,
    time_grid,
    parameter_keys=("direction", "wavelength", "speed"),
    win_half_ns=0.0,
):
    """
    Map family member parameters onto the full dataset time grid, Option 1
    (half-window extension) coverage semantics.

    Faithful port of gabor_reconstruction.py's function of the same name --
    no behavior change. Kept as its own function (not merged into
    resolve_frame_ownership) since it's genuinely a per-family operation,
    called once per family before the cross-family Voronoi tie-break in
    resolve_frame_ownership needs all families' results at once.
    """
    members = sorted(family["members"], key=lambda m: m["time"])
    t_mem = np.array(
        [np.datetime64(m["time"], "ns") for m in members],
        dtype="datetime64[ns]",
    ).astype(np.float64)
    t_grid = time_grid.astype(np.float64)

    dist_matrix = np.abs(t_grid[:, None] - t_mem[None, :])
    nearest_dist = dist_matrix.min(axis=1)
    valid_mask = nearest_dist <= win_half_ns

    n_valid = int(valid_mask.sum())
    n_total = len(t_grid)
    out = {"_valid_mask": valid_mask, "_nearest_dist": nearest_dist}

    for key in parameter_keys:
        vals = np.array([m[key] for m in members], dtype=np.float64)
        out_key = "orientation_deg" if key == "direction" else key

        if key == "direction":
            # Circular interpolation for orientation [0,180): double the
            # angle to map onto the full circle, interpolate the unit-vector
            # components (always the short arc), halve back.
            angle2_rad = np.deg2rad(vals * 2.0)
            cos_vals = np.cos(angle2_rad)
            sin_vals = np.sin(angle2_rad)
            cos_interp = np.interp(t_grid, t_mem, cos_vals, left=cos_vals[0], right=cos_vals[-1])
            sin_interp = np.interp(t_grid, t_mem, sin_vals, left=sin_vals[0], right=sin_vals[-1])
            angle2_interp = np.arctan2(sin_interp, cos_interp)
            orient_interp = np.rad2deg(angle2_interp) / 2.0 % 180.0
            result = np.where(valid_mask, orient_interp, np.nan)
        else:
            interp_full = np.interp(t_grid, t_mem, vals, left=vals[0], right=vals[-1])
            result = np.where(valid_mask, interp_full, np.nan)

        out[out_key] = result

    pct = 100.0 * n_valid / n_total if n_total > 0 else 0.0
    print(f"    [family {family.get('family_id', '?')}] "
          f"{n_valid}/{n_total} frames covered ({pct:.0f}%)")
    return out


def resolve_frame_ownership(
    families, time_grid, n_stds=3.0,
    win_half_min=45.0,
    dir_tol_deg=15.0, lambda_tol_km=20.0,
    voronoi_dir_tol=None, voronoi_lam_tol=None,
):
    """
    Build the per-frame-owned wave list from discrete families, using
    Option 1 (half-window extension) + Option 2 (Voronoi) to give every
    frame an unambiguous owner.

    Faithful port of gabor_reconstruction.py's build_waves_for_gabor(),
    using angles.py's axis_diff_deg/full_circle_diff_deg in place of that
    function's local _adiff/_full_circle_diff (identical formulas). No
    ownership/coverage behavior change from the current, already-fixed
    mechanism -- ownership itself (valid_stack/dist_stack/owned_mask) never
    depended on the scalar seed values below, so this is unaffected by the
    one deliberate change from the ported original:

    CONFIRMED DEVIATION FROM THE PORTED ORIGINAL (explicit decision, not a
    silent fix): the scalar seed values (median_orient, median_azimuth) use
    angles.py's circular_mean_axis_deg/circular_mean_full_deg instead of a
    plain np.median over each family's raw member values. The original's
    np.median could get a wrong answer for a family whose members straddle
    the 0/180 (orientation) or 0/360 (azimuth) wrap boundary -- e.g.
    np.median([350, 10]) = 180, not ~0. wavelength and speed are not
    circular quantities and still use np.median, unchanged. This changes
    each affected wave's founding 'orientation_deg'/'azimuth' seed, which
    also feeds resolve_collisions' tie-break distance metric -- re-verified
    against the reference nights after this change, not assumed safe.

    Returns
    -------
    waves : list of dict, one per family, each with '_owned_mask' (bool,
        shape (n_time,)) as its authoritative per-frame ownership answer.
    """
    win_half_ns = win_half_min * 60.0 * 1e9
    n_time = len(time_grid)
    n_fam = len(families)

    all_interp = []
    valid_stack = np.zeros((n_fam, n_time), dtype=bool)
    dist_stack = np.full((n_fam, n_time), np.inf)

    for fi, fam in enumerate(families):
        interp = interpolate_family_to_time_grid(
            fam, time_grid,
            parameter_keys=("direction", "wavelength", "speed"),
            win_half_ns=win_half_ns,
        )
        all_interp.append(interp)
        valid_stack[fi] = interp["_valid_mask"]
        dist_stack[fi] = interp["_nearest_dist"]

    # Two families belong to the same physical wave (subject to Voronoi
    # tie-break) if their founding centroids are within voronoi_dir_tol
    # (axis AND full-circle azimuth) and voronoi_lam_tol of each other.
    # Default: 1x the family-building tolerances (transitive union-find, so
    # a looser multiplier risks chaining unrelated families -- see
    # gabor_reconstruction.py:647-654 for the full original reasoning).
    if voronoi_dir_tol is None:
        voronoi_dir_tol = dir_tol_deg
    if voronoi_lam_tol is None:
        voronoi_lam_tol = lambda_tol_km

    group_id = list(range(n_fam))
    for i in range(n_fam):
        for j in range(i + 1, n_fam):
            if (axis_diff_deg(families[i]["direction_center"],
                               families[j]["direction_center"]) < voronoi_dir_tol
                    and full_circle_diff_deg(families[i]["azimuth_full_center"],
                                              families[j]["azimuth_full_center"]) < voronoi_dir_tol
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
    n_unowned = int(any_unowned.sum())
    for fi, fam in enumerate(families):
        n_owned = int(owned_stack[fi].sum())
        print(f"  Family {fam['family_id']}: {n_owned} frames ({100. * n_owned / n_time:.0f}%)")
    if n_unowned > 0:
        print(f"  Frames with no family active: {n_unowned} ({100. * n_unowned / n_time:.0f}%)")
    else:
        print("  All frames covered by at least one family.")

    waves = []
    for fi, (fam, interp) in enumerate(zip(families, all_interp)):
        members = fam["members"]
        owned_mask = owned_stack[fi]
        orient_series = np.where(owned_mask, interp["orientation_deg"], np.nan)
        wl_series = np.where(owned_mask, interp["wavelength"], np.nan)

        median_orient = circular_mean_axis_deg([m["direction"] for m in members])
        median_wl = float(np.median([m["wavelength"] for m in members]))
        median_speed = float(np.median([m["speed"] for m in members]))
        full_azimuths = [m.get("azimuth_full", m["direction"]) for m in members]
        median_azimuth = circular_mean_full_deg(full_azimuths)

        waves.append({
            "label": (f"Family {fam['family_id']}  "
                      f"(az~{median_azimuth:.0f} deg, "
                      f"lam~{median_wl:.0f} km)"),
            "orientation_deg": median_orient,
            "wavelength_km": median_wl,
            "azimuth": median_azimuth,
            "phase_speed": median_speed,
            "n_stds": n_stds,
            "orientation_deg_series": orient_series,
            "wavelength_km_series": wl_series,
            "_owned_mask": owned_mask,
            "_win_half_min": win_half_min,
            "_group_id": group_id[fi],
        })

    return waves


# ---------------------------------------------------------------------------
# Stage C -- resolve field-correlation collisions, pass-agnostic
# ---------------------------------------------------------------------------

def resolve_collisions(
    waves: list,
    wave_cubes: list,
    complex_amplitudes: list,
    time_vals: np.ndarray,
    pass_label: str,
    corr_threshold: float = 0.7,
    smoothing_window: int = 5,
    min_episode_frames: int = 3,
    dir_tol_deg: float = 22.5,
    lambda_tol_km: float = 20.0,
    apply_duplicate_gate: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Flag periods where two or more waves' reconstructions are essentially
    the same physical structure, via direct field cross-correlation, and
    resolve one primary per episode.

    Generalizes phase_speed_kalman.py's flag_kf_wave_collisions_by_correlation
    (algorithm unchanged: per-pair smoothed correlation -> per-frame
    connected components -> episode detection -> eligibility-gated
    founding-parameter-distance tie-break) to run on EITHER reconstruction
    pass, not just the second. Per the refactor kickoff decision, this
    replaces gabor_reconstruction.py's check_duplicate_waves() entirely --
    that function's weaker, whole-night-averaged, informational-only check
    is superseded by this same per-frame/episode mechanism now running
    operationally on the first pass too. `pass_label` is used only for
    logging (e.g. "first-pass" / "second-pass"), so callers can tell which
    pass a given result came from.

    Structural fix over the ported original, not just a port: the original
    wrote results into a copy of an existing kf_results DataFrame, using
    kf_results' own "frame_idx" column for one alignment step and a
    timestamp-based searchsorted for another -- two different index spaces
    that a later stage (the tiebreak fix) found could silently disagree by
    a fixed offset (kf_results' frame_idx is relative to an edge-trimmed
    slice, not the full night grid). This version builds its output
    DataFrame directly from `waves`/`time_vals` -- the same grid
    `wave_cubes`/`complex_amplitudes` already use -- so there is no second,
    independently-indexed space to accidentally conflate. No kf_results-
    shaped input required, which is also what makes this usable on the
    first pass, before any KF-stage output exists.

    Eligibility gate (already-fixed tiebreak behavior, extended here -- see
    FIRSTPASS_DUPLICATE_GATE_DESIGN.md / firstpass_gate_rollout_task.md): a
    wave with amplitude effectively zero for every frame of an episode
    doesn't enter the comparison AT ALL now, not just lose the tie-break --
    it's dropped from the episode's group entirely and left untouched
    (collision_primary stays True), rather than being markable as "lost to
    a duplicate" when it had no real signal to duplicate in the first place
    (this was the exact w1v18 moot case STAGE1_FINAL_CHECKS.md's Check 2
    found). If every member of an episode is signal-free, falls back to the
    original distance-only choice over the full group (outcome is moot
    either way) -- this fallback is carried forward unchanged.

    Duplicate-detection gate, `apply_duplicate_gate` (added
    `firstpass_gate_rollout_task.md`, scoped
    `decouple_and_fix_smoothing_task.md`, accepted as-is with documented
    limitations `stage1_closeout_task.md`): when True (the default),
    correlation alone is not sufficient for two waves to be treated as
    colliding at a frame -- FIRSTPASS_DUPLICATE_GATE_DESIGN.md found several
    cases (first-pass reconstructions specifically) where correlation
    crossed threshold (0.876, 0.705) for waves with visibly different axis
    or wavelength. Edge formation also requires
    axis_diff_deg(orientation) < dir_tol_deg AND
    abs(wavelength diff) < lambda_tol_km -- both evaluated on the SAME
    smoothing_window rolling mean already applied to correlation
    (`decouple_and_fix_smoothing_task.md`'s Task 3). The zero-amplitude
    eligibility rule also gates group MEMBERSHIP under this mode (see
    above), not just tie-break preference.

    Scoping is an explicit parameter, not implicit pass-detection --
    `pass_label` is a logging string only and this function does not
    inspect it. **Callers decide per call: the default (True) is for
    first-pass calls only. Second-pass calls MUST explicitly pass
    `apply_duplicate_gate=False`** -- second pass stays on its original,
    extensively-validated correlation-only mechanism (byte-for-byte match
    to phase_speed_kalman.py's flag_kf_wave_collisions_by_correlation, exact
    regression match confirmed across all 5 reference nights). Do not
    assume this gate is pass-agnostic just because the function accepts
    both passes -- it structurally can run on either, but it is only
    validated, and only intended, for the first pass. With
    `apply_duplicate_gate=False`, zero-amplitude waves stay in the full
    group (eligible only preferences primary selection, original "fall back
    to the full group if nobody's eligible" semantics, not a group-
    membership exclusion).

    **Known, accepted limitation of the axis/wavelength proximity check
    (not fixed, documented per `stage1_closeout_task.md`)**: `lambda_tol_km`
    was designed for a different physical question -- Stage A/B use it to
    ask "has this one wave's own tracked wavelength drifted too far from
    where it started," a temporal-continuity question -- and was reused here
    unvalidated for "are these two simultaneously-reconstructed waves the
    same physical wave," a completely different question, without checking
    whether a fixed absolute km tolerance is appropriate for it.
    `FIRSTPASS_GATE_ROLLOUT.md`/`DECOUPLE_AND_FIX_SMOOTHING.md` found two
    concrete cases where it lets through a real, non-noisy relative
    wavelength gap: `w4v2` (DOY150, ~92.8 vs ~78.9 km, 13.9 km / ~17%
    relative) and `w6v15` (DOY150, ~126.2 vs ~145.8 km, 19.5 km / ~14%
    relative) -- both pairs whose reconstructed fields visibly differ in
    fringe spacing yet pass `lambda_tol_km=20.0` comfortably, stably across
    many frames (confirmed not a smoothing artifact -- Task 3's fix changed
    almost nothing for either). Accepted for Stage 1 because this gate's
    actual purpose is an intermediate kernel-tuning signal (deciding which
    detections feed second-pass kernel construction), not final output --
    the cost of an occasional false-negative duplicate here is different
    from, and lower than, the cost of the same failure in final estimation.
    If this needs revisiting -- including if second-pass duplicate-gating is
    ever reconsidered -- the natural next step is a purpose-specific,
    possibly relative/scale-aware tolerance for duplicate-detection
    specifically, not a first-pass-specific override of the existing one
    (dir_tol_deg/lambda_tol_km stay the same values Stage A/B use; no
    first-pass-specific override was introduced).

    **Known, accepted limitation of `smoothing_window=5`**: no documented
    physical rationale exists for this value anywhere in the codebase or its
    history (`decouple_and_fix_smoothing_task.md` Task 2: `corr_threshold`
    has a detailed empirical justification -- "just above the observed p99
    of ~0.68" -- `smoothing_window` has never had one; one commit, `63f5b4d`,
    has ever touched it, and never explained the number). Per
    `refactor_foundation.md`'s standing requirement that every tunable
    parameter carry a physically-derived rationale, this is flagged as a
    known gap rather than silently treated as fine -- inherited, not
    validated, not changed here.

    CONFIRMED DECISION on `complex_amplitudes`: always pass the SAME pass's
    own amplitude (first-pass amplitude for a first-pass call, second-pass
    for a second-pass call), not always first-pass regardless of pass. The
    original `flag_kf_wave_collisions_by_correlation` always used
    `kf_results['amplitude']` (first-pass-derived) for this gate even when
    resolving second-pass collisions -- almost certainly because second-pass
    amplitude was never retained anywhere before this session's fixes, not a
    deliberate choice. Verified on all 5 reference nights: feeding this
    function first-pass amplitude for a second-pass call reproduces the old
    function's primary/loser decisions exactly (0 mismatches everywhere);
    each pass using its own amplitude (this function's actual behavior)
    differs from that old behavior by 26-482 primary-flag disagreements per
    night -- expected and intended, not a bug.

    Parameters
    ----------
    waves : list of dict
        Must have 'label', 'orientation_deg', 'wavelength_km' (founding
        scalar values) and 'orientation_deg_series'/'wavelength_km_series'
        ((n_time,) arrays for this pass -- for the first pass this is
        `waves` straight from resolve_frame_ownership(); for the second
        pass it's the KF-refined per-frame series built by kernel_tuning.py).
    wave_cubes : list of np.ndarray, each (n_time, ny, nx)
        This pass's reconstructed fields, one per wave, same order as `waves`.
    complex_amplitudes : list of np.ndarray, each (n_time,) complex
        This pass's cross-correlation phasor per wave (from
        reconstruct_all_waves' own return value) -- used for the
        eligibility gate via |A|. Decoupled from any kf_results-shaped
        input specifically so this works before Step 3 exists.
    time_vals : np.ndarray of datetime64
        Full night time grid, same length/order as wave_cubes[i].
    pass_label : str
        Logging label only ("first-pass" / "second-pass" or similar).

    Returns
    -------
    pd.DataFrame, one row per (wave_id, frame), columns:
        wave_id, time, frame_idx, pass, collision_id, collision_group_wave_ids,
        collision_primary, collision_primary_wave_id
    """
    n = len(waves)
    n_time = len(time_vals)

    base_rows = pd.DataFrame({
        "wave_id": np.repeat(np.arange(n), n_time),
        "time": np.tile(time_vals, n),
        "frame_idx": np.tile(np.arange(n_time), n),
    })
    base_rows["collision_id"] = -1
    base_rows["collision_group_wave_ids"] = ""
    base_rows["collision_primary_wave_id"] = -1
    base_rows["collision_primary"] = True

    if n < 2 or len(wave_cubes) < 2:
        if verbose:
            print(f"No collision check ({pass_label}): fewer than 2 waves.")
        return base_rows

    smoothed = {}
    axis_diff_by_pair = {}
    wl_diff_by_pair = {}
    for i in range(n):
        for j in range(i + 1, n):
            cube_i, cube_j = wave_cubes[i], wave_cubes[j]
            raw = np.full(n_time, np.nan)
            for t in range(n_time):
                a, b = cube_i[t].ravel(), cube_j[t].ravel()
                if not (np.any(a) and np.any(b)):
                    continue
                m = np.isfinite(a) & np.isfinite(b)
                if m.sum() > 1:
                    r, _ = pearsonr(a[m], b[m])
                    if np.isfinite(r):
                        raw[t] = r
            smoothed[(i, j)] = (
                pd.Series(raw).rolling(window=smoothing_window, min_periods=1).mean().to_numpy()
            )
            # Per-frame axis/wavelength proximity, from each wave's own
            # per-frame series -- the duplicate-detection gate (see
            # docstring): correlation alone is not sufficient to call two
            # waves the same physical structure. Smoothed with the SAME
            # rolling window as correlation (Task 3, decouple_and_fix_
            # smoothing_task.md) -- comparing a smoothed correlation series
            # against instantaneous axis/wl values let genuinely-different
            # pairs slip through when their correlation dipped below
            # threshold on the sampled frame but stayed elevated nearby
            # (the smoothed series), while axis/wl (unsmoothed) still
            # happened to read close on that same frame; see
            # FIRSTPASS_GATE_ROLLOUT.md's w4v2/w6v15 findings.
            if apply_duplicate_gate:
                oi = np.asarray(waves[i]["orientation_deg_series"], dtype=float) % 180.0
                oj = np.asarray(waves[j]["orientation_deg_series"], dtype=float) % 180.0
                li = np.asarray(waves[i]["wavelength_km_series"], dtype=float)
                lj = np.asarray(waves[j]["wavelength_km_series"], dtype=float)
                axis_diff_by_pair[(i, j)] = (
                    pd.Series(axis_diff_deg(oi, oj))
                    .rolling(window=smoothing_window, min_periods=1).mean().to_numpy()
                )
                wl_diff_by_pair[(i, j)] = (
                    pd.Series(np.abs(li - lj))
                    .rolling(window=smoothing_window, min_periods=1).mean().to_numpy()
                )

    all_ids = list(range(n))

    def _components(edges):
        parent = {wid: wid for wid in all_ids}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for a, b in edges:
            union(a, b)
        groups = {}
        for wid in all_ids:
            groups.setdefault(find(wid), set()).add(wid)
        return [frozenset(g) for g in groups.values() if len(g) >= 2]

    frame_groups = []
    for t in range(n_time):
        edges = []
        for pair, s in smoothed.items():
            if not (np.isfinite(s[t]) and s[t] >= corr_threshold):
                continue
            if apply_duplicate_gate:
                ad = axis_diff_by_pair[pair][t]
                wld = wl_diff_by_pair[pair][t]
                if not (np.isfinite(ad) and ad < dir_tol_deg):
                    continue
                if not (np.isfinite(wld) and wld < lambda_tol_km):
                    continue
            edges.append(pair)
        frame_groups.append(set(_components(edges)))

    episodes = []
    open_eps = {}
    for t in range(n_time + 1):
        active = frame_groups[t] if t < n_time else set()
        for g in list(open_eps.keys()):
            if g not in active:
                start = open_eps.pop(g)
                if t - start >= min_episode_frames:
                    episodes.append((g, start, t - 1))
        for g in active:
            if g not in open_eps:
                open_eps[g] = t

    if not episodes:
        if verbose:
            print(f"No correlation-based collisions detected ({pass_label}, "
                  f"threshold={corr_threshold:.2f}).")
        return base_rows

    label_map = {i: w["label"] for i, w in enumerate(waves)}
    amp_by_wave = {wid: np.abs(complex_amplitudes[wid]) for wid in range(n)}

    rows_by_key = {}  # (wave_id, frame_idx) -> row dict, for O(1) write-back
    for wid in range(n):
        for t in range(n_time):
            rows_by_key[(wid, t)] = dict(
                wave_id=wid, time=time_vals[t], frame_idx=t,
                collision_id=-1, collision_group_wave_ids="",
                collision_primary_wave_id=-1, collision_primary=True,
            )

    next_cid = 0
    printed = []
    for group, t_start, t_end in episodes:
        seg = slice(t_start, t_end + 1)
        eligible = set()
        for wid in group:
            amp_i = np.nan_to_num(amp_by_wave[wid][seg], nan=0.0)
            if not np.all(amp_i < 1e-9):
                eligible.add(wid)

        # Zero-amplitude ineligibility: under the duplicate gate (see
        # docstring), this excludes signal-free members from GROUP
        # MEMBERSHIP entirely, not just primary-preference -- a signal-free
        # member can't be marked collision_primary=False for "losing" to a
        # duplicate it had no real presence to lose to. With the gate off,
        # this reverts exactly to the original semantics: `eligible` only
        # preferences which member is picked primary; every member of
        # `group` still gets a row (this is the byte-for-byte
        # flag_kf_wave_collisions_by_correlation match apply_duplicate_gate
        # =False requires).
        if apply_duplicate_gate:
            comparison_group = eligible if eligible else set(group)
        else:
            comparison_group = set(group)

        dists = {}
        for wid in comparison_group:
            gi = float(waves[wid]["orientation_deg"]) % 180.0
            gli = float(waves[wid]["wavelength_km"])
            oi = waves[wid]["orientation_deg_series"][seg]
            li = waves[wid]["wavelength_km_series"][seg]
            d = np.nanmean(axis_diff_deg(oi % 180.0, gi) / dir_tol_deg
                            + np.abs(li - gli) / lambda_tol_km)
            dists[wid] = d if np.isfinite(d) else np.inf

        if apply_duplicate_gate:
            primary = min(comparison_group, key=dists.get)
        else:
            candidates = eligible if eligible else set(dists)
            primary = min(candidates, key=dists.get)

        cid = next_cid
        next_cid += 1
        group_str = ", ".join(label_map[w] for w in sorted(comparison_group))
        printed.append((comparison_group, t_start, t_end, primary))

        for wid in comparison_group:
            for t in range(t_start, t_end + 1):
                row = rows_by_key[(wid, t)]
                row["collision_id"] = cid
                row["collision_group_wave_ids"] = group_str
                row["collision_primary_wave_id"] = primary
                row["collision_primary"] = (wid == primary)

    if verbose:
        print(f"{len(episodes)} correlation-based collision episode(s) "
              f"detected ({pass_label}, threshold={corr_threshold:.2f}):")
        for group, t_start, t_end, primary in printed:
            members = ", ".join(label_map[w] for w in sorted(group))
            print(f"  {{{members}}}  frames {t_start}-{t_end} ({t_end - t_start + 1} frames) "
                  f"-- kept {label_map[primary]}")

    out = pd.DataFrame(rows_by_key.values())
    out["pass"] = pass_label
    return out.sort_values(["wave_id", "frame_idx"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Unified per-frame validity mask
# ---------------------------------------------------------------------------

def unified_validity_mask(owned_mask, collision_primary, amplitude, snr_min_amplitude=0.05):
    """
    The single authoritative per-frame validity answer, combining Stage B's
    ownership with Stage C's collision outcome and a fresh signal-presence
    check -- replaces the ad hoc combination STEP5_DESIGN_SCOPING.md had
    final_estimation.py build for itself (owned_mask AND collision_primary
    AND fresh-SNR); that combining logic now lives here, computed once,
    for kernel_tuning.py and final_estimation.py to both consume identically.

    Closes a real gap `collision_primary` has on its own: it defaults True
    for any frame that never triggered the correlation check at all --
    including a frame with no real signal that also doesn't resemble any
    other wave. `amplitude > 0` and the fresh SNR check catch that case
    directly, in whichever pass's own amplitude series is passed in
    (should be `wave_cubes`/`complex_amplitudes`-derived for the same pass
    `collision_primary` was resolved against).

    Parameters
    ----------
    owned_mask : np.ndarray, bool, shape (n_time,)
        Stage B's per-frame ownership answer for this wave.
    collision_primary : np.ndarray, bool, shape (n_time,)
        Stage C's per-frame outcome for this wave, this pass.
    amplitude : np.ndarray, float, shape (n_time,)
        |complex_amplitudes| for this wave, this pass.
    snr_min_amplitude : float
        Same threshold already tuned/used elsewhere in the pipeline for
        this exact purpose (kf_snr_min) -- reused, not reinvented.

    Returns
    -------
    np.ndarray, bool, shape (n_time,)
    """
    owned_mask = np.asarray(owned_mask, dtype=bool)
    collision_primary = np.asarray(collision_primary, dtype=bool)
    amp = np.nan_to_num(np.asarray(amplitude, dtype=float), nan=0.0)
    amp_ref = amp.max() if amp.max() > 0 else 1.0
    return owned_mask & collision_primary & (amp > 1e-9) & ((amp / amp_ref) >= snr_min_amplitude)
