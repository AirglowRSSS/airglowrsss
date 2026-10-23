# Final output schema — design proposal (not implemented)

Task 2, resumed per `task_1c_remove_and_unblock.md`. Design only — no export code
written in this pass. Direction source is settled by the sign-disagreement
investigation: **`wave['azimuth']`/`azimuth_full` is the trusted source of truth for
final propagation direction.** `kf_omega_rads` and its sign play no role in the final
direction field (see `SIGN_DISAGREEMENT_SYNTHETIC_SWEEP.md`).

## Where things stand today (for reference)

- `families_to_xarray()` (`wave_validation.py`) — first-pass only, per-window raw
  detections, `(family, time)` xarray Dataset, no `_2`-anything, no second-pass data.
- `kf_results.csv` — per-frame, per-wave, second-pass, `kf_`-prefixed columns, includes
  the collision/dedup flags (`collision_id`, `collision_primary`,
  `collision_primary_wave_id`, `collision_group_wave_ids`).
- `kf_binned.csv` — per-wave, per-hour (default `bin_width_min=60`), already computed
  from `collision_primary == True` frames only (i.e., already deduped), via
  `compute_binned_wave_stats()`. Circular mean/std for orientation, plain mean/std for
  speed and wavelength, duty cycle from `wave_active` fraction.

`kf_binned.csv` is close to what's being asked for, but it's hourly, not one row per
wave per night, and it doesn't carry the fixed `wave['azimuth']` value at all — only
the dynamic, per-bin `kf_orient_deg` circular mean. Both gaps are addressed below.

## 1. Proposed schema

**New table: `kf_final.csv`, one row per `(wave_id, night)`.** Container: CSV via
pandas, not xarray — see the naming-convention section for why. Built as a new
aggregation step on top of the *existing* `collision_primary`-filtered per-frame data
(the same input `compute_binned_wave_stats` already uses), not a replacement for
`kf_results.csv` or `kf_binned.csv` — both stay as-is; this is an additional, coarser
summary layer.

Proposed columns:

| Column | Source / derivation | Notes |
|---|---|---|
| `site`, `date` | run config | night identifier |
| `wave_id` | as today | family index for this night |
| `label` | `wave['label']` | human-readable, e.g. "Family 3 (az~194 deg, lam~28 km)" |
| `final_azimuth_deg` | `wave['azimuth']` (fixed, full-circle, ω-k detection) | **direction source of truth — not derived from `kf_omega_rads`** |
| `final_orientation_deg` | `wave['orientation_deg']` | fixed half-circle axis, kept for traceability back to the kernel actually used |
| `final_wavelength_km` | duty-cycle/amplitude-weighted mean of `kf_lambda_km` over `collision_primary==True & wave_active==True` frames | see aggregation section |
| `wavelength_std_km` | weighted std, same frame set | |
| `final_speed_ms` | duty-cycle/amplitude-weighted mean of `kf_wavespeed_ms`, same frame set | |
| `speed_std_ms` | weighted std, same frame set | |
| `kf_orient_deg_mean` | circular mean of `kf_orient_deg`, same frame set | **not** the final direction; see note below |
| `kf_orient_deg_std` | circular std, same frame set | large value signals real intra-night rotation — see open question 4 |
| `n_frames_total` | count of all frames this wave_id was ever assigned (owned window), regardless of validity/suppression | |
| `n_frames_active` | count where `wave_active == True` | |
| `n_frames_suppressed` | count where `collision_primary == False` | |
| `frac_suppressed` | `n_frames_suppressed / n_frames_total` | see suppressed-waves section |
| `duty_cycle` | `n_frames_active / n_frames_total` | matches `kf_binned.csv` convention |
| `t_start`, `t_end` | first/last `time` with `wave_active == True` | |
| `mean_amplitude` | mean `amplitude` over active frames | rough SNR/confidence proxy |
| `merged_into_wave_id` | see suppressed-waves section | null unless this wave was ever fully absorbed |
| `source_kf_results_csv` | path | traceability back to full per-frame record |

**Why keep `kf_orient_deg_mean` at all if it's not the direction source of truth?**
Because it's a genuinely useful cross-check and diagnostic (e.g., large disagreement
between `final_azimuth_deg` and `kf_orient_deg_mean` could flag a family whose
detected axis genuinely doesn't match what the second-pass reconstruction tracks) —
but it must not be confused with, or silently substituted for, `final_azimuth_deg` in
any downstream use. Column naming keeps this explicit (`kf_orient_deg_mean`, not
`final_orient_deg_mean`) precisely to avoid the kind of accidental substitution that
caused this whole investigation.

## 2. Naming convention: `_2` suffix vs. `kf_` prefix

**Recommendation: adopt `kf_` prefix as the standing convention going forward; treat
baseline's `_2`-suffix convention as superseded, and update
`CLAUDE_original_2026-07-02.md` to say so explicitly** (not done in this pass — this is
a design recommendation for you to accept or reject, consistent with how the Voronoi
2×→1× revision was handled).

Reasoning: the `_2` suffix was designed for an xarray-Dataset-with-shared-column-names
export path (`orientation_deg` vs. `orientation_deg_2` as two data variables on the
same Dataset) that was never built — `families_to_xarray()` only ever produces
first-pass data, and every actual second-pass artifact produced since (across multiple
sessions, not just this one) uses the `kf_` prefix in a CSV. Reversing course now to
retroactively adopt `_2` suffixes would mean either (a) migrating `kf_results.csv`
and `kf_binned.csv`'s already-in-use column names, which has a real cost for anyone
already consuming those files, or (b) maintaining two parallel naming conventions
across the codebase indefinitely. Neither seems worth it relative to just formalizing
what's already the actual, working practice.

## 3. Suppressed waves: dropped or retained?

**Retained, not dropped — but excluded from active climatology counts by default.**

- A wave_id that is **partially** suppressed (some frames `collision_primary==True`,
  some `False`) gets a normal `kf_final.csv` row, built only from its
  `collision_primary==True` frames (exactly matching current `kf_binned.csv`
  behavior) — `frac_suppressed` reports how much of its lifetime was ceded to another
  wave, as a data-quality signal, not a reason to drop the row.
- A wave_id that is **fully** suppressed (`frac_suppressed == 1.0` — every single frame
  it ever had was resolved in favor of another wave_id) gets **no independent summary
  row** of its own, since it contributes zero unique frames to the record and would
  otherwise silently double-count a wave already fully represented by its target. It is
  instead recorded via `merged_into_wave_id` on... itself would be a dead row, so
  concretely: keep a **separate, small companion table**, `kf_merge_log.csv`
  (`night, wave_id, merged_into_wave_id, n_frames`), so a fully-absorbed family is
  auditable (you can always answer "what happened to Family N tonight?") without
  polluting the primary per-wave-per-night table with zero-unique-frame rows that would
  need to be filtered out by every downstream consumer.

This makes climatology counting correct by construction (summing `kf_final.csv` rows
never double-counts a physical wave) while keeping the resolution fully auditable
after the fact, per the task's explicit requirement.

## 4. What's lost going from per-frame to per-wave-per-night — and one open question

Lost or compressed by this aggregation, worth stating plainly rather than discovering
later:

- **Intra-night drift.** If a wave's orientation or wavelength genuinely evolves over
  the night (the kind of drift the whole family-clustering/Voronoi system exists to
  track), collapsing to one row per night flattens that into a single mean ± std.
  `kf_orient_deg_std`/`wavelength_std_km` are included specifically so a large value is
  visible as a flag that the single-point summary may not be representative — but the
  *shape* of the drift (linear ramp vs. two distinct regimes vs. noise) is not
  recoverable from `kf_final.csv` alone; `kf_binned.csv`'s hourly rows remain the
  right place to look when that matters.
- **Per-frame uncertainty structure.** `kf_lambda_std_km`, `kf_orient_std_deg`,
  `innovation_residual`, etc. exist per-frame in `kf_results.csv` with their own
  Kalman-filter-derived uncertainty that isn't a simple sample std of the point
  estimates — the proposed schema uses sample std across frames (simpler, consistent
  with what `compute_binned_wave_stats` already does) rather than propagating the KF's
  own per-frame covariance forward, which would be more rigorous but is a larger design
  change than this pass should make unilaterally.
- **Aggregation weighting choice.** Proposed: amplitude-weighted mean (frames with
  stronger signal count more), which is a genuine choice among several defensible
  options (plain mean over active frames; duty-cycle-weighted; median for robustness
  to outlier frames). Amplitude-weighting is proposed because `amplitude` already
  exists per-frame as an SNR proxy and using it is a small, well-motivated
  extension of `compute_binned_wave_stats`'s existing approach — but this is a
  judgment call I'd want confirmed rather than assumed, same as the Task 0 threshold
  question.

**Open question for you, not resolved here:** should `kf_final.csv` really be one row
per wave per night, or would one row per wave *per hourly bin* (i.e., essentially
`kf_binned.csv` plus the fixed `final_azimuth_deg` column bolted on) better serve the
climatology use case? The task description says "one final parameter set... per unique
physical wave per night," which I've taken at face value as one-row-per-night above,
but if downstream climatology work actually wants diurnal resolution preserved, the
right move might be to keep `kf_binned.csv`'s hourly grain and just add
`final_azimuth_deg` to it directly, rather than building a third, coarser table. I
don't have enough information about the downstream climatology consumer's actual needs
to make this call — flagging it explicitly rather than guessing.

## Not done in this pass

No export function was written. No existing file (`kf_results.csv`, `kf_binned.csv`,
`families_to_xarray()`) was modified. Per Task 2's original scope and this task's
instructions, this stops here for review.
