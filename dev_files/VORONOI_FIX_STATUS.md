# Status: Voronoi axis-only grouping fix (`voronoi_azimuth_fix_task.md`)

Current state as of this write-up. Nothing in this thread of work is committed yet
(`git status`: `gabor_reconstruction.py` modified, `FAMILY13_INVESTIGATION.md` and
`VORONOI_BUG_PREFIX_SWEEP.md` untracked).

## Task A — Pre-fix sweep: **done**

`VORONOI_BUG_PREFIX_SWEEP.md`. Scanned 22 already-processed nights/runs for the
counter-propagating-axis-mates signature. 19 candidate instances across 10
nights. Headline finding, confirmed directly (not just signature-matched): on
DOY 173 2023 LOW, Family 2 — the single most-owned, strongest-amplitude family
in the whole dataset — loses 89% of its frames (116/130, essentially the whole
night) to two families that own **zero** frames of their own, ever. This is
larger in scope than the Family 13 case the original investigation was scoped
around.

## Task B — Implement the fix: **done**

`gabor_reconstruction.py`, `build_waves_for_gabor`'s Voronoi merge condition
(`_adiff`, around line 656) now also requires `azimuth_full` proximity, mirroring
`build_wave_families`'s existing (already-correct) pattern — same tolerance
(`voronoi_dir_tol`), same reasoning, no new tolerance introduced.
`build_wave_families` itself untouched, as instructed. Syntax-checked, not yet
committed.

Verified directly against `waves_detected`'s `_group_id`/`_owned_mask` on DOY 173:
Family 13 and Family 2 are now each in their own solo Voronoi group (previously
shared with Family 6/10 and Family 14 respectively). Family 13's ownership window
now starts at its true Option-1 boundary (07:24, was 07:50). Family 14 — total
ghost before, `n_owned=0` — now legitimately owns 50 frames. Same-direction
drift-stitching (the mechanism's original purpose) is unaffected, as expected.
**This part of the fix is correct and works exactly as designed.**

## Task C — Verify against the known case: **partially done, blocked on a real finding**

The specific checklist item in the task file — "Family 6 no longer flagged as
colliding with Family 13, its own reconstruction no longer suppressed" — **does
not hold**. Before/after collision losses on DOY 173:

| wave | OLD losses/130 | NEW losses/130 |
|---|---|---|
| Family 13 (12) | 0 | 21 |
| Family 6 (5) | 113 | 114 |
| Family 2 (1) | 116 | **120** |
| Family 14 (13) | 15 | 14 |
| Family 10 (9) | 0 | **120** |

Family 6 keeps losing at essentially the same rate. Family 2 gets slightly worse.
Family 10 — previously another total ghost, now given real ownership by the fix
— immediately starts losing 120/130 frames to Family 11 in the collision check.
A fresh run with `make_animation=True` (for the movie generated on request) shows
the full extent: 26 separate collision episodes post-fix, several 3-5-family
pileups sharing one axis (e.g. `{Family 2, Family 6, Family 10, Family 11,
Family 14}` all contesting one span at once), with Family 11/14 usually winning
over Family 2/6.

**Why:** the Voronoi ownership fix and the collision-correlation suppression
(`flag_kf_wave_collisions_by_correlation`) are two separate mechanisms sharing
the same root vulnerability — Gabor kernels select by axis only, blind to true
360° direction — but the second one was never gated by Voronoi group membership
in the first place. It runs on the second-pass reconstruction
(`wave_cubes_kf`), which rebuilds a kernel from `kf_orient_deg` against raw data
regardless of who owns what. Fixing ownership correctly gave two former ghosts
(Family 14, Family 10) real reconstructions for the first time — a genuine,
correct effect of the fix — which is exactly what newly exposed them to that
same, still-unfixed collision tie-break.

**This is where the work is currently paused**, per the task file's own
instruction to stop and describe rather than guess when a change reaches beyond
the scenario it targeted. Reported to you last turn; **awaiting a decision** on
how to proceed:
- expand scope to also make `flag_kf_wave_collisions_by_correlation` (or the
  underlying kernel selectivity) azimuth-aware, or
- accept the ownership-grouping fix as complete on its own terms (it is,
  narrowly) and document Task C's outcome as "fix correct, symptom only
  partially resolved," or
- something else.

The post-fix movie for DOY 173
(`outputs/case_studies/realdata_low_2023_173_voronoi_fix/movies/realdata_low_2023_173_postfix.mp4`)
was generated on request, separate from the formal task list, for visual
inspection while this decision is pending.

## Task D — Post-fix sweep: **not started**

Blocked on Task C's open question — re-running the full sweep now would produce
a report describing this same partial-fix nuance for every night, which is
useful either way, but better done once we know whether the scope is expanding
first (so it isn't redone).

## Task E — Documentation: **not started**

Blocked on the same decision — the lesson to record in
`CLAUDE_original_2026-07-02.md` differs depending on whether the fix ends up
scoped narrowly (ownership grouping only, with collision-correlation flagged as
a known separate gap) or expanded to cover both mechanisms.

## Files touched so far

- `src/airglow/gabor_reconstruction.py` — modified, uncommitted (Task B fix)
- `FAMILY13_INVESTIGATION.md` — new, untracked (prior task, diagnosis)
- `VORONOI_BUG_PREFIX_SWEEP.md` — new, untracked (Task A)
- `VORONOI_FIX_STATUS.md` — this file
- `outputs/case_studies/realdata_low_2023_173_voronoi_fix/` (notebooks folder,
  not git-tracked) — post-fix verification run + movie, output separately from
  the canonical pre-fix reference directory so the pre-fix baseline stays intact
  for Task D's before/after comparison
