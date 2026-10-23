# Pre-fix sweep: scope of the Voronoi axis-only grouping bug

Per Task A of `voronoi_azimuth_fix_task.md`. No code changed in this pass — this is
a scan of already-processed `kf_results.csv` outputs only.

## Method

For every processed night, `kf_prop_azimuth_deg` and `gabor_wavelength_km` are fixed
per `wave_id` (set once at family construction), so they can be read directly off
`kf_results.csv` without re-running anything. For every `(loser, winner)` pair that
appears in `collision_primary_wave_id` (i.e. an already-recorded, already-applied
suppression from `composite_kf`), I flagged it as a candidate instance of this bug
when:

- axis difference (mod 180°) between the two waves' fixed azimuths < 25°
- full-circle difference > 155° (i.e. close to 180° apart — opposite propagation)
- wavelength difference < 25 km

(25° and 25 km are deliberately looser than the pipeline's own `dir_tol_deg=22.5`/
`lambda_tol_km=20.0` to avoid missing borderline cases; script and raw output at
`sign_disagreement`-style scratch files, available on request.)

**Caveat on confidence:** this signature test only proves "these two waves are
axis-close and near-opposite" — it does not, by itself, prove they were actually
placed in the same Voronoi `group_id`. I independently confirmed that for **DOY 173
2023 LOW only** (both pairs below), by re-checking `waves_detected`'s `_group_id`/
`_owned_mask` directly, since that data was already in hand from
`FAMILY13_INVESTIGATION.md`. For every other night below, the flag is signature-only
— consistent with the bug, not individually re-verified against live group
membership (that requires a fresh pipeline run, which Task A explicitly says not to
do yet). Treat DOY 173 as **confirmed**, everything else as **candidate**.

Excluded from the scan: all `outputs/smoketest_*` and `outputs/task1c_removal_check`
directories (throwaway dev/debug iterations from earlier this session, not
independent processed nights), and three stale/superseded duplicates:
`outputs/case_studies/realdata_low_2023_173/realdata_low_2023_173_kf_results.csv`
(top-level, pre-`results/`-subdirectory reorg — superseded by the copy actually
used throughout this investigation), `outputs/case_studies/realdata_low_2022_147_o`
(Aug 7, superseded by the Aug 18 `realdata_low_2022_147` run), and
`outputs/case_studies/realdata_cfs_2023_173/realdata_low_2023_173_kf_results.csv`
(a `low`-stemmed file sitting inside the `cfs` site directory — looks like a stray
leftover, not a genuine "cfs" result).

## Headline finding: DOY 173 2023 LOW — confirmed, and worse than originally scoped

`FAMILY13_INVESTIGATION.md` characterized this night's damage as Family 13 (wave_id
12) borrowing Family 6's (wave_id 5) signal for ~1h44m. The sweep surfaces a second,
**more severe** instance on the same night that investigation didn't go looking for:

**wave_id 1 (Family 2, az=17.4°) loses 116 of its 130 frames (89%, spanning
05:56–10:14 — essentially the whole night) to wave_id 13 (Family 14, az=180.0°,
axis-identical, **owns zero frames of its own, ever**) and briefly to wave_id 9
(Family 10, az=0.0°, also owns zero frames of its own, ever).**

Both wave_id 13 and wave_id 9 were confirmed in the earlier investigation's
ownership dump to have `n_owned=0` — they exist only as frozen, ever-never-updated
second-pass reconstructions built directly from raw data at their founding axis, per
the mechanism in `FAMILY13_INVESTIGATION.md`'s Task 3. Family 2 is not a marginal
family here — it is the single most-owned family in the whole dataset
(`n_owned=135/142` frames per the earlier ownership dump) and its amplitude reaches
into the hundreds (up to 891 at 09:42). Confirming this isn't just correlation
noise: **mean amplitude during the frames Family 2 loses (117.4) is nearly double
its mean amplitude during the frames it legitimately wins (63.1)** — the
suppression disproportionately hits its strongest signal, exactly as expected if
the phantom duplicate is winning the tie-break specifically when there's a strong
real signal at that axis to copy.

Practical consequence: `kf_binned.csv` for this night shows wave_id 1 as
`insufficient_data=True` in essentially every bin — which, read in isolation,
looks like "Family 2 barely detected anything," when the raw `kf_results.csv`
shows the opposite: it's the dominant wave of the night, almost entirely erased
downstream by this bug.

Second, previously-known instance on the same night — Family 13 (wave_id 12,
az=19.1°) vs Family 6 (wave_id 5, az=200.7°), 05:56–07:40, 40 frames — matches
`FAMILY13_INVESTIGATION.md` exactly (confirms the sweep's method against the
already-hand-verified case).

## All candidate instances found

| Night | Loser (az) | Winner (az) | axis Δ | full Δ | λ Δ (km) | Span | Frames | Loser active | Winner active |
|---|---|---|---|---|---|---|---|---|---|
| **low_2023_173 (canonical)** | **1 (17.4°)** | **13 (180.0°)** | 17.4° | 162.6° | 6.7 | 05:56–10:14 | **116** | 64 | **0** |
| low_2023_173 (canonical) | 5 (200.7°) | 9 (0.0°) | 20.7° | 159.3° | 24.6 | 07:42–08:42 | 29 | 54 | **0** |
| low_2023_173 (canonical) | 5 (200.7°) | 12 (19.1°) | 1.6° | 178.4° | 6.1 | 05:56–07:40 | 40 | 54 | 71 |
| low_2022_143 | 6 (160.5°) | 0 (327.8°) | 12.7° | 167.3° | 22.4 | 04:22–04:34 | 7 | 81 | 100 |
| low_2022_144 | 5 (311.5°) | 4 (143.0°) | 11.5° | 168.5° | 21.5 | 08:24–08:42 | 10 | 137 | 100 |
| low_2022_146 | 4 (180.0°) | 5 (8.9°) | 8.9° | 171.1° | 10.1 | 04:24–09:20 | 53 | 124 | 90 |
| low_2022_146 | 5 (8.9°) | 4 (180.0°) | 8.9° | 171.1° | 10.1 | 09:22–10:02 | 21 | 90 | 124 |
| low_2022_147 (batch) | 12 (187.4°) | 4 (13.3°) | 5.8° | 174.2° | 9.6 | 05:00–09:50 | 13 | 91 | 152 |
| low_2022_147 (batch) | 12 (187.4°) | 9 (25.2°) | 17.8° | 162.2° | 17.1 | 04:26–05:16 | 21 | 91 | 113 |
| low_2022_150 | 1 (22.0°) | 13 (223.3°) | 21.3° | 158.7° | 21.6 | 08:36–10:18 | 52 | 110 | 78 |
| low_2022_151 | 2 (43.3°) | 5 (234.7°) | 11.4° | 168.6° | 20.1 | 08:32–09:22 | 26 | 152 | 129 |
| low_2022_151 | 5 (234.7°) | 2 (43.3°) | 11.4° | 168.6° | 20.1 | 04:30–10:18 | 42 | 129 | 152 |
| low_2022_151 | 5 (234.7°) | 10 (32.2°) | 22.6° | 157.4° | 2.8 | 04:38–08:00 | 24 | 129 | 49 |
| low_2022_154 | 3 (13.3°) | 5 (209.5°) | 16.2° | 163.8° | 9.8 | 05:54–06:24 | 11 | 84 | 59 |
| low_2022_155 | 3 (8.9°) | 6 (164.9°) | 24.0° | 156.0° | 11.7 | 09:04–09:26 | 12 | 107 | 60 |
| low_2022_147 (case study, different config) | 3 (250.5°) | 12 (54.7°) | 15.8° | 164.2° | 19.6 | 05:54–06:34 | 16 | 101 | 36 |
| low_2022_147 (case study, different config) | 10 (70.5°) | 14 (270.0°) | 19.5° | 160.5° | 4.4 | 04:32–10:20 | **163** | 48 | 39 |
| low_2022_147 (case study, different config) | 12 (54.7°) | 2 (242.1°) | 7.3° | 172.7° | 23.8 | 05:14–08:34 | 14 | 36 | 138 |
| cfs_2023_173 | 4 (212.2°) | 5 (13.3°) | 18.9° | 161.1° | 17.0 | 06:10–09:26 | 55 | 31 | 30 |

19 candidate pairs total, across 10 of the 22 scanned nights/runs. Two nights carry
the two directly-verified DOY 173 instances; the rest are signature-only.

Nights scanned with **no** candidate pairs found: low_2022_145 (batch — note:
distinct from the separately-run `sign_investigation_low_2022_145`, same night, both
clean), 148, 149, 152, 153, 156, 157, 158, 159, and `blo_2023_173`.
`sign_investigation_low_2022_145` (the fresh DOY 145 2022 LOW run from the earlier
sign-disagreement investigation) is also clean.

Note `low_2022_147` appears twice with different results (batch-run config vs.
case-study config) — expected, since family/wave_id assignment depends on the
processing parameters, not just the raw data.

## Which nights have downstream outputs that would go stale

Every scanned night/run **has** `kf_binned.csv` already generated (i.e. every one
of the above is a live, in-use output, not a leftover from an abandoned run).
`kf_merge_log.csv` only exists for the canonical DOY 173 2023 LOW `results/` run
(the only one processed after that file type was added in the prior task) — it
would need to be generated fresh, not refreshed, for every other night.

| Output | Present for |
|---|---|
| `kf_binned.csv` | All 22 scanned nights/runs |
| `kf_merge_log.csv` | Only `outputs/case_studies/realdata_low_2023_173/results/` |
| `*_kf_collisions.png` | All except `outputs/case_studies/realdata_low_2022_147/` and `realdata_blo_2023_173/`, `realdata_cfs_2023_173/` (those have a `12a_kf_collisions.png` instead — an older naming convention, same underlying content) |

So: **every** night with a candidate finding above has a `kf_binned.csv` in active
use that is potentially wrong in the specific way described (a suppressed real wave
undercounted/marked `insufficient_data`, and/or a phantom wave overcounted) — 9
nights/runs with at least one candidate, most severely `low_2023_173 (canonical)`.

No regeneration performed in this pass, per the task's instruction — this is scope
assessment only.
