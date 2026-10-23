# Investigation: wave_id 12 (Family 13), early-night direction mismatch

Diagnosis only, per `family13_investigation_task.md`. No fix implemented in this pass.

**Dataset/config used throughout:** DOY 173, 2023 LOW, `dir_tol_deg=22.5`,
`lambda_tol_km=20.0`, `orientation_resolution_deg=22.5`, `power_threshold=90`,
`min_detections=2` — the same config used for this session's other DOY 173 runs.
`WIN_LEN_MIN=90`, `HOP_MIN=10` (pipeline defaults, confirmed to match the
notebook's own values). All findings below are read directly from the current
code (paths/line numbers cited) and from a fresh pipeline run against this
config, not assumed from memory or from the task file's description.

## Task 0 — Establish the actual mechanics first

**0.1 — Is the displayed propagation azimuth fixed, or can it change? Does
`innovation_valid` gate updates?**

It changes over time, and yes, `innovation_valid` gates it — but indirectly,
through a chain of two separate quantities that is easy to conflate:

- `kf_prop_azimuth_deg` (`phase_speed_kalman.py:1146`, `prop_az = omk_az`) *is*
  fixed for the whole wave/night — set once from `wave['azimuth']` (the
  founding ω-k detection's median azimuth) and never updated per frame. This
  is **not** what the movie plots.
- The movie's arrow instead uses `kf_orient_deg` (`airglow_pipeline.py:1500`,
  `az_kf[kf_idx] = wdf["kf_orient_deg"].values[match]`) — the **time-varying**
  output of the orientation Kalman tracker (`_kalman_theta_tracker`, Feature
  A), driven by the spatial phase gradient (kx, ky) of the reconstructed
  field. This one genuinely updates frame-to-frame.
- Critically, that update is gated: `_kalman_theta_tracker`'s `ext_valid`
  (`phase_speed_kalman.py:440`) is fed the **omega** filter's combined
  validity mask (`combined_mask = innov_valid_w & snr_ok_w`,
  `phase_speed_kalman.py:1072`), not anything of its own. When
  `innovation_valid` (from the omega tracker) is False, the theta tracker
  coasts on its own prediction with no measurement update
  (`phase_speed_kalman.py:462-465`) — its estimate stays essentially frozen
  wherever it last was (drift only via the ~zero `dtheta/dt` state).

So: **not fixed forever, but frozen for the duration of any `innovation_valid
== False` span**, and that freeze is a direct, intentional consequence of the
omega filter's gate. This exactly matches "the arrow doesn't change" during
the reported invalid period, and "the arrow updates" once valid resumes.

**0.2 — What does `innovation_valid` measure? Can direction disagreement
alone cause it to go False?**

`innovation_valid` (`_kalman_omega_tracker`, `phase_speed_kalman.py:241`) is a
chi²-gate + SNR check on **ω's magnitude only**:
`innov_valid[t] = snr_ok and (e**2 / S <= chi2_gate)`, where `e` is the
innovation between the observed and predicted `|ω|` (both already `abs()`-ed
upstream — see the standing baseline note on `kf_omega_rads` always being
≥ 0 by design, established earlier this session). There is no phase or
direction term in this gate at all — it cannot fail *because* of a direction
disagreement per se. It fails when the observed frequency magnitude disagrees
with the filter's running frequency estimate, or when `|A_cross|` (SNR) is
too low. A genuine direction reversal only shows up here indirectly — e.g. if
it coincides with a change in the underlying signal's coherence or apparent
frequency.

**0.3 — Does the family-merge criterion check full-circle direction, or only
the half-circle axis? Would a reversed-but-same-axis wave get caught or
merge silently?**

This is where the real gap is, and it is **not** where the task's framing
expected it (`build_wave_families`, the founding-detection clustering step
that groups raw per-window ω-k detections into a family). That function
already, explicitly, checks both:

```python
# wave_validation.py:346-349
_angular_diff(w["direction"], g["direction_center"]) < dir_tol_deg
and _full_circle_diff(w["azimuth_full"], g["azimuth_full_center"]) < dir_tol_deg
and abs(w["wavelength"] - g["wavelength_center"]) < lambda_tol_km
```

The surrounding comment (`wave_validation.py:328-341`) documents that this
full-circle check was added specifically to stop two axis-aligned,
opposite-direction detections from silently merging — this part of the
pipeline is already hardened against exactly the failure mode this task
asks about.

**The gap is one step later**, in `build_waves_for_gabor`'s Voronoi
ownership grouping (`gabor_reconstruction.py:656-668`), which decides which
of several already-separate *families* get treated as "the same physical
wave" for frame-ownership tie-breaking:

```python
def _adiff(a, b):
    d = abs(a - b) % 180
    return min(d, 180 - d)
...
if (_adiff(families[i]["direction_center"], families[j]["direction_center"]) < voronoi_dir_tol
        and abs(families[i]["wavelength_center"] - families[j]["wavelength_center"]) < voronoi_lam_tol):
    # merge into the same Voronoi group
```

`_adiff` only compares the half-circle axis (mod 180°) — it never looks at
`azimuth_full`. So two families whose true propagation directions are ~180°
apart, but which share an axis and a similar wavelength, **are** placed in
the same ownership group here, silently. This is confirmed to be exactly
what happens to Family 13 below.

## Task 1 — Is Family 13 actually two merged episodes?

**Ruled out.** Family 13 (family_id 13, wave_id 12) has only **2** raw
pre-family-grouping detections for the whole night (it just clears
`min_detections=2`):

| time | direction (axis) | azimuth_full | wavelength | speed |
|---|---|---|---|---|
| 08:08 | 20.66° | 20.66° | 81.8 km | 58.9 m/s |
| 09:38 | 17.45° | 17.45° | 69.5 km | 73.7 m/s |

Both detections agree closely in azimuth (3.2° apart) and wavelength (within
`lambda_tol_km`). There is no bimodal, ~180°-apart split in the raw
detections underlying this family — `build_wave_families`'s full-circle
check (0.3 above) did its job correctly here. Task 1's hypothesis is not the
explanation. Per the task's own instruction, this pushes the explanation to
"elsewhere" — which turns out to be the Voronoi grouping gap identified in
0.3, not (only) Task 2's territory.

## The actual mechanism (Voronoi ownership, then Task 2/3 confirm it)

Pulling `waves_detected`'s `_group_id` / `_owned_mask` for this run:

```
wave_id=12 label=Family 13 (az~19 deg, lam~76 km)  group_id=5  n_owned=77
wave_id=12 group members (wave_id): [5, 9, 12]
  wave_id=5  Family 6  (az~201 deg, lam~82 km)
  wave_id=9  Family 10 (az~0 deg,   lam~106 km)
  wave_id=12 Family 13 (az~19 deg,  lam~76 km)
```

Family 6's median azimuth (200.66°) is 181.6° from Family 13's (19.06°) —
essentially exactly opposite — but only 1.6° apart on the axis (mod 180°),
and its founding wavelength is within `lambda_tol_km` of Family 13's. Exactly
the blind spot from 0.3: two counter-propagating, same-axis, similar-λ
families placed in one Voronoi group. (Family 10 is chained in transitively
— its own axis/λ don't obviously match Family 13's directly, consistent with
the union-find transitive-chaining risk the code's own comment already warns
about.)

Family 13's Option-1 half-window coverage (±45 min around its two
detections) should span 07:23–10:23. Its **actual** owned range is
**07:50–10:22** — the first 27 minutes were won by a group-mate. Family 6
is confirmed strongly present and valid there:

| time | Family 6 (wave_id 5) amplitude | innovation_valid |
|---|---|---|
| 05:56 | 772 | True |
| 06:30–07:24 | 40–970 (mostly hundreds) | True (continuously) |

Meanwhile Family 13 (wave_id 12) over the same span:

| time | amplitude | innovation_valid | kf_orient_deg |
|---|---|---|---|
| 05:56–07:50 | **0.000000, exactly, every frame** | False, every frame | frozen at 19.055° (theta_init) |
| 07:52 onward | 178.9, then sustained 40–500 | True, continuously through 10:14 | updates, ranges ~9–24° |

## Task 2 — Omega/amplitude trajectory at the transition

The amplitude trajectory isn't "noisy then clean" — it's **exactly zero**
for 54 consecutive frames, then a clean jump. That precision is the tell:
this isn't signal-quality noise, it's a hard structural zero. Confirmed in
`_reconstruct_adaptive` (`gabor_reconstruction.py:270-278`):

```python
if not np.isfinite(od) or not np.isfinite(wl):
    cube[t]      = 0.0
    cmplx_amp[t] = 0.0 + 0.0j
    ...
```

`orientation_deg_series[t]` is NaN for every frame Family 13 doesn't own
(`build_waves_for_gabor`: `orient_series = np.where(owned_mask, ..., np.nan)`,
`gabor_reconstruction.py:720`). Before 07:50, Family 13 doesn't own those
frames (Family 6 does) — so its **first-pass** reconstruction is a hard,
literal zero there, not a noisy or contaminated signal. The KF sees no
observation to update on, `innovation_valid` is False, and the theta tracker
freezes at its init value, per 0.1/0.2. The transition (07:50 owned / 07:52
`innovation_valid` first True) lines up cleanly — a 2-minute lag consistent
with the KF needing one non-NaN observation before it can flag valid.

This on its own would predict a **blank** panel during the invalid period,
not visible opposite motion — which is where Task 3 matters.

## Task 3 — What does the movie actually plot?

Not the first-pass reconstruction just discussed — a **second, independent**
reconstruction pass. `airglow_pipeline.py:1637-1638`:

```python
[(cube, f"Wave {wi + 1}") for wi, cube in enumerate(wave_cubes_kf)]
```

`wave_cubes_kf` comes from a **second call** to `reconstruct_all_waves`
(`airglow_pipeline.py:1515`), using `waves_kf`, whose per-frame orientation
series is built from `kf_orient_deg` (`airglow_pipeline.py:1500`) —
**not** gated by ownership at all. `kf_results` has a row for every wave at
every one of the full night's 130 frames regardless of ownership, so
`orient_kf` is finite (frozen at 19.055°, per 0.1) for the entire
05:56–07:50 span. This second-pass kernel is applied directly to
`data_filtered_temporal` (the raw filtered data), at a real, finite,
axis-19° orientation, for the whole night — including the period where
Family 13 itself has no legitimate detection there.

Since Gabor kernels select by orientation **axis** only (mod 180°, matching
the fixed `orientation_deg`/`Btheta` band — see `create_gabor_kernel`, used
identically in both passes) and are structurally blind to which of the two
180°-apart directions the energy at that axis belongs to, this kernel
faithfully picks up whatever real signal exists at axis ~19° in the raw
data during 05:56–07:50 — which is Family 6's real, strong, valid,
opposite-direction (~201°) wave. That is the "visibly moving, ~180° opposite
the frozen arrow" field the task describes: it is a real wave, just not
Family 13's.

**Direct, independent confirmation** — `flag_kf_wave_collisions_by_correlation`
(which runs on exactly this second-pass `wave_cubes_kf`) flags wave_id 5
(Family 6) and wave_id 12 (Family 13) as colliding (correlation > 0.7)
continuously from **05:56 to 07:40**, with wave_id 12 judged
`collision_primary` and Family 6's real, independently-detected,
continuously-valid signal marked `collision_primary=False` and suppressed
from `composite_kf` for that whole span (`airglow_pipeline.py:1613`,
`wave_cubes_kf[wi][kf_idx] = 0.0`). This is a concrete, checkable, material
side effect: for ~1h44m, a genuine 7-member family's own reconstruction is
zeroed out of the composite in favor of what is, per the analysis above, a
duplicate of itself wearing Family 13's (frozen, wrong) label.

## Leading hypothesis

Family 13's early-night direction mismatch is not a bug in the KF, the
omega/theta trackers, or the founding family-clustering step (all of which
behave exactly as documented and, per 0.3, are already correctly
full-circle-aware). It is a consequence of `build_waves_for_gabor`'s Voronoi
ownership-grouping step using an axis-only (`_adiff`, mod 180°) merge test
that has no visibility into `azimuth_full`. Family 13 and Family 6 —
genuinely counter-propagating (~181° apart), same-axis, similar-wavelength —
land in one Voronoi group. Family 6, stronger and temporally denser, wins
ownership of the group's early frames, leaving Family 13 with zero
legitimate first-pass signal there (hence exact-zero amplitude,
`innovation_valid=False`, and a frozen theta estimate). The **second-pass**
reconstruction, though, is not ownership-gated and rebuilds a kernel at the
frozen (wrong, borrowed) orientation directly from the raw data for every
frame regardless — which faithfully reconstructs Family 6's real signal
under Family 13's label, producing the visible ~180°-opposite motion behind
a stale arrow. The collision-correlation check independently corroborates
this: it detects the resulting near-duplicate and suppresses Family 6's own,
legitimate reconstruction in favor of it for the affected span.

Of the 14 wave fields in this dataset, Family 13's minimal detection count
(exactly `min_detections=2`, the smallest a family can be and still survive)
made it the one most exposed to this: with only two founding detections
90 minutes apart, essentially its entire early coverage was up for grabs in
the Voronoi tie-break, whereas families with many, temporally dense
detections rarely lose enough of their own window to a group-mate for this
to become visible. This also suggests where else to look if this recurs:
any low-detection-count family sharing a Voronoi group with a
stronger, opposite-direction, same-axis family is a candidate for the same
symptom.
