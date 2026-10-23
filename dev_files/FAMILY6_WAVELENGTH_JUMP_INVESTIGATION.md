# Investigation: Family 6 (wave_id 5) wavelength jump, DOY 173 2023 LOW

Diagnosis only, per `family6_wavelength_jump_task.md`. No fix, no parameter change.

**Data used:** `outputs/case_studies/realdata_low_2023_173_voronoi_fix/results/realdata_low_2023_173_postfix_kf_results.csv`
— the post-Voronoi-fix run (current working-tree state, same run the requested
movie was generated from). Cross-checked against the untouched pre-fix canonical
run (`outputs/case_studies/realdata_low_2023_173/results/`) for Task 4; the two
are effectively identical for this wave/window (see Task 4).

## Task 1 — Characterize the jump itself

**Smoothed KF output (`kf_lambda_km`):** rises from ~82 km (07:00) through the
90s (07:40) into the low 100s by 08:00, reaching ~112 km at 08:12 — a
**gradual-looking ramp**, consistent with Kalman inertia smoothing something.
Taken at face value this matches the "~80 to ~120 over 30 min" framing loosely
but stretches over closer to 70 minutes end-to-end.

**Raw, pre-smoothing observation (`kf_lambda_obs_km`) — the more important
number, and it tells a different, sharper story:**

| span | behavior |
|---|---|
| 06:54–07:16 | flat/noisy, **78–85 km**, no trend |
| 07:18–07:20 | transition (83.6 → 91.5) |
| **07:22** | **97.7 km** |
| 07:22–07:50 (and on to 08:12) | flat/stable, **~100–105 km** |

This is not a ramp — it's a **step function**: ~22 minutes flat at one level,
a 2-frame (4-minute) transition, then ~28+ minutes flat at a new level roughly
20 km higher. The KF's own inertia (it's a constant-velocity kinematic model on
k_h) is what turns this sharp step into the smoother-looking `kf_lambda_km`
ramp — exactly the "smoothing interpolates across a discontinuity" pattern the
task's working hypothesis describes, though (see Task 2/3) not for the reason
originally guessed.

## Task 2 — Cross-reference against ownership, collision, validity, amplitude

- **Amplitude / ownership:** nonzero and substantial (37–970) continuously from
  05:56 through 08:12 — Family 6 is never unowned, never hits exact-zero,
  anywhere near this jump. This rules out the "ownership loss → blank →
  borrowed second-pass reconstruction" mechanism from `FAMILY13_INVESTIGATION.md`
  specifically — that mechanism requires an exact-zero gap, and there isn't one
  here.
- **`innovation_valid`:** True continuously through 08:12 (only goes False
  starting 08:14, well after this jump, feeding the separate coasting-to-clamp
  runaway described below). No gate transition at the jump.
- **`wavelength_kernel_mismatch`:** False throughout this window. Doesn't fire.
- **`collision_primary` / `collision_primary_wave_id` — this is where it lines
  up.** Pulling the exact frames around the step:

  | time | obs (km) | collision_primary | partner |
  |---|---|---|---|
  | 07:00–07:02 | 82.6, 83.7 | loses→**12**, then briefly independent | |
  | 07:04–07:16 | 81.7–80.4 (flat ~82) | loses → **10**, continuously | |
  | 07:18–07:20 | 83.6 → 91.5 (mid-step) | **independent** (True, no partner) | |
  | 07:22 onward | 97.7, stable ~100–105 | loses → **10**, continuously through 08:12+ | |

  Family 6 briefly regains independence for exactly the 2 frames spanning the
  step (07:18–07:20), then resumes losing to wave_id 10 — the same partner as
  before the step, but now for a sustained, stable, ~30-minute run at the new
  wavelength level. The step is bracketed by a collision-partner transition,
  not embedded in the middle of a stable one.

## Task 3 — Plausible donor wave

wave_id 10 (Family 11): fixed azimuth 346.74°, fixed wavelength **106.41 km**.
The new plateau (~100–105 km, averaging ~103 km) sits close to Family 11's own
wavelength — a real candidate match, similar in spirit to the Family 13/Family 6
azimuth match, though not quite as exact.

**But the direct check complicates this:** wave_id 10's own `amplitude` is
**exactly 0.0000 for every frame from 07:00 through 07:34**, and its
`kf_lambda_km` is frozen at exactly 106.4082 (`k_h_init`, never updated) over
that same span — it only starts producing real, nonzero output at 07:36. So for
most of the window where Family 6 is recorded as "losing to" wave_id 10
(07:04–07:34), wave_id 10 itself has **no real signal to be the donor of**. This
is not the same clean picture as Family 13/Family 6, where the donor
(Family 6) had strong, continuous, valid signal throughout.

**Axis check, also against the established mechanism:** Family 6's azimuth is
200.66°, Family 11's is 346.74° — axis difference (mod 180°) is **33.9°**,
outside `dir_tol_deg` (22.5°). They are not axis-close. This means the
established "same-axis Gabor kernel bleed" mechanism from
`FAMILY13_INVESTIGATION.md` does **not** apply here — the two waves' kernels
shouldn't be picking up each other's raw signal at all. Something else is
producing the correlation `flag_kf_wave_collisions_by_correlation` is keying
off, and it's worth being precise about what: reading that function directly
(`phase_speed_kalman.py:1268`) resolves this cleanly (see leading hypothesis).

## Task 4 — Same event as `VORONOI_FIX_STATUS.md`, or distinct?

**Partially the same, previously under-characterized.** `VORONOI_FIX_STATUS.md`
already flagged Family 6 losing 113→114/130 frames post-fix and specifically
named a 5-way pileup — `{Family 2, Family 6, Family 10, Family 11, Family 14}`
— as one of the ongoing post-fix collision groups. wave_id 10 (Family 11) is a
member of exactly that group. So this is the **same underlying collision
churn** already reported, just examined here through the wavelength channel
specifically rather than noted only as an aggregate frame-loss count. It isn't
a new, separate occurrence — it's a more detailed look at part of an already-
flagged pileup.

Also checked against the untouched pre-fix run: same step, same timing (07:18–
07:22), same ~78–85→~100–105 km levels, same alternation with wave_id 12/9
instead of 10 (partner identities differ slightly pre/post-fix, as expected
since the fix changes group membership) — this phenomenon predates the
Voronoi fix and is unaffected by it, consistent with `VORONOI_FIX_STATUS.md`'s
finding that the collision-correlation mechanism is a separate, still-unfixed
piece.

## Task 5 — Wavelength Kalman filter's process noise, as reported

The wavelength/wavenumber analog of `KF_Q_THETA` is **`kf_q_kh`** (process
noise on k_h) and **`kf_q_dkh`** (process noise on dk_h/dt), both defined in
`_kalman_kh_tracker` (`phase_speed_kalman.py:544-545`) and threaded through
`analyze_all_waves_kalman`/`run_airglow_pipeline`. Current values:

```
kf_q_kh:  1e-12   ((rad/m)^2/step)
kf_q_dkh: 1e-16   ((rad/m/s)^2/step)
```

**No physical rationale found anywhere** — not in the docstring (which states
only "Default 1e-12" / "Default 1e-16" with no derivation), not in any code
comment, not in `CLAUDE_original_2026-07-02.md` (whose parameter table lists
only `KF_Q_THETA` with its explicit ~10°/hr sizing; `kf_q_kh`/`kf_q_dkh` don't
appear there at all), and not in git history (`git log -S"q_kh"` on
`phase_speed_kalman.py` returns exactly one commit — the original bulk
addition of this code, `97246e4`, not a dedicated sizing commit). This is a
bare, unjustified default, unlike `KF_Q_THETA`'s documented, physically-derived
one. Per the task's instruction, no value is proposed or estimated here.

## Leading hypothesis

Not a re-run of the Family 13 mechanism (no ownership loss, no exact-zero gap,
no axis-sharing with the implicated partner), and not obviously genuine
physical signal either (a ~25% wavelength step in 4 minutes, holding at a new
plateau matching a nearby wave's own founding wavelength almost as it
completes, is a hard coincidence to credit as real evolution).

The evidence points to a **third, distinct mechanism**, grounded directly in
`flag_kf_wave_collisions_by_correlation`'s own documented tie-break rule
(`phase_speed_kalman.py:1317-1319`): *"Within each episode, ONE wave is judged
primary: whichever stayed closest, averaged over the episode, to its own
founding parameters."* This rule has a built-in bias: a wave that is coasting
with zero real signal (like wave_id 10 during 07:04–07:34, `amplitude=0.0`
exactly, `kf_lambda_km` frozen dead-on its own founding value) is, by
construction, at zero distance from its own founding parameters — it cannot
drift, because nothing is updating it. A wave with genuine, evolving signal
(Family 6, actively tracking a real, moving wavelength observation) will
almost always measure "further" from its own founding value than a frozen
ghost measures from its. The tie-break can therefore favor an inactive,
data-free wave over an active, data-bearing one, independent of axis-sharing
or actual reconstructed-field similarity — which would also explain why the
"donor" here doesn't need to be axis-close to Family 6 the way Family 6 was to
Family 13.

Under this reading, the wavelength "jump" is less a case of Family 6's own
identity being swapped for a donor's signal (as in the Family 13 case) and more
a case of Family 6's **real, own, genuinely-evolving observation being folded
out of `composite_kf`/`kf_binned.csv`** at exactly the frames where the tie-
break's founding-parameter-distance criterion judged a nearby, currently-inert
wave "more faithful" than it — with the visible jump in `kf_lambda_obs_km`
marking not a substitution of signal, but the boundary of that (mis-)judged
episode. This would mean the underlying vulnerability driving both this case
and the pileups already reported in `VORONOI_FIX_STATUS.md` is not (only) the
axis-only kernel selectivity found earlier, but specifically **this tie-break
rule's blindness to whether the "closer to founding parameters" wave has any
real signal behind it at all** — a more precisely-located, and possibly more
consequential, root cause than what either prior investigation identified. Not
confirmed beyond this one case; would need the same cross-check (frozen,
zero-amplitude "winner" vs. active, evolving "loser") applied to the other
pileups already flagged to know how general it is.
