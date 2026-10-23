# Design: fix the founding-parameter-distance tie-break bias

Design only, no implementation yet — follows `TIEBREAK_BIAS_VALIDATION.md`, which
found the bias explains ~54% of collision outcomes cleanly (likely 60–70% including
the self-reinforcing `MIXED` cases), found zero legitimate counterexamples to "frozen
never deserves to win," and concluded a simple, categorical fix is well-supported by
the evidence rather than something more delicate.

## Exact fix location

`phase_speed_kalman.py`, `flag_kf_wave_collisions_by_correlation`, the per-episode
resolution loop, lines 1481–1493:

```python
for group, t_start, t_end in episodes:
    seg = slice(t_start, t_end + 1)
    dists = {}
    for wid in group:
        gi  = float(waves_detected[wid]["orientation_deg"]) % 180.0
        gli = float(waves_detected[wid]["wavelength_km"])
        oi  = waves_kf[wid]["orientation_deg_series"][seg]
        li  = waves_kf[wid]["wavelength_km_series"][seg]
        d = np.nanmean(_circ_diff_180(oi % 180.0, gi) / dir_tol_deg
                        + np.abs(li - gli) / lambda_tol_km)
        dists[wid] = d if np.isfinite(d) else np.inf
    primary = min(dists, key=dists.get)
```

`oi`/`li` come from `waves_kf[wid]`'s always-finite, ownership-blind series (built
in `airglow_pipeline.py` from `kf_orient_deg`/`kf_lambda_km`, frozen at the wave's
own init value rather than NaN when there's no real data — see
`FAMILY6_WAVELENGTH_JUMP_INVESTIGATION.md` Task 1/3). A frozen wave's `oi`/`li` sit
exactly on `gi`/`gli` for every frame it has no data, contributing exactly 0 to the
distance sum — that's the mechanism. This loop has no visibility into `amplitude` at
all right now; that's the missing ingredient.

## The fix: an eligibility gate before the distance comparison

```python
for group, t_start, t_end in episodes:
    seg = slice(t_start, t_end + 1)
    dists = {}
    eligible = set()
    for wid in group:
        amp_i = amp_by_wave[wid][seg]                       # NEW
        if not np.all(amp_i < 1e-9):                        # NEW: not 100% zero
            eligible.add(wid)                                # NEW

        gi  = float(waves_detected[wid]["orientation_deg"]) % 180.0
        gli = float(waves_detected[wid]["wavelength_km"])
        oi  = waves_kf[wid]["orientation_deg_series"][seg]
        li  = waves_kf[wid]["wavelength_km_series"][seg]
        d = np.nanmean(_circ_diff_180(oi % 180.0, gi) / dir_tol_deg
                        + np.abs(li - gli) / lambda_tol_km)
        dists[wid] = d if np.isfinite(d) else np.inf

    candidates = eligible if eligible else set(dists)        # NEW: fallback
    primary = min(candidates, key=dists.get)
```

`amp_by_wave[wid]`: a new, precomputed `(n_time,)` array per wave, built once
before the episode loop (parallel to how `smoothed[(i,j)]` is already precomputed)
by pulling `amplitude` from `kf_results`/`df`, sorted by `frame_idx` so it's
aligned to the same frame-index grid `waves_kf[wid]`'s series already use — no new
alignment mechanism needed, just mirroring what the function already does.

**Threshold: exactly (within float tolerance) zero for every frame in the
episode — not a softer "mostly zero" cut.** This is deliberately the tightest,
best-grounded rule available: `amplitude == 0.0` for an unowned/no-detection frame
is not a noisy small value, it's a hard, literal `0.0 + 0.0j` assignment
(`_reconstruct_adaptive`, confirmed in two prior investigations) — so "100% zero
across the whole episode" identifies exactly the population this investigation
found zero legitimate counterexamples for. A wave with even one genuinely nonzero
frame in the episode keeps competing under the existing metric unchanged.

**Fallback when nothing is eligible** (`BOTH_FROZEN`, ~4% of episodes, both/all
members 100%-zero the whole episode): keep today's behavior exactly as-is. Nothing
real is being reconstructed by anyone in that episode either way, so the choice of
"primary" has no material effect on `composite_kf`/`kf_binned.csv` — not worth
adding special-case logic to a path where the outcome doesn't matter.

## What this changes vs. leaves alone

| episode type (from validation) | share | effect of this fix |
|---|---|---|
| `GHOST_WINS_OVER_ACTIVE` | 54% | **flips** — the real wave becomes eligible-only-candidate, wins |
| `MIXED` | 22% | **partially affected** — a 100%-zero member loses eligibility; a partially-active member (e.g. Family 14's later, self-reinforcing episodes) keeps competing under the unchanged metric |
| `BOTH_ACTIVE_AMBIGUOUS` | 15% | **unaffected** — both stay eligible, same comparison as today |
| `BOTH_FROZEN` | 4% | **unaffected** — fallback preserves current (moot) behavior |
| `ACTIVE_WINS_OVER_GHOST` | 4% | **unaffected** — already correct, stays correct (real wave was already winning; now for a more defensible reason too) |

Suppression mechanics don't need to change — a wave that loses eligibility was, by
definition, contributing nothing to `composite_kf` already (`wave_cubes_kf` is
already zero there); this only changes *which* member gets labeled `primary` and
therefore *whose real signal survives* when a genuine wave was being displaced by
a ghost.

## Deliberately not proposed here (flagging, not deciding)

- **A looser eligibility threshold** (e.g. the ≥90%-zero cut the validation
  analysis used for its own classification) would additionally catch some of the
  `MIXED` cases — closer to the 60–70% "true" estimate rather than the ~54%
  this narrow fix directly resolves. Not proposed as the default because the
  validation only established "100% zero" as unambiguous; nothing in the evidence
  rules out a genuinely marginal-but-real detection somewhere in the 1–89%-zero
  range, and a looser cut risks disqualifying one. This is a real tuning decision,
  not a technical detail — your call if you want the more aggressive version.
- **Replacing the founding-distance metric entirely** (e.g. amplitude-weighted, or
  "highest amplitude wins") — not proposed. The evidence says nothing about
  whether the existing metric is right or wrong for the 15% genuinely-contested
  `BOTH_ACTIVE_AMBIGUOUS` cases; changing it there would be an unevidenced,
  separate design decision, not a fix for the bias actually found.

## Verification plan for when this is implemented

Re-run DOY 173 (pre- and post-Voronoi-fix) and DOY 145, then re-apply the same
classifier used in `TIEBREAK_BIAS_VALIDATION.md` as a regression check:
`GHOST_WINS_OVER_ACTIVE` should drop to ~0 (any survivors individually
explainable, e.g. both-100%-zero fallback cases); `BOTH_ACTIVE_AMBIGUOUS` and
`ACTIVE_WINS_OVER_GHOST` episodes should be byte-identical before/after, since
their eligible set is unchanged. Also worth re-checking `kf_merge_log.csv` output
— genuinely real waves like Family 2 should now appear there far less often (or
not at all), since they'll stop losing every collision they're actually winning
on the merits.
