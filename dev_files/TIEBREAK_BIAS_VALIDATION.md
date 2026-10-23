# Validation: how general is the founding-parameter-distance tie-break bias?

Diagnosis only, per `tiebreak_bias_validation_task.md`. No fix, no parameter change.

**Method:** for every `collision_id` episode in `kf_results.csv`, pulled each member
wave's mean amplitude and `%` of frames at exactly-zero amplitude over that episode's
span. "Frozen/ghost" = ≥90% of frames at exact zero. "Active" = mean amplitude > 20
AND <50% zero frames. Classified each episode by comparing the winner
(`collision_primary_wave_id`) against its losers on this basis. Ran across three
datasets: DOY 173 2023 LOW pre-fix (canonical), DOY 173 2023 LOW post-fix
(`realdata_low_2023_173_voronoi_fix`), and DOY 145 2022 LOW
(`sign_investigation_low_2022_145`) — 68 episodes total, 16 + 26 + 26.

## Task 1 — Specific already-flagged cases

**Family 2's 89% frame loss (pre-fix).** Every recorded collision episode
involving wave_id 1 (Family 2) as a loser shows the exact predicted pattern,
with no exception:

| episode | winner | winner amp / %zero | Family 2's amp / %zero |
|---|---|---|---|
| 1 | w13 | 0.0 / 100% | 96.6 / 0% |
| 3 | w13 | 0.0 / 100% | 56.3 / 0% |
| 6 | w13 | 0.0 / 100% | 46.6 / 0% |
| 8 | w9 | 0.0 / 100% | 194.9 / 0% |
| 9 | w9 | 0.0 / 100% | 73.7 / 0% |
| 11 | w13 | 0.0 / 100% | 103.1 / 0% |
| 14 | w13 | 0.0 / 100% | 299.9 / 0% |

Every single one: winner at exactly 0.0 amplitude for 100% of the episode,
Family 2 real and fully present (0% zero frames) throughout. This is the
cleanest, most unambiguous confirmation in the dataset — the mechanism fully
accounts for this finding.

**The 5-way pileup `{Family 2, Family 6, Family 10, Family 11, Family 14}`**
(wave_ids 1, 5, 9, 10, 13), checked pair-by-pair across the post-fix run:

- Episodes before ~08:26 (0, 1, 5, 7, 8, 9, 13, 15, 16): clean
  `GHOST_WINS_OVER_ACTIVE` every time — w13 or w10/w12 (0.0 amp, 100% zero)
  beating w1, w5, w9, or w10 with real, fully-present signal.
- Episodes from ~08:28 onward (19, 20, 21, 23, 25): w13 stops being a pure
  ghost and starts carrying real-looking amplitude itself (73–306), still
  beating w1 and others. Episode 25 is notable: w13's mean amplitude
  (303.9) is **exactly equal** to w1's (303.9) — not close, identical — the
  strongest available evidence that by this point w13 isn't detecting
  anything of its own, it's reconstructing a duplicate of w1's real signal
  (consistent with `FAMILY13_INVESTIGATION.md`'s kernel-bleed mechanism) and
  the tie-break is now comparing a copy against its own source.

  This matters for the prevalence count below: a ghost that wins early
  collisions can end up "looking active" in later ones simply because
  winning earlier gave it real reconstructed frames to copy from — the bias
  is partly self-reinforcing, and a pure amplitude snapshot at any one
  episode can understate how much of the episode's outcome still traces
  back to the same root cause.

## Task 2 — Prevalence

| classification | count | % of 68 |
|---|---|---|
| `GHOST_WINS_OVER_ACTIVE` (frozen/zero winner over a real, present loser — the predicted bias) | 37 | 54% |
| `MIXED` (winner partially active/coasting, contesting a mix of ghosts and real waves — largely the same phenomenon in a self-reinforcing later stage, see above) | 15 | 22% |
| `BOTH_ACTIVE_AMBIGUOUS` (both sides real and present — genuinely contested, this hypothesis doesn't apply) | 10 | 15% |
| `BOTH_FROZEN` (nobody real involved — outcome is moot either way) | 3 | 4% |
| `ACTIVE_WINS_OVER_GHOST` (flip side — legitimate) | 3 | 4% |

By dataset, the pattern is consistent, not concentrated in one run:
pre-fix DOY 173: 12/16 (75%) clean `GHOST_WINS_OVER_ACTIVE`; post-fix DOY 173:
12/26 (46%) clean, most of the remainder in `MIXED`; DOY 145: 13/26 (50%)
clean. The mechanism is not an artifact of one particular night or of the
Voronoi fix (present in both pre- and post-fix runs, at comparable rates).

**Bottom line on prevalence:** a conservative count (clean cases only) puts
this mechanism behind just over half of all collision outcomes (54%). Including
the `MIXED` cases that case-by-case inspection shows are largely the same
mechanism in a later, self-reinforcing form (Task 1's pileup detail), the
true figure is likely closer to **60–70%**. Only 15% of episodes are cases
this hypothesis has nothing to say about (both sides genuinely real and
contested) — that residual is real and unexplained by this mechanism, and
would need separate investigation (possibly genuine ambiguity the tie-break
isn't equipped to resolve at all, real vs. real).

## Task 3 — Sanity-check the flip side

Found exactly 3 `ACTIVE_WINS_OVER_GHOST` cases, all clean:

| night | winner | winner amp/%zero | loser(s) |
|---|---|---|---|
| post-fix 173, ep 17 | w10 | 848.1 / 0% | w5 (0.0/100%), w9 (0.0/100%) |
| post-fix 173, ep 22 | w10 | 555.3 / 0% | w9 (0.0/100%) |
| 145, ep 21 | w4 | 79.3 / 0% | w5 (0.0/100%), w6 (0.0/100%) |

In every one, the winner is fully active (0% zero frames) and every loser is
a complete ghost (100% zero frames) — not a single borderline or partial case.
**No counterexample was found where a frozen/coasting wave legitimately
deserved to win against a genuinely active competitor**, and no case was found
where an active wave lost to a ghost through some other, more defensible
reason. Within the limits of this analysis (episode-level mean/%-zero, not
sub-episode frame-by-frame dynamics — a genuinely-real wave that coasts for
just 1–2 frames inside an otherwise-real episode wouldn't necessarily show up
distinctly here), **"frozen for the duration of a contested episode" appears
to reliably mean "not a real detection there," full stop** — there is no
evidence in these 68 episodes of a case where a brief, legitimate coast should
have been allowed to win. This suggests a fix could reasonably be simple and
categorical (a wave with near-zero/frozen signal over its episode is
ineligible to be judged primary) rather than needing a more delicate,
graduated treatment — though this is diagnosis, not a fix proposal, per scope.

## What this explains vs. what remains open

- **Family 2's 89% loss:** fully explained, no exceptions found.
- **The 5-way pileup:** explained for its early portion cleanly; its later
  portion (w13 "becoming" active) is still very likely the same root cause,
  just visible through the same axis-bleed mechanism `FAMILY13_INVESTIGATION.md`
  identified rather than through a pure zero-amplitude signature — the tie-
  break bias and the kernel-selectivity issue compound each other here rather
  than being fully independent findings.
- **Family 6's wavelength jump:** the originating case, one confirmed instance.
- **Unexplained residual:** ~15% of episodes (`BOTH_ACTIVE_AMBIGUOUS`) are
  contests between two waves that both have real, present signal — genuinely
  ambiguous by any criterion, and this investigation doesn't have evidence
  bearing on whether the tie-break resolves those correctly or not.
