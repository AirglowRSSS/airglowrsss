# Investigation: ω-k / `kf_omega_rads` sign disagreement

Diagnosis only — no code changed as part of this investigation, per Task 1's scope.

## Datasets

1. **DOY 173, 2023, LOW** (primary, already in hand). 14 families, 12 with valid frames.
2. **DOY 145, 2022, LOW** (second dataset, run fresh for this investigation — freshly
   processed with the current, already-fixed pipeline; every other dataset from earlier
   sessions predates the `convert_angle` fix and would not be valid evidence here). 10
   families, all 10 with valid frames.

Both processed with identical parameters (`dir_tol_deg=22.5`, `lambda_tol_km=20.0`,
`kf_collision_corr_threshold=0.7`, serial execution), differing only in input night.

## Headline finding: this is not noise

Before going through the requested checks individually, the single most important
pattern in the data is this: **`frac_matching` is bimodal, not graded.**

DOY 173 flagged families: 0.000, 0.000, 0.029, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000
(9 families). Agreeing families: 1.000, 1.000, 1.000 (3 families with data).

DOY 145 flagged families: 0.000 × 9. Agreeing: 1.000 × 1.

Every single family in both nights is at (or within 3% of) 0% or 100% match — nothing
in between. If `kf_omega_rads`'s sign were being pushed around by genuine per-frame
noise, individual families should show a *spread* of `frac_matching` values (a
weakly-tracked wave might agree 55% of frames, a strongly-tracked one 95%, etc.),
especially since `frac_matching` is computed only over `innovation_valid` frames —
i.e., frames that already passed the KF's own chi²/SNR gate, which should filter out
the noisiest observations before this vote is even taken. A clean per-family bimodal
split instead means: for a given family, essentially every valid frame agrees, or
essentially every valid frame disagrees. That is the signature of something
**deterministic and fixed per family** (decided once, at or before detection),
not a frame-by-frame noise effect. This directly bears on all five checks below: I
was checking for correlates of a *systematic* effect, not trying to characterize
noise.

Also notable: disagreement is the **majority case** in both datasets (9/12 = 75% of
DOY 173 families with data; 9/10 = 90% of DOY 145 families). This is not a rare edge
case being caught by the diagnostic; it's the dominant outcome. That by itself argues
against "an occasional bad detection" and toward either a very common real-world
scenario the sign convention doesn't handle, or a bug that manifests most of the time
rather than some of the time.

## Check 1: detected period near the filter passband edge

DOY 173 — flagged families' median periods (min): 18.89, 5.61, 27.76, 16.14, 11.27,
12.02, 36.44, 32.29, 8.89. Agreeing: 73.20, 10.59, 44.85.

DOY 145 — flagged: 11.06, 25.31, 15.36, 14.85, 27.13, 23.20, 23.09, 15.93, 56.19.
Agreeing: 88.00.

No clean separation. Two flagged families sit near/below the ~8 min recommended
minimum (DOY173 wave 2 at 5.61 min, wave 11 at 8.89 min), but the majority of flagged
families (period 11–36 min) are nowhere near either passband edge, and the single
agreeing family in DOY 145 (88.00 min) is itself well outside the nominal ≤60 min
upper range mentioned in the task, same as DOY 173's own agreeing wave 1 (73.20 min).
If anything, the agreeing families skew toward *longer* periods in both nights, which
is the opposite of what an aliasing/leakage-at-short-period explanation would predict
for disagreement. **Not a clean discriminator; weakly suggestive in the wrong
direction if anything.**

## Check 2: wavelength regime (short-λ aliasing risk)

DOY 173 — flagged median λ (km): 59, 32, 127, 86, 65, 54, 147, 106, 34. Agreeing: 143,
38, 75.

DOY 145 — flagged: 42, 113, 58, 68, 127, 106, 112, 64, 141. Agreeing: 98.

**No family in either dataset has λ ≲ 20 km** — the short-wavelength aliasing risk
zone the task asked me to check is simply not populated by any detection in this data,
flagged or not. This specific mechanism is ruled out for both datasets as currently
configured (it cannot explain disagreement here because no family is in the risk
regime to begin with).

## Check 3: SNR / amplitude / confidence

DOY 173 — flagged mean amplitude: 1440, 937, 62, 307, 468, 405, 87, 397, 3946 (huge
spread, 62 to ~4000). Agreeing: 159, 2704, 204 (also spread).

DOY 145 — flagged: 3814, 327, 1899, 1986, 424, 409, 720, 177, 163. Agreeing: 91 (the
*lowest* amplitude value in the entire DOY 145 dataset).

Mean activity fraction shows the same pattern — both flagged and agreeing groups span
low and high values in both nights, with no threshold separating them. If anything,
DOY 145's one agreeing family has the *weakest* signal of any family in that dataset,
which is the opposite of "only strong, unambiguous detections agree." **Ruled out as a
clean discriminator.**

## Check 4: orientation quadrant

This is the most interesting of the five checks, though it does not resolve cleanly.

DOY 173 — agreeing families' `orientation_deg` (axis): 17.4°, 0.0°, 19.1° — **all three
below 20°.** Flagged families: 164.9°, 14.1°, 54.7°, 20.7°, 117.9°, 28.3°, 109.5°,
166.7°, 158.3° — a mix spanning the full [0°,180°) range, including several *below*
20° (14.1°, 20.7°) that still disagree.

DOY 145 — the one agreeing family is at 25.2°. All 9 flagged families are at 75.1° or
above (75.1°, 90.0°, 104.9°, 117.9°, 125.3°, 133.1°, 136.7°, 141.8°, 162.6°).

Combined picture: **every agreeing family in both datasets (4 total) has
orientation_deg ≤ 25.2°.** But the relationship is not sufficient in the other
direction — DOY 173 has flagged families as low as 14.1° and 20.7°, well inside the
range where DOY 173's own agreeing families sit. So low orientation looks
*necessary but not sufficient* for agreement, at least in this small sample (n=16
families with data across both nights, only 4 agreeing). This is not a hard 90°
cutoff (ruling out a simple residual of the already-fixed `convert_angle` bug, which
broke exactly at 90°) — but it is also not nothing. With only 4 agreeing families
total, I don't think this is strong enough evidence to call it a confirmed pattern;
it's the strongest lead among the five checks, but a larger sample (more nights)
would be needed to know whether "near-zero orientation" is really predictive or
whether DOY 173's two low-orientation flagged families (14.1°, 20.7°) mean it isn't.

## Check 5: time of night / activity fraction

DOY 173 flagged families' time spans range from as early as 05:56 to as late as
10:14 (the dataset's own start/end bounds), same as agreeing families (05:56–10:14 for
wave 1). DOY 145 shows the same pattern (04:24–09:36 bounds shared by both flagged and
the one agreeing family). No clustering near dawn/dusk moon-masking boundaries or any
other time-of-night structure — flagged and agreeing families both simply span
whatever portion of the night their underlying wave was active. **Ruled out.**

## A candidate mechanism I checked and ruled out: FFT axis ordering

Given the dominant, systematic (not noise-like) nature of the disagreement, I checked
whether `OmegaKFilterXarray`'s (kx, ky, ω) meshgrid construction
(`OmegaKFilter.py:81-82`) could have its axes swapped relative to what the later
peak-detection loop in `OmegaK_enhanced.py:394-403` assumes (`significant_points[i,j,k]`
indexed as `[omega, ky, kx]`). This would be exactly the kind of majority-case,
deterministic bug the data pattern suggests. **On inspection, this is not the bug**:
the meshgrid call explicitly passes `indexing='ij'`
(`self.Omega, self.Ky, self.Kx = np.meshgrid(self.omega, self.ky, self.kx,
indexing='ij')`), which produces arrays with axis order exactly matching input order
(axis 0 = omega, axis 1 = ky, axis 2 = kx) — consistent with, not contradicting, the
peak-detection loop's indexing assumption. I want to flag that my first pass at this
check was wrong: an initial `grep` truncated the meshgrid call at line 81 and did not
show the `indexing='ij'` argument on line 82, and I nearly reported a false lead before
reading the actual source in full. Recorded here so this specific hypothesis doesn't
get re-investigated from scratch later — it's checked and ruled out, not unexamined.

## Leading hypothesis

**No single mechanism among the five requested checks (period, wavelength, SNR,
quadrant, time-of-night) cleanly explains the disagreement, and the FFT-axis-ordering
hypothesis is ruled out.** The evidence is strong that this is *not* physical or
statistical noise — the clean per-family bimodal split and the fact that disagreement
is the majority outcome in both nights both argue for something systematic. Of the
five checks, orientation quadrant is the only one with a real (if incomplete) signal:
every agreeing family in both datasets has a low orientation_deg (≤25.2°), though low
orientation does not guarantee agreement.

Given that `wave['azimuth']`'s sign is established once, at initial ω-k detection, by
`identify_physical_waves`/`arctan2(-kx, -ky)` on the FFT-peak-picked (kx, ky) — and that
this is a *different* code path from the (now-verified-correct) `create_gabor_kernel`/
`kf_omega_rads` chain, sharing only the same underlying real image data — my honest
best guess is that the discrepancy originates somewhere in that original detection path
rather than in the KF/reconstruction side (which has now been independently validated
against synthetic ground truth across all four quadrants using the real, deployed
function). But I was not able to isolate the specific line or mechanism responsible
within this investigation's scope, and the meshgrid check above shows that guessing at
a specific culprit without reading the full, real code path first produces false leads.
I'd want to either instrument the real `OmegaKFilterXarray`/`analyze_wave_parameters_
with_aliasing` path directly with a synthetic wave of known ground-truth direction (the
same technique used to validate `create_gabor_kernel`), or trace a specific
disagreeing family's actual (kx, ky) values by hand through `identify_physical_waves`,
before I'd be confident naming an exact bug. Both are natural next steps but are
implementation/further-diagnosis work beyond what this pass covers.

## What this means for Task 2 (final output design)

Whatever the mechanism, its practical effect right now is that `kf_prop_azimuth_deg` —
the fixed, whole-night azimuth label — disagrees with the wave's own independently
re-measured `kf_omega_rads` sign for the *majority* of families on both nights checked.
Since the reverted auto-correction demonstrated that "trust `kf_omega_rads`'s sign"
is not safe either, neither value can currently be treated as a reliable source of
truth for final direction on its own. This should be treated as an open blocker on
any final-output schema that reports a single, authoritative propagation direction per
wave until root-caused.
