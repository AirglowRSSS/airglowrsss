"""
resolution.py
-------------
One shared definition of when two wave components are "resolvable", used by
every grouping gate and every Gabor kernel in the pipeline.

The user specifies two numbers:

    dlam_frac  : fractional wavelength resolution (e.g. 0.20 = 20 %)
    dtheta_deg : azimuth resolution (degrees)

A finite image aperture L cannot resolve finer than its native limit. A 2-D
Fourier transform resolves wavenumber uniformly, dk ~ c/L (cycles/km), so in
the (ln lambda, theta) coordinates the spec is written in, the native cell
is square, with side c*lambda/L on both axes:

    dlam/lambda >= c * lambda / L          dtheta >= c * lambda / L  (rad)

The effective resolution at wavelength lambda is therefore

    dlam_eff(lambda)   = max(dlam_frac,            c * lambda / L)
    dtheta_eff(lambda) = max(radians(dtheta_deg),  c * lambda / L)

and, using the box rule, two components are the *same wave* when BOTH
their wavelength and azimuth separations are within the effective limit
(`components`: within the spec OR within the native cell, the latter measured
directly in k so it stays exact for large separations), i.e. they are
distinct if either separation exceeds its limit.

The same numbers set the Gabor kernels: each kernel's amplitude FWHM equals
the effective resolution on both axes (a critically-spaced filter bank --
adjacent resolvable waves cross at half amplitude, and a wave exactly one
resolution element away leaks ~6 % amplitude into its neighbour's kernel).
The native floor keeps kernels at least c times the native spectral width,
which bounds how far a kernel can pull a wave's estimated wavenumber toward
its own centre.

Stage-1 peak suppression uses the native cell itself (radius c/L in k, c/T in
omega): a peak inside a stronger peak's native cell cannot be a distinct wave
under any spec, and anything outside it is left for the spec-level grouping.

Two detection gates complete the spec:

    detection_snr : a peak must exceed this multiple of the local noise floor
        (the larger of two median / ln 2 estimates: the surrounding bins in
        (kx, ky, omega) outside the peak's own native cell -- a CFAR
        estimate -- and the in-passband bins on its |k| ring). Noise power
        per bin is exponential, so the false-alarm rate is set by this
        number alone, whatever the scene's brightness or contrast.
    leakage_ratio : a peak must also exceed this fraction of the window's
        strongest peak. This rejects window sidelobes of a strong wave that
        land more than one resolution element away; it sets the dynamic
        range (weakest resolvable amplitude = sqrt(leakage_ratio)).

A mirror gate then drops a wave that sits at the opposite k (same
wavelength, azimuth + 180, within one resolution element) of a stronger wave
in the same window with less than mirror_ratio of its power: the sidelobe of
the stronger wave's conjugate image, which would otherwise appear as a wave
travelling the opposite way.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import numpy as np


@dataclass(frozen=True)
class ResolutionSpec:
    """
    Resolution requested for wave grouping and Gabor reconstruction.

    dlam_frac   : fractional wavelength resolution (0.20 = 20 %).
    dtheta_deg  : azimuth resolution, degrees.
    native_c    : native-floor coefficient c in c * lambda / L. Default 1.5,
                  calibrated with noise-free equal-amplitude, equal-frequency
                  pairs on the MANGO grid (2026-10-17): c = 1 failed to
                  separate pairs at 1.25x the limit in both axes; pairs
                  bracket the true limit at c ~ 1.35-1.56.
    aperture_km : image aperture L, km. Set from the data with
                  `with_aperture`; required by everything that applies the
                  native floor.
    detection_snr : minimum peak power / local noise floor for a detection.
                  Default 30, calibrated on 640 windows of white-noise-only
                  MC scenes with the local (CFAR) floor (2026-10-19): the
                  strongest noise peak per window had median 18, max 26;
                  the fitted tail gives P(window > 30) ~ 1.5e-4, i.e. < 1 %
                  per 10-h night even before requiring >= 2 detections per
                  family, while waves at per-pixel SNR >= 0.03 sit at >= 90.
    leakage_ratio : minimum peak power / strongest peak in the window.
                  Default 0.05: on noise-free single waves the strongest
                  sidelobe more than one resolution element from the wave
                  was 3.4 % (Tukey-0.1 analysis taper), so 0.05 keeps a 1.5x
                  margin; amplitude dynamic range ~0.22.
    mirror_ratio : a wave within one resolution element of the exact
                  opposite (k -> -k: same wavelength, azimuth + 180) of a
                  stronger wave in the same window is dropped when its power
                  is below this fraction of that wave's. Default 0.2: waves
                  whose period is near or beyond the window length / FIR
                  high-pass leave a sidelobe of their conjugate image
                  (-k, -omega) at (-k, +omega); on noise-free MC waves (120
                  km, 100 min) these mirrors were 7-9 % of the wave, so 0.2
                  keeps a ~2x margin. A real counter-propagating wave at the
                  same wavelength is lost only if it carries < 20 % of the
                  power (amplitude < 0.45x).
    period_min, period_max : the analysed period band, minutes: full gain
                  of the temporal band-pass (temporal_filter). The omega-k
                  passband spans the band-pass's nonzero support
                  (`period_support`). period_min None = Nyquist
                  (2 x cadence). Default 20 min max: the study band (2-20
                  min, i.e. 4-20 min at 2-min cadence).
    period_rolloff : the band-pass gain falls to 0 along a raised cosine
                  at period_max x rolloff (and period_min / rolloff).
                  Default 2: ringing ~40 min for a 20-min edge, inside a
                  90-min analysis window.
    whiten_floor_by_band : set when the data were band-passed with this
                  spec's band (temporal_filter.bandpass_cube). Noise floors
                  and peak_snr are then computed on power / gain(omega)^2,
                  so the noise is flat across the band's edges: otherwise
                  the local floor's training bins reach into the
                  attenuated 20-40 min edge and underestimate the noise at
                  ~20 min (2 false families in 60 white-noise MC scenes,
                  vs 0 with the FIR the threshold was calibrated on).
    """
    dlam_frac: float = 0.20
    dtheta_deg: float = 22.5
    native_c: float = 1.5
    aperture_km: float | None = None
    detection_snr: float = 30.0
    leakage_ratio: float = 0.05
    mirror_ratio: float = 0.2
    period_min: float | None = None
    period_max: float = 20.0
    period_rolloff: float = 2.0
    whiten_floor_by_band: bool = False

    # ── construction ─────────────────────────────────────────────────────
    def with_aperture(self, east_km, north_km) -> "ResolutionSpec":
        """Copy with aperture_km = the smaller image extent (n * spacing)."""
        def extent(x):
            x = np.asarray(x, dtype=float)
            return len(x) * float(np.median(np.abs(np.diff(x))))
        return replace(self, aperture_km=min(extent(east_km), extent(north_km)))

    def _require_aperture(self) -> float:
        if self.aperture_km is None:
            raise ValueError("ResolutionSpec.aperture_km is not set -- call with_aperture() first")
        return self.aperture_km

    # ── effective resolution ─────────────────────────────────────────────
    def native(self, lam_km):
        """Native resolution c*lambda/L (fraction of lambda == radians)."""
        return self.native_c * np.asarray(lam_km, dtype=float) / self._require_aperture()

    def dlam_frac_eff(self, lam_km):
        """Effective fractional wavelength resolution at lam_km."""
        return np.maximum(self.dlam_frac, self.native(lam_km))

    def dtheta_deg_eff(self, lam_km):
        """Effective azimuth resolution (degrees) at lam_km."""
        return np.maximum(self.dtheta_deg, np.degrees(self.native(lam_km)))

    def native_dk_cyc_per_km(self) -> float:
        """Radius of the native resolution cell in k, cycles/km (c / L)."""
        return self.native_c / self._require_aperture()

    # ── the shared same-wave test ────────────────────────────────────────
    def components(self, lam1, az1, lam2, az2, axial: bool = False) -> tuple:
        """
        Per-axis normalised separations (d_lam, d_az) (broadcasts); 1 =
        exactly one effective resolution element. axial=True compares
        orientations modulo 180 deg instead of full-circle azimuths.

        Each axis is within its effective tolerance max(spec, native) when
        it is within EITHER tolerance, so each component is the smaller of

          spec   : |dlam| / mean(lam) / dlam_frac,   |daz| / dtheta_deg
          native : |k1 - k2| * L / c (radial),   2 sin(daz/2) * k_mean * L / c
                   (tangential chord), with k = 1/lambda in cycles/km

        For small separations the native term equals the fraction-of-lambda
        form c*lambda/L used by dlam_frac_eff/dtheta_deg_eff; measuring it
        in k keeps it exact for large separations, where a fraction of the
        mean wavelength would wrongly call e.g. 80 and 300 km the same.
        """
        L = self._require_aperture()
        lam1 = np.asarray(lam1, dtype=float)
        lam2 = np.asarray(lam2, dtype=float)
        lam_m = 0.5 * (lam1 + lam2)
        d_lam = np.minimum(np.abs(lam1 - lam2) / lam_m / self.dlam_frac,
                           np.abs(1.0 / lam1 - 1.0 / lam2) * L / self.native_c)
        period = 180.0 if axial else 360.0
        d = np.abs((np.asarray(az1, dtype=float) - np.asarray(az2, dtype=float)) % period)
        d = np.minimum(d, period - d)
        d_az = np.minimum(d / self.dtheta_deg,
                          2.0 * np.sin(np.radians(d) / 2.0) * (1.0 / lam_m) * L / self.native_c)
        return d_lam, d_az

    def distance(self, lam1, az1, lam2, az2, axial: bool = False):
        """
        Normalised box distance between components (broadcasts):
        max(d_lam, d_az) from `components`. <= 1 means the same wave.
        """
        return np.maximum(*self.components(lam1, az1, lam2, az2, axial=axial))

    def same_wave(self, lam1, az1, lam2, az2, axial: bool = False):
        """True where two components are within the effective resolution."""
        return self.distance(lam1, az1, lam2, az2, axial=axial) <= 1.0

    def pairwise_distance(self, lam, az, axial: bool = False) -> np.ndarray:
        """(n, n) matrix of `distance` between all components."""
        lam = np.asarray(lam, dtype=float)
        az = np.asarray(az, dtype=float)
        return self.distance(lam[:, None], az[:, None], lam[None, :], az[None, :], axial=axial)

    # ── Gabor kernels ────────────────────────────────────────────────────
    def gabor_bandwidths(self, lam_km: float) -> tuple:
        """
        (Bf, Btheta) for `cluster_reconstruction.create_gabor_kernel` such
        that the kernel's amplitude FWHM is dlam_eff * lambda in wavelength
        and dtheta_eff in azimuth (measured along the |k| = 1/lambda ring).

        Inverts create_gabor_kernel's actual frequency response, whose
        Gaussian envelope has sigma_x = s * (2^Bf+1)/(2^Bf-1) and
        sigma_y = s * cot(Btheta/2), with s = sqrt(ln2/2) / (2 pi f) px.
        """
        lam_km = float(lam_km)
        # Wavelength: closed form from wave_clustering.compute_bf_for_wavelength,
        # without its [0.2, 2] clamp -- the native floor already keeps the
        # kernel inside what the aperture supports.
        T = float(self.dlam_frac_eff(lam_km))
        rho = (np.sqrt(1.0 + T ** 2) - 1.0) / T
        Bf = float(np.log2((2.0 + rho) / (2.0 - rho)))

        # Azimuth: half amplitude at angle phi = dtheta_eff/2 on the ring,
        # where the offset from the kernel centre (f, 0) is
        # (f(cos phi - 1), f sin phi). In units of 2 pi f sigma:
        #   (sy * sin phi)^2 + (sx * (1 - cos phi))^2 = 2 ln 2
        phi = 0.5 * np.radians(float(self.dtheta_deg_eff(lam_km)))
        sx = np.sqrt(np.log(2) / 2) * (2 ** Bf + 1) / (2 ** Bf - 1)   # 2 pi f sigma_x
        rem = 2.0 * np.log(2) - (sx * (1.0 - np.cos(phi))) ** 2
        sy = np.sqrt(max(rem, 1e-12)) / np.sin(phi)                   # 2 pi f sigma_y
        Btheta = float(2.0 * np.arctan(np.sqrt(np.log(2) / 2) / sy))
        return Bf, Btheta

    def kernel_mismatch_limits(self, lam_km: float) -> tuple:
        """
        (half-FWHM azimuth in degrees, half-FWHM fractional wavelength):
        how far a tracked wave may sit from its kernel centre before it
        is outside the kernel's half-amplitude passband.
        """
        return float(self.dtheta_deg_eff(lam_km)) / 2.0, float(self.dlam_frac_eff(lam_km)) / 2.0

    # ── Stage-1 peak suppression footprint ───────────────────────────────
    def peak_footprint(self, n_kx_padded: int, dx_km: float, n_ky_padded: int, dy_km: float,
                       n_t: int, n_t_padded: int) -> np.ndarray:
        """
        Boolean (omega, ky, kx) ellipsoid footprint for maximum_filter: the
        native resolution cell, radius c/L in kx and ky (cycles/km) and
        c/T in omega, converted to FFT bins of the padded grid.
        """
        dk = self.native_dk_cyc_per_km()
        rx = dk * n_kx_padded * dx_km           # bins of 1/(n_pad*dx)
        ry = dk * n_ky_padded * dy_km
        rw = self.native_c * n_t_padded / n_t   # c/T in bins of 1/(n_t_pad*dt)
        nx, ny, nw = (int(np.floor(r)) for r in (rx, ry, rw))
        w, y, x = np.mgrid[-nw:nw + 1, -ny:ny + 1, -nx:nx + 1]
        return (w / max(rw, 1e-9)) ** 2 + (y / max(ry, 1e-9)) ** 2 + (x / max(rx, 1e-9)) ** 2 <= 1.0

    def period_band(self, dt_min: float) -> tuple:
        """(period_min, period_max) in minutes, period_min defaulting to Nyquist."""
        p_lo = 2.0 * dt_min if self.period_min is None else max(self.period_min, 2.0 * dt_min)
        return p_lo, self.period_max

    def period_support(self, dt_min: float) -> tuple:
        """
        Periods (minutes) where the band-pass gain is nonzero: the omega-k
        passband. Its hard edges then never clip a peak the band-pass let
        through -- a wave near period_max has a spectral peak ~c/T wide
        (0.017 cycles/min in a 90-min window, a third of 1/20 min).
        """
        p_lo, p_hi = self.period_band(dt_min)
        if p_lo > 2.0 * dt_min:
            p_lo = max(p_lo / self.period_rolloff, 2.0 * dt_min)
        return p_lo, p_hi * self.period_rolloff

    # ── reporting ────────────────────────────────────────────────────────
    def crossover_km(self) -> tuple:
        """Wavelengths above which the native floor overrides (lambda, theta)."""
        L = self._require_aperture()
        return (self.dlam_frac * L / self.native_c,
                np.radians(self.dtheta_deg) * L / self.native_c)

    def describe(self) -> str:
        s = (f"resolution: dlam={100 * self.dlam_frac:.0f}%, dtheta={self.dtheta_deg:g} deg, c={self.native_c:g}, "
             f"detection >= {self.detection_snr:g}x floor and >= {100 * self.leakage_ratio:g}% of strongest, "
             f"mirrors < {100 * self.mirror_ratio:g}% dropped, "
             f"periods {'Nyquist' if self.period_min is None else f'{self.period_min:g}'}-{self.period_max:g} min")
        if self.aperture_km is not None:
            lx, lt = self.crossover_km()
            s += (f", L={self.aperture_km:.0f} km (native floor above "
                  f"{lx:.0f} km in lambda, {lt:.0f} km in theta)")
        return s
