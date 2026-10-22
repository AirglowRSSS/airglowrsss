"""Tests for airglow.resolution and the gates that use it."""

import io
import contextlib

import numpy as np
import pytest

from airglow.resolution import ResolutionSpec
from airglow.cluster_reconstruction import create_gabor_kernel
from airglow.OmegaK_enhanced import identify_physical_waves

DX = 1.3
EAST = np.arange(166) * DX
NORTH = np.arange(174) * DX


@pytest.fixture
def spec():
    return ResolutionSpec(dlam_frac=0.20, dtheta_deg=22.5, native_c=1.0).with_aperture(EAST, NORTH)


def test_aperture_is_smaller_extent(spec):
    assert spec.aperture_km == pytest.approx(166 * DX)


def test_aperture_required():
    with pytest.raises(ValueError):
        ResolutionSpec().native(40.0)


def test_effective_resolution_native_floor(spec):
    L = spec.aperture_km
    # below both crossovers the spec applies
    assert spec.dlam_frac_eff(20.0) == pytest.approx(0.20)
    assert spec.dtheta_deg_eff(20.0) == pytest.approx(22.5)
    # above them the native c*lambda/L takes over, equal on both axes
    lam = 150.0
    assert spec.dlam_frac_eff(lam) == pytest.approx(lam / L)
    assert spec.dtheta_deg_eff(lam) == pytest.approx(np.degrees(lam / L))
    lx, lt = spec.crossover_km()
    assert spec.native(lx) == pytest.approx(0.20)
    assert np.degrees(spec.native(lt)) == pytest.approx(22.5)


def test_box_rule(spec):
    # within both limits -> same; either limit exceeded -> distinct
    assert spec.same_wave(30.0, 10.0, 33.0, 30.0)          # 9.5 %, 20 deg
    assert not spec.same_wave(30.0, 10.0, 30.0, 34.0)      # 24 deg
    assert not spec.same_wave(30.0, 10.0, 37.0, 10.0)      # 21 %
    # corner case the old ellipse/quadrature rules disagreed on
    assert spec.same_wave(30.0, 10.0, 35.0, 31.0)          # 15 %, 21 deg


def test_wrap_and_axial(spec):
    assert spec.same_wave(30.0, 355.0, 30.0, 5.0)
    assert not spec.same_wave(30.0, 10.0, 30.0, 190.0)              # counter-propagating
    assert spec.same_wave(30.0, 10.0, 30.0, 190.0, axial=True)      # same axis


def test_fractional_wavelength_is_scale_invariant(spec):
    # the same ratio is the same distance at any wavelength below the floor
    assert spec.distance(15.0, 0.0, 17.0, 0.0) == pytest.approx(spec.distance(25.0, 0.0, 25.0 * 17 / 15, 0.0))


def _kernel_fwhm(lam, Bf, Btheta, N=4096):
    k = create_gabor_kernel(lam, 0.0, DX, n_stds=4, Bf=Bf, Btheta=Btheta)
    K = np.abs(np.fft.fft2(k, (N, N)))
    f = np.fft.fftfreq(N, DX)
    i = np.unravel_index(K.argmax(), K.shape)
    k0 = np.hypot(f[i[0]], f[i[1]])
    th0 = np.arctan2(f[i[1]], f[i[0]])

    def val(kk, th):
        a = np.round(kk * np.cos(th) * N * DX).astype(int) % N
        b = np.round(kk * np.sin(th) * N * DX).astype(int) % N
        return K[a, b] / K.max()

    ph = np.radians(np.linspace(-89, 89, 3561))
    a = np.degrees(ph[val(np.full_like(ph, k0), th0 + ph) >= 0.5])
    kk = np.linspace(0.2 * k0, 3 * k0, 20000)
    l = 1 / kk[val(kk, np.full_like(kk, th0)) >= 0.5]
    return a.max() - a.min(), (l.max() - l.min()) / lam


@pytest.mark.parametrize("lam", [15.0, 40.0, 100.0])
def test_gabor_kernel_fwhm_matches_resolution(spec, lam):
    Bf, Btheta = spec.gabor_bandwidths(lam)
    a, lf = _kernel_fwhm(lam, Bf, Btheta)
    assert a == pytest.approx(float(spec.dtheta_deg_eff(lam)), rel=0.05)
    assert lf == pytest.approx(float(spec.dlam_frac_eff(lam)), rel=0.05)


def _n_waves(spec, peaks, snr=None):
    az, wl, per, w = map(np.array, zip(*peaks))
    snr = np.full(len(az), 1e3) if snr is None else np.asarray(snr, dtype=float)
    wp = dict(reliability=np.array(['good'] * len(az)), azimuths=az.astype(float),
              wavelengths=wl.astype(float), phase_speeds=wl * 1000 / (per * 60),
              periods=per.astype(float), weights=w.astype(float), peak_snr=snr, resolution=spec,
              window_min=88.0)
    with contextlib.redirect_stdout(io.StringIO()):
        return len(identify_physical_waves(wp))


def test_stage1_grouping_uses_spec(spec):
    assert _n_waves(spec, [(30, 40, 20, 1.0), (30, 40, 10, 0.3)]) == 1      # harmonic
    assert _n_waves(spec, [(30, 30, 9, 1.0), (50, 30, 9, 1.0)]) == 1        # 20 deg
    assert _n_waves(spec, [(30, 30, 9, 1.0), (55, 30, 9, 1.0)]) == 2        # 25 deg
    assert _n_waves(spec, [(30, 15, 9, 1.0), (30, 18.5, 9, 1.0),
                           (200, 100, 30, 1.0)]) == 3                         # 21 % at short lambda


def test_peak_footprint_is_native_cell(spec):
    fp = spec.peak_footprint(336, DX, 350, DX, 45, 225)
    # c/L in k with 2x zero-padding is ~2 bins; c/T with 5x padding is 5 bins
    assert fp.shape == (11, 5, 5)
    assert fp[5, 2, 2] and fp[0, 2, 2] and fp[5, 0, 2] and fp[5, 2, 0]
    assert not fp[0, 0, 0]


def test_native_distance_monotonic_for_large_separations():
    s = ResolutionSpec(dlam_frac=0.20, dtheta_deg=22.5, native_c=1.5).with_aperture(EAST, NORTH)
    d = [float(s.distance(80.0, 0.0, l2, 0.0)) for l2 in (100.0, 150.0, 300.0, 1000.0)]
    assert all(np.diff(d) > 0)
    # 80 vs 300 km: |dk| = 0.0092 cyc/km > c/L = 0.0069 -> distinct
    assert not s.same_wave(80.0, 0.0, 300.0, 0.0)


def test_small_separation_matches_effective_form(spec):
    # for small offsets the k-space native term equals c*lambda/L
    lam = 150.0
    f = float(spec.dlam_frac_eff(lam))
    l2 = lam * (1 + 0.01 * f)
    assert float(spec.distance(lam, 0.0, l2, 0.0)) == pytest.approx(0.01, rel=0.01)
    th = 0.1 * float(spec.dtheta_deg_eff(lam))
    assert float(spec.distance(lam, 0.0, lam, th)) == pytest.approx(0.1, rel=0.01)


def test_detection_gates(spec):
    far = [(30, 30, 9, 1.0), (150, 60, 12, 1.0)]                  # clearly distinct pair
    assert _n_waves(spec, far, snr=[100, 100]) == 2
    # below the noise-floor gate -> dropped
    assert _n_waves(spec, far, snr=[100, spec.detection_snr - 1]) == 1
    assert _n_waves(spec, far, snr=[1, 1]) == 0
    # leakage gate: a weak peak at a stronger peak's frequency is its sidelobe ...
    r = spec.leakage_ratio
    assert _n_waves(spec, [(30, 30, 9, 1.0), (150, 60, 9, 0.5 * r)]) == 1
    assert _n_waves(spec, [(30, 30, 9, 1.0), (150, 60, 9, 2 * r)]) == 2
    # ... as is one at its 3rd-harmonic wavenumber (either sign), at any frequency
    assert _n_waves(spec, [(30, 30, 9, 1.0), (30, 10, 5, 0.5 * r)]) == 1
    assert _n_waves(spec, [(30, 30, 9, 1.0), (210, 10, 5, 0.5 * r)]) == 1
    # elsewhere a weak peak stands down to leakage_floor x the strongest
    assert _n_waves(spec, [(30, 30, 9, 1.0), (150, 60, 12, 0.5 * r)]) == 2
    assert _n_waves(spec, [(30, 30, 9, 1.0), (150, 60, 12, 0.5 * spec.leakage_floor)]) == 1
    # an insignificant (noise) peak cannot act as a parent
    assert _n_waves(spec, [(30, 30, 9, 100.0), (150, 60, 9, 1.0)], snr=[1, 100]) == 1


def test_mirror_gate(spec):
    r = spec.mirror_ratio
    # weak wave at the opposite k of a strong one: a conjugate-image sidelobe
    assert _n_waves(spec, [(30, 40, 20, 1.0), (210, 40, 30, 0.5 * r)]) == 1
    assert _n_waves(spec, [(30, 40, 20, 1.0), (215, 44, 30, 0.5 * r)]) == 1   # within one element
    # strong enough to be a real counter-propagating wave
    assert _n_waves(spec, [(30, 40, 20, 1.0), (210, 40, 30, 2 * r)]) == 2
    # weak but not at the opposite k
    assert _n_waves(spec, [(30, 40, 20, 1.0), (150, 40, 30, 0.5 * r)]) == 2
    assert _n_waves(spec, [(30, 40, 20, 1.0), (210, 60, 30, 0.5 * r)]) == 2
