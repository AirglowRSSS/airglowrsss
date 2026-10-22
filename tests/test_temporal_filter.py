"""Zero-phase period band-pass (airglow.temporal_filter)."""
import numpy as np

from airglow.temporal_filter import bandpass_cube, bandpass_gain
from airglow.resolution import ResolutionSpec

DT = 2.0


def _gain_phase(period, **kw):
    t = np.arange(0, 240, DT)
    x = np.cos(2 * np.pi * t / period)
    y = bandpass_cube(x[:, None], DT, axis=0, **kw)[:, 0]
    mid = slice(30, -30)
    z = np.exp(-2j * np.pi * t[mid] / period)
    c = np.sum(y[mid] * z) / np.sum(x[mid] * z)
    return abs(c), np.angle(c)


def test_passband_unity_and_zero_phase():
    for p in (4.5, 8.0, 15.0, 20.0):
        g, ph = _gain_phase(p, period_min=2 * DT, period_max=20.0)
        assert abs(g - 1) < 0.01 and abs(ph) < 0.01


def test_stopband_and_rolloff():
    assert _gain_phase(60.0, period_min=2 * DT, period_max=20.0)[0] < 0.01
    assert abs(bandpass_gain(1 / 30.0, DT, 2 * DT, 20.0) - 0.25) < 1e-9   # raised cosine, 20 -> 40 min
    assert bandpass_gain(0.0, DT, 2 * DT, 20.0) == 0.0
    # short-period edge when period_min is above Nyquist: 0 at period_min / rolloff
    assert bandpass_gain(1 / 4.0, DT, 8.0, 20.0) == 0.0
    assert 0.0 < bandpass_gain(1 / 5.0, DT, 8.0, 20.0) < 1.0
    assert bandpass_gain(1 / 10.0, DT, 8.0, 20.0) == 1.0


def test_nan_preserved_and_axis():
    x = np.random.default_rng(0).normal(size=(3, 4, 120))
    x[0, 0, :] = np.nan
    x[1, 1, 7] = np.nan
    y = bandpass_cube(x, DT, 4.0, 20.0, axis=2)
    assert np.isnan(y[0, 0]).all() and np.isnan(y[1, 1, 7]) and np.isnan(y).sum() == 121


def test_spec_band_and_support():
    spec = ResolutionSpec()
    assert spec.period_band(DT) == (4.0, 20.0)
    assert spec.period_support(DT) == (4.0, 40.0)
    assert ResolutionSpec(period_min=8.0).period_support(DT) == (4.0, 40.0)
