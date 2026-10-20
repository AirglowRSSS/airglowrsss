"""Propagation sign and speed in final_estimation with per-frame lobe flips."""
import numpy as np

from airglow.final_estimation import resolve_wave_lobe, _smooth_episode
from airglow.angles import full_circle_diff_deg

DT = 120.0


def _frames(direction_deg, period_min, n=80, lam_km=60.0, noise=0.0, seed=0):
    """Second-pass inputs for a wave travelling toward direction_deg whose
    kernel lobe alternates between the two ends of its axis, as kernel
    tuning does near the 0/180 boundary. A positive phase step means travel
    toward lobe + 180."""
    rng = np.random.default_rng(seed)
    k = 2 * np.pi / (lam_km * 1000.0)
    toward = rng.random(n) < 0.5                          # lobe = direction + 180
    lobe = np.deg2rad(direction_deg + np.where(toward, 180.0, 0.0) + rng.normal(0, 2, n))
    kx, ky = -np.sin(lobe) * k, -np.cos(lobe) * k
    step = 2 * np.pi * DT / (period_min * 60.0)
    phase = np.where(toward, step, -step) + noise * rng.normal(size=n)
    return kx, ky, np.exp(1j * phase)


def test_sign_survives_lobe_flips_near_axis_boundary():
    for direction in (1.0, 181.0, 359.0, 90.0):
        kx, ky, A = _frames(direction, period_min=20.0)
        lobe = resolve_wave_lobe(kx, ky, A, [(0, len(A) - 1)])
        assert full_circle_diff_deg(lobe["wave_azimuth_deg"], direction) < 5.0
        assert lobe["wave_sign_z"] > 10


def test_speed_uses_signed_aligned_step():
    # noisy long-period wave: abs() of the step would bias omega high
    period, lam = 50.0, 60.0
    kx, ky, A = _frames(181.0, period, n=400, lam_km=lam, noise=1.0, seed=1)
    sm = _smooth_episode(kx, ky, A, DT, 0, len(A) - 1, 181.0, 1)
    true_speed = lam * 1000.0 / (period * 60.0)
    assert abs(np.nanmedian(sm["speed_ms"]) - true_speed) < 0.25 * true_speed
