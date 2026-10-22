"""Propagation sign and speed in final_estimation with per-frame lobe flips."""
import numpy as np

from airglow.final_estimation import resolve_wave_lobe, _smooth_episode, split_at_reversals, estimate_wave_final
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


def _reversing(n1, n2, direction_deg, period_min, noise, seed):
    """n1 frames toward direction_deg, then n2 frames toward direction_deg + 180."""
    a = _frames(direction_deg, period_min, n=n1, noise=noise, seed=seed)
    b = _frames(direction_deg + 180.0, period_min, n=n2, noise=noise, seed=seed + 1000)
    return tuple(np.concatenate([x, y]) for x, y in zip(a, b))


def test_split_at_mid_episode_reversal():
    for direction in (1.0, 135.0, 315.0):
        kx, ky, A = _reversing(37, 22, direction, 20.0, noise=0.5, seed=3)
        eps = split_at_reversals(kx, ky, A, [(0, len(A) - 1)])
        assert len(eps) == 2 and abs(eps[1][0] - 37) <= 2
        df = estimate_wave_final({"label": "w"}, kx, ky, A, np.arange(len(A)), DT,
                                 np.ones(len(A), bool), np.ones(len(A), bool))
        az = df["final_azimuth_deg"].to_numpy()
        assert np.nanmedian(full_circle_diff_deg(az[:30], direction)) < 10
        assert np.nanmedian(full_circle_diff_deg(az[45:], direction + 180.0)) < 10


def test_no_split_without_reversal():
    # steady noisy wave: false splits should be rare
    n_split = sum(len(split_at_reversals(*_frames(181.0, 30.0, n=150, noise=1.0, seed=s),
                                         [(0, 149)])) > 1 for s in range(200))
    assert n_split <= 4


def test_slow_wave_flagged_ambiguous():
    kx, ky, A = _frames(90.0, 20.0, n=60, lam_km=10.0)      # 10 km / 20 min = 8 m/s
    df = estimate_wave_final({"label": "w"}, kx, ky, A, np.arange(60), DT,
                             np.ones(60, bool), np.ones(60, bool))
    assert df.loc[df.valid, "direction_ambiguous"].all()
    kx, ky, A = _frames(90.0, 20.0, n=60, lam_km=60.0)      # 50 m/s, noise-free
    df = estimate_wave_final({"label": "w"}, kx, ky, A, np.arange(60), DT,
                             np.ones(60, bool), np.ones(60, bool))
    assert not df.loc[df.valid, "direction_ambiguous"].any()
