"""
temporal_filter.py
------------------
Zero-phase period band-pass for airglow image cubes, applied per pixel to a
whole night's time series in the frequency domain.

The gain is 1 inside [period_min, period_max] and falls to 0 along a
raised cosine in frequency: on the long-period side from period_max to
rolloff * period_max, on the short-period side from period_min to
period_min / rolloff (clipped at Nyquist; with period_min at or below the
Nyquist period, 2 * dt, there is no short-period edge). DC is always
removed.

Compared with a short FIR (e.g. 13 taps at 2-min cadence), the band edge
can be as sharp as the night's length allows, there is no phase shift, and
no frames are lost at the ends of the night. The edge width sets the time
response: ringing lasts ~ 1 / (edge width in frequency), ~40 min for a
20 -> 40 min edge, so the band-pass adds no smoothing beyond a 90-min
analysis window's own. The series is reflected at both ends before the FFT
so the ends of the night do not wrap into each other.
"""

import numpy as np
from scipy import fft as sp_fft


def bandpass_gain(freq_cpm, dt_min: float, period_min: float, period_max: float,
                  rolloff: float = 2.0) -> np.ndarray:
    """
    Gain at frequencies `freq_cpm` (cycles/min, >= 0).

    period_min, period_max : passband edges, minutes (full gain inside).
        period_min <= 2 * dt_min means no short-period edge (pass up to
        Nyquist).
    rolloff : > 1. The gain reaches 0 at rolloff * period_max on the long
        side and period_min / rolloff on the short side.
    """
    f = np.abs(np.asarray(freq_cpm, dtype=float))
    f_nyq = 0.5 / dt_min
    g = np.ones_like(f)

    f1, f0 = 1.0 / period_max, 1.0 / (rolloff * period_max)          # long-period edge
    g = np.where(f <= f0, 0.0, g)
    edge = (f > f0) & (f < f1)
    g = np.where(edge, 0.5 * (1.0 - np.cos(np.pi * (f - f0) / (f1 - f0))), g)

    if period_min > 2.0 * dt_min:                                       # short-period edge
        f2, f3 = 1.0 / period_min, min(rolloff / period_min, f_nyq)
        g = np.where(f >= f3, 0.0, g)
        edge = (f > f2) & (f < f3)
        g = np.where(edge, g * 0.5 * (1.0 + np.cos(np.pi * (f - f2) / (f3 - f2))), g)

    g = np.where(f == 0.0, 0.0, g)
    return g


def bandpass_cube(cube: np.ndarray, dt_min: float, period_min: float, period_max: float,
                  rolloff: float = 2.0, axis: int = 0, chunk_pixels: int = 20000) -> np.ndarray:
    """
    Band-pass every pixel's time series along `axis`.

    NaNs are filled with 0 for the transform and restored afterwards, so a
    pixel that is NaN in a frame stays NaN there (an always-NaN pixel, e.g.
    outside the field of view, stays NaN everywhere). Pixels are processed in
    chunks of `chunk_pixels` to bound memory on full-resolution nights.
    Returns float64, same shape as `cube`.
    """
    x = np.moveaxis(np.asarray(cube, dtype=float), axis, 0)
    n_t = x.shape[0]
    flat = x.reshape(n_t, -1)
    nan = ~np.isfinite(flat)

    # Reflect-pad by about two time-response lengths of the long-period edge.
    response_min = 1.0 / (1.0 / period_max - 1.0 / (rolloff * period_max))
    pad = int(min(n_t - 1, np.ceil(2.0 * response_min / dt_min)))
    n_fft = sp_fft.next_fast_len(n_t + 2 * pad, real=True)
    gain = bandpass_gain(sp_fft.rfftfreq(n_fft, d=dt_min), dt_min, period_min, period_max, rolloff)

    out = np.empty_like(flat)
    for c0 in range(0, flat.shape[1], chunk_pixels):
        seg = np.where(nan[:, c0:c0 + chunk_pixels], 0.0, flat[:, c0:c0 + chunk_pixels])
        seg = np.pad(seg, ((pad, pad), (0, 0)), mode='reflect')
        spec = sp_fft.rfft(seg, n=n_fft, axis=0)
        out[:, c0:c0 + chunk_pixels] = sp_fft.irfft(spec * gain[:, None], n=n_fft, axis=0)[pad:pad + n_t]
    out[nan] = np.nan
    return np.moveaxis(out.reshape(x.shape), 0, axis)
