"""
wave_clustering.py
-------------------
Derives Gabor kernel bandwidth parameters (Bf, Btheta) from the
wavelength/orientation clustering resolution actually in use, so the
reconstruction kernel neither over- nor under-resolves relative to how
detections are grouped into families. `compute_bf_for_wavelength`/
`compute_adaptive_gabor_parameters` are the two functions used by
`gabor_reconstruction.py`'s kernel construction.
"""

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from typing import Optional, Tuple




def compute_bf_for_wavelength(wavelength_resolution_km: float, wavelength_km: float) -> float:
    """
    Gabor frequency bandwidth (octaves) whose actual wavelength FWHM is
    ~wavelength_resolution_km AT wavelength_km specifically.

    Two corrections relative to a naive Bf = log2(1 + res/lambda):

    1. Wavelength-dependence: Bf is a *fractional* (octave) bandwidth, so a
       single Bf value computed at one reference wavelength gives an
       absolute km-wide selectivity that scales with wavelength -- correct
       only near that reference, several times too wide elsewhere. Calling
       this per wave/kernel with that wave's own wavelength_km fixes that
       part on its own.

    2. Calibration: create_gabor_kernel's sigma_x is a factor of 2 narrower
       (in frequency) than the standard Gabor/Daugman formula relating sigma
       to octave bandwidth via the half-amplitude frequency points, so a
       kernel built from the naive Bf actually achieves roughly 1.9-2.4x
       more wavelength bandwidth than the naive formula implies (verified
       empirically: measured FWHM matches this closed form to <1% wherever
       the clamp below isn't binding). Working through the exact frequency
       response of create_gabor_kernel's kernel and inverting it for the
       *achieved* FWHM instead of the naive approximation:

           rho = 2*(2**Bf - 1) / (2**Bf + 1)             # forward
           FWHM(lambda, Bf) = lambda * 2*rho / (1 - rho**2)

       Inverting for Bf given a target FWHM = wavelength_resolution_km:

           T   = wavelength_resolution_km / wavelength_km
           rho = (sqrt(1 + T**2) - 1) / T
           Bf  = log2((2 + rho) / (2 - rho))

    The [0.2, 2.0] clamp is a real, separate physical limit, not just safety
    margin: achieving a tight wavelength_resolution_km at long wavelengths
    needs a kernel many times larger than a typical MANGO domain (e.g. an
    unclamped Bf for a 116 km wave at 20 km resolution needs an ~1150 px
    kernel against a ~170 px domain). Below the clamp, the true FWHM will
    exceed wavelength_resolution_km -- Bf can't be pushed low enough to
    compensate without a domain-size-aware redesign of the clamp itself,
    which this does not attempt.
    """
    T = wavelength_resolution_km / wavelength_km
    rho = (np.sqrt(1 + T ** 2) - 1) / T
    Bf = np.log2((2 + rho) / (2 - rho))
    return float(np.clip(Bf, 0.2, 2.0))


def compute_adaptive_gabor_parameters(
    wavelength_resolution_km: float,
    orientation_resolution_deg: float,
    wavelength_km: float,
    resolution_km_per_pixel: float = None,
    verbose: bool = False
) -> Tuple[float, float]:
    """
    Compute optimal Gabor filter parameters based on clustering resolution

    The Gabor filter bandwidths should match the clustering resolution to avoid
    over-resolving (which would detect the sub-clusters as separate waves).

    Parameters:
    -----------
    wavelength_resolution_km : float
        Wavelength clustering resolution (km)
    orientation_resolution_deg : float
        Orientation clustering resolution (degrees)
    wavelength_km : float
        Wavelength of the wave this Gabor kernel is being built for. Bf is
        computed (see compute_bf_for_wavelength) so the kernel's wavelength
        bandwidth is ~wavelength_resolution_km AT this specific wavelength,
        not just near some fixed reference scale.
    resolution_km_per_pixel : float, optional
        Unused (kept for backward compatibility).
    verbose : bool
        Print calculated parameters

    Returns:
    --------
    Bf : float
        Frequency bandwidth parameter (octaves)
    Btheta : float
        Angular bandwidth parameter (radians)
    """

    # Bf (frequency bandwidth in octaves)
    # Relates to fractional wavelength resolution: Δλ/λ ≈ 2^Bf - 1
    # For wavelength resolution of ±Δλ around λ:
    # Bf = log2(1 + Δλ/λ)
    Bf = compute_bf_for_wavelength(wavelength_resolution_km, wavelength_km)

    # Btheta (angular bandwidth in radians) -- wavelength-independent
    # The angular resolution in radians
    Btheta = 2 * np.radians(orientation_resolution_deg)

    # Clamp to reasonable range
    Btheta = np.clip(Btheta, np.radians(10), np.radians(45))

    if verbose:
        print(f"\n{'='*70}")
        print("ADAPTIVE GABOR PARAMETERS")
        print(f"{'='*70}")
        print(f"Based on clustering resolution:")
        print(f"  Wavelength resolution: {wavelength_resolution_km} km  (at λ={wavelength_km:.1f} km)")
        print(f"  Orientation resolution: {orientation_resolution_deg}°")
        print(f"\nCalculated Gabor parameters:")
        print(f"  Bf (frequency bandwidth): {Bf:.3f} octaves")
        print(f"  Btheta (angular bandwidth): {Btheta:.3f} rad ({np.degrees(Btheta):.1f}°)")
        print(f"\nThis means:")
        print(f"  Wavelength half-width: ~{wavelength_km * (2**Bf - 1):.1f} km at λ={wavelength_km:.1f}km")
        print(f"  Orientation half-width: ~{np.degrees(Btheta/2):.1f}°")

    return Bf, Btheta






# Example usage
#
# This used to demonstrate a clustering/reconstruction/animation workflow
# (cluster_refined_waves, plot_clustering_results, export_clustered_catalog,
# RefinedWavefieldReconstructor, refined_animation.create_refined_wavefield_animation)
# that was abandoned before either the pre-refactor or the current five-stage
# pipeline existed -- none of those names exist anywhere in this codebase.
# See PIPELINE_AUDIT_RESULTS.md / PIPELINE_CLEANUP.md. The functions that
# actually remain live in this file are compute_adaptive_gabor_parameters
# and compute_bf_for_wavelength, demonstrated below instead.
if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*70)
    print("EXAMPLE USAGE")
    print("="*70)

    wavelength_km = 40.0
    Bf, Btheta = compute_adaptive_gabor_parameters(
        wavelength_resolution_km=15.0,
        orientation_resolution_deg=15.0,
        wavelength_km=wavelength_km,
        verbose=True,
    )
    print(f"\nBf={Bf:.3f} octaves, Btheta={Btheta:.3f} rad "
          f"(wavelength_km={wavelength_km})")

    bf_only = compute_bf_for_wavelength(
        wavelength_resolution_km=15.0,
        wavelength_km=wavelength_km,
    )
    print(f"compute_bf_for_wavelength alone: Bf={bf_only:.3f} octaves "
          f"(should match Bf above).")