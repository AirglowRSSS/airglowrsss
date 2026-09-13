"""
cluster_reconstruction.py
---------------------------
`create_gabor_kernel`: builds the complex Gabor kernel used everywhere a
wave is reconstructed in this pipeline (first pass, second pass, fixed and
adaptive kernel modes) -- the single most-used low-level function in the
whole reconstruction chain, called from `gabor_reconstruction.py`'s
`_reconstruct_fixed`/`_reconstruct_adaptive`.
"""

import numpy as np
import pandas as pd
import xarray as xr
from scipy import signal
from typing import Dict, List, Optional, Tuple
import warnings
import matplotlib.pyplot as plt
from airglow.coordinate_conversion_adapter import convert_angle
import logging




def create_gabor_kernel(wavelength_km: float, orientation_deg: float,
                       resolution_km: float, n_stds: float = 3.0,
                       Bf: float = 1.0, Btheta: float = np.pi/4):
    """
    Build a complex Gabor kernel tuned to one wave's wavelength/orientation.

    `Bf`/`Btheta` set the kernel's spatial-frequency/angular selectivity --
    too narrow and a genuinely-drifting wave's true wavelength or
    orientation falls outside the kernel's passband, biasing the
    reconstructed wavenumber back toward this kernel's own center
    wavelength/orientation rather than the wave's actual value; too wide
    and nearby distinct waves are not separated. See
    `wave_clustering.compute_bf_for_wavelength`/
    `compute_adaptive_gabor_parameters` for how these are actually chosen.

    Parameters:
    -----------
    wavelength_km : float
        Wavelength in km
    orientation_deg : float
        Orientation in degrees, compass-bearing convention (0=North,
        90=East) -- converted internally via `convert_angle` to the
        math-angle rotation this kernel's construction needs.
    resolution_km : float
        Spatial resolution (km/pixel)
    n_stds : float
        Number of standard deviations for kernel size
    Bf : float
        Frequency bandwidth parameter (octaves)
    Btheta : float
        Angular bandwidth parameter (radians)

    Returns:
    --------
    kernel : np.ndarray (complex)
        Gabor kernel
    """
    # Convert to normalized frequency (cycles per pixel)
    wavelength_pixels = wavelength_km / resolution_km
    frequency = 1.0 / wavelength_pixels
    
    # Compute standard deviations
    sigma_x = 1.0 / (2 * np.pi * frequency) * np.sqrt(np.log(2) / 2) * \
              (2**Bf + 1) / (2**Bf - 1)
    sigma_y = 1.0 / (2 * np.pi * frequency) * np.sqrt(np.log(2) / 2) * \
              (1 / np.tan(Btheta / 2))
    
    # Kernel size
    kernel_size = int(2 * np.ceil(n_stds * max(sigma_x, sigma_y))) + 1
    if kernel_size % 2 == 0:
        kernel_size += 1
    
    # Create meshgrid
    center = kernel_size // 2
    x = np.arange(kernel_size) - center
    y = np.arange(kernel_size) - center
    X, Y = np.meshgrid(x, y)
    
    # Rotate coordinates
    theta_rad = np.radians(convert_angle(orientation_deg))
    X_rot = X * np.cos(theta_rad) + Y * np.sin(theta_rad)
    Y_rot = -X * np.sin(theta_rad) + Y * np.cos(theta_rad)
    
    # Gaussian envelope
    gaussian = np.exp(-0.5 * (X_rot**2 / sigma_x**2 + Y_rot**2 / sigma_y**2))
    
    # Sinusoidal carrier
    sinusoid = np.exp(2j * np.pi * frequency * X_rot)
    
    # Gabor kernel
    kernel = gaussian * sinusoid
    
    return kernel


