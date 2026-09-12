"""
Cluster-Based Wavefield Reconstruction

Reconstructs wavefields using cluster centroids rather than individual detections.
Each cluster gets a single Gabor filter based on its representative parameters.
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
    Create Gabor kernel for specific wave parameters
    
    Parameters:
    -----------
    wavelength_km : float
        Wavelength in km
    orientation_deg : float
        Orientation in degrees
    resolution_km : float
        Spatial resolution (km/pixel)
    n_stds : float
        Number of standard deviations for kernel size
    Bf : float
        Frequency bandwidth parameter
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
#    logging.info('cluster_reconstructor convert_angle')
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


