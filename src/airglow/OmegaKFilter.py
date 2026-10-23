"""
OmegaKFilter.py
----------------
The 3D (time, north, east) omega-k spectral filter: the pipeline's Stage 1
entry point. Detrends and windows a night's image cube, 3D-FFTs it into
(omega, ky, kx) space, and builds/applies a passband mask there (temporal
period, horizontal wavelength, and optionally a gravity-wave dispersion-
relation band) before inverse-transforming back to physical space.
Instantiated once per sliding analysis window and once on the full night
(the latter as an independent cross-validation reference).
"""

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from scipy import signal
from scipy.fft import fftn, ifftn, fftfreq, next_fast_len

class OmegaKFilterXarray:
    """
    Builds and applies a 3D omega-k passband filter to one night's (or one
    window's) image cube.
    """
    
    def __init__(self, ds, data_var='ImageData', zero_pad_factor=1, spatial_zero_pad_factor=1):
        """
        Parameters
        ----------
        ds : xarray.Dataset
            Dataset with dimensions (time, north, east)
        data_var : str
            Name of the data variable to analyze
        zero_pad_factor : int
            Zero-padding multiplier for the temporal axis.
            1 = no padding (original behavior)
            2 = pad to 2× length (doubles frequency bins)
            4 = pad to 4× length (quadruples frequency bins)
        spatial_zero_pad_factor : int
            Zero-padding multiplier for the north/east (kx, ky) axes.
            1 = no padding (original behavior; kx/ky bin spacing is fixed at
                2*pi/(domain size), so wave parameter estimates are quantized
                to whichever wavelength happens to tile the domain evenly).
            2 = pad to 2x length per spatial axis (halves the kx/ky bin
                spacing, ~4x the FFT memory/compute per call).
            Independent from zero_pad_factor because spatial padding is much
            more expensive (it multiplies two axes, not one).
        """
        self.ds = ds
        self.data_var = data_var
        self.zero_pad_factor = zero_pad_factor
        self.spatial_zero_pad_factor = spatial_zero_pad_factor
        
        # Extract coordinates
        self.time = ds.time.values
        self.east = ds.east.values
        self.north = ds.north.values
        
        # Calculate grid spacing (in meters)
        self.dx = np.abs(np.mean(np.diff(self.east))) * 1000  # km to m
        self.dy = np.abs(np.mean(np.diff(self.north))) * 1000
        
        # Calculate time step (in seconds)
        dt_ns = np.diff(self.time).astype('timedelta64[ns]').astype(float)
        self.dt = np.mean(dt_ns) / 1e9  # nanoseconds to seconds
        
        # Get dimensions
        self.nt = len(self.time)
        self.ny = len(self.north)
        self.nx = len(self.east)
        
        # Padded dimensions, rounded up to an FFT-efficient length (next_fast_len)
        # so a prime axis length (e.g. 193) doesn't force scipy into a slow
        # Bluestein-algorithm fallback for every padded multiple of it.
        self.nt_padded = next_fast_len(self.nt * self.zero_pad_factor)
        self.nx_padded = next_fast_len(self.nx * self.spatial_zero_pad_factor)
        self.ny_padded = next_fast_len(self.ny * self.spatial_zero_pad_factor)

        print(f"Grid spacing: dx={self.dx/1000:.2f} km, dy={self.dy/1000:.2f} km")
        print(f"Time step: {self.dt:.1f} seconds ({self.dt/60:.1f} minutes)")
        print(f"Dimensions: {self.nt} time × {self.ny} north × {self.nx} east")
        if self.zero_pad_factor > 1:
            print(f"Zero-padding: {self.zero_pad_factor}× → {self.nt_padded} temporal bins")
        if self.spatial_zero_pad_factor > 1:
            print(f"Spatial zero-padding: {self.spatial_zero_pad_factor}× → "
                  f"{self.ny_padded} north × {self.nx_padded} east bins")

        # Calculate frequency and wavenumber arrays
        # Use padded lengths so FFT bins are finer
        self.omega = 2 * np.pi * fftfreq(self.nt_padded, self.dt)  # rad/s
        self.kx = 2 * np.pi * fftfreq(self.nx_padded, self.dx)  # rad/m
        self.ky = 2 * np.pi * fftfreq(self.ny_padded, self.dy)  # rad/m
        
        # Create meshgrid
        self.Omega, self.Ky, self.Kx = np.meshgrid(self.omega, self.ky, self.kx, 
                                                     indexing='ij')
        
        # Calculate horizontal wavenumber magnitude
        self.Kh = np.sqrt(self.Kx**2 + self.Ky**2)
        
    def gravity_wave_dispersion(self, N=0.02, kz=None, H=7000):
        """
        Calculate theoretical gravity wave dispersion relation.
        
        Parameters
        ----------
        N : float
            Brunt-Väisälä frequency (rad/s), default ~0.02 rad/s for mesosphere
        kz : float or array
            Vertical wavenumber (rad/m). If None, assumes kz << kh
        H : float
            Scale height (m), default 7 km
            
        Returns
        -------
        omega_theory : array
            Theoretical frequency from dispersion relation
        """
        if kz is None:
            # Simplified dispersion for kz << kh
            omega_theory = N * self.Kh / np.sqrt(self.Kh**2 + (1/(4*H**2)))
        else:
            # Full dispersion relation
            omega_theory = N * self.Kh / np.sqrt(self.Kh**2 + kz**2 + (1/(4*H**2)))
            
        return omega_theory
    
    def create_filter(self, omega_min=None, omega_max=None, 
                     lambda_h_min=None, lambda_h_max=None,
                     period_min=None, period_max=None,
                     dispersion_width=0.3, N=0.02, apply_dispersion=True):
        """
        Create omega-k filter mask.
        
        Parameters
        ----------
        omega_min, omega_max : float
            Frequency range (rad/s)
        lambda_h_min, lambda_h_max : float
            Horizontal wavelength range (km)
        period_min, period_max : float
            Period range (minutes)
        dispersion_width : float
            Width of dispersion relation acceptance band (fraction)
        N : float
            Brunt-Väisälä frequency for dispersion relation
        apply_dispersion : bool
            If True, apply gravity wave dispersion relation filter. The
            pipeline's own default is False -- omega measured from the FFT
            is in the observer frame, while the dispersion relation is a
            statement about intrinsic frequency (they differ by k.U for a
            nonzero background wind); this filter stays off for real data
            until a wind (U) correction is available, not because of a bug.

        Returns
        -------
        filter_mask : xarray.DataArray
            3D filter mask in omega-k space
        """
        filter_mask = np.ones_like(self.Omega, dtype=float)
        
        # Frequency filtering
        if omega_min is not None:
            filter_mask[np.abs(self.Omega) < omega_min] = 0
        if omega_max is not None:
            filter_mask[np.abs(self.Omega) > omega_max] = 0
            
        # Period filtering (more intuitive)
        if period_min is not None:
            omega_max_period = 2 * np.pi / (period_min * 60)
            filter_mask[np.abs(self.Omega) > omega_max_period] = 0
        if period_max is not None:
            omega_min_period = 2 * np.pi / (period_max * 60)
            filter_mask[np.abs(self.Omega) < omega_min_period] = 0
            
        # Wavelength filtering
        if lambda_h_min is not None:
            kh_max = 2 * np.pi / (lambda_h_min * 1000)
            filter_mask[self.Kh > kh_max] = 0
        if lambda_h_max is not None:
            kh_min = 2 * np.pi / (lambda_h_max * 1000)
            filter_mask[self.Kh < kh_min] = 0
            
        # Dispersion relation filter
        if apply_dispersion:
            omega_theory = self.gravity_wave_dispersion(N=N)
            
            # Create band around dispersion curve
            deviation = np.abs(self.Omega) - omega_theory
            relative_deviation = np.abs(deviation) / (omega_theory + 1e-10)
            
            # Only pass points that satisfy dispersion AND are in valid region
            valid_region = (np.abs(self.Omega) > 1e-10) & (self.Kh > 1e-10)
            dispersion_mask = (relative_deviation < dispersion_width) & valid_region
            
            filter_mask *= dispersion_mask
        
        # Convert to xarray DataArray with proper coordinates
        filter_da = xr.DataArray(
            filter_mask,
            dims=['omega', 'ky', 'kx'],
            coords={
                'omega': self.omega,
                'kx': self.kx,
                'ky': self.ky
            },
            name='filter_mask'
        )
        
        return filter_da
    
    def apply_filter(self, filter_mask, return_fft=False, detrend_method='linear'):
        """
        Apply omega-k filter to data.
        
        Parameters
        ----------
        filter_mask : xarray.DataArray
            Filter mask from create_filter()
        return_fft : bool
            If True, also return filtered FFT
        detrend_method : str
            Method for detrending: 'linear', 'constant', or None
            
        Returns
        -------
        filtered_ds : xarray.Dataset
            Dataset with filtered data
        """
        # Extract data
        data = self.ds[self.data_var].values
        
        # Detrend data
        if detrend_method == 'linear':
            data_detrended = signal.detrend(data, axis=0, type='linear')
        elif detrend_method == 'constant':
            data_detrended = signal.detrend(data, axis=0, type='constant')
        else:
            data_detrended = data.copy()
        
        # Apply window to reduce edge effects
        window_t = signal.windows.tukey(self.nt, alpha=0.1)
        window_x = signal.windows.tukey(self.nx, alpha=0.1)
        window_y = signal.windows.tukey(self.ny, alpha=0.1)
        
        window_3d = window_t[:, None, None] * window_y[None, :, None] * window_x[None, None, :]
        data_windowed = data_detrended * window_3d
        
        # Zero-pad the temporal and/or spatial axes if requested
        pad_t = self.nt_padded - self.nt
        pad_y = self.ny_padded - self.ny
        pad_x = self.nx_padded - self.nx
        if pad_t > 0 or pad_y > 0 or pad_x > 0:
            data_windowed = np.pad(
                data_windowed,
                ((0, pad_t), (0, pad_y), (0, pad_x)),
                mode='constant',
                constant_values=0
            )

        # 3D FFT
        data_fft = fftn(data_windowed)

        # Apply filter
        filtered_fft = data_fft * filter_mask.values

        # Inverse FFT
        filtered_data_full = np.real(ifftn(filtered_fft))

        # Trim back to original temporal and spatial extent
        filtered_data = filtered_data_full[:self.nt, :self.ny, :self.nx]
        
        # Create output dataset
        filtered_ds = xr.Dataset(
            {
                self.data_var + '_filtered': (['time', 'north', 'east'], filtered_data),
                self.data_var + '_original': (['time', 'north', 'east'], data),
            },
            coords={
                'time': self.time,
                'north': self.north,
                'east': self.east
            }
        )
        
        # Copy attributes
        filtered_ds[self.data_var + '_filtered'].attrs = self.ds[self.data_var].attrs.copy()
        filtered_ds[self.data_var + '_filtered'].attrs['processing'] = 'omega-k filtered'
        
        if return_fft:
            # Also create FFT dataset
            fft_ds = xr.Dataset(
                {
                    'fft_filtered': (['omega', 'ky', 'kx'], filtered_fft),
                    'fft_original': (['omega', 'ky', 'kx'], data_fft),
                },
                coords={
                    'omega': filter_mask.omega,
                    'ky': filter_mask.ky,
                    'kx': filter_mask.kx
                }
            )
            return filtered_ds, fft_ds
        
        return filtered_ds
    
    
