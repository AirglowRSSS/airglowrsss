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


def circular_mean_0_180(angles_deg: np.ndarray, weights: Optional[np.ndarray] = None) -> float:
    """
    Compute circular mean for angles in [0, 180) range
    
    Handles the fact that 0° and 180° represent the same wave axis.
    """
    if len(angles_deg) == 0:
        return np.nan
    
    # Convert to radians and double (for 180° periodicity)
    angles_rad = np.radians(angles_deg)
    angles_doubled = 2 * angles_rad
    
    # Compute weighted circular mean
    if weights is not None:
        sin_mean = np.average(np.sin(angles_doubled), weights=weights)
        cos_mean = np.average(np.cos(angles_doubled), weights=weights)
    else:
        sin_mean = np.mean(np.sin(angles_doubled))
        cos_mean = np.mean(np.cos(angles_doubled))
    
    # Compute mean angle
    mean_doubled = np.arctan2(sin_mean, cos_mean)
    mean_angle = np.degrees(mean_doubled / 2)
    
    # Ensure in [0, 180) range
    if mean_angle < 0:
        mean_angle += 180
    if mean_angle >= 180:
        mean_angle -= 180
    
    return mean_angle


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


class ClusterWavefieldReconstructor:
    """
    Reconstruct wavefield using cluster-level parameters
    
    Each cluster is represented by its centroid parameters (mean wavelength,
    mean orientation) binned to specified resolution.
    """
    
    def __init__(self, analyzer, clustered_df: pd.DataFrame,
                 wavelength_bin_km: float = 15.0,
                 orientation_bin_deg: float = 15.0,
                 n_stds: float = 3.0,
                 Bf: float = 1.0,
                 Btheta: float = np.pi/4,
                 max_workers: int = 48,
                 verbose = False):
        """
        Initialize cluster-based reconstructor
        
        Parameters:
        -----------
        analyzer : AirglowWaveAnalyzer
            Analyzer with loaded data
        clustered_df : pd.DataFrame
            DataFrame with columns: frame, cluster_id, wavelength_km, orientation_deg, power (or energy)
        wavelength_bin_km : float
            Bin size for wavelength (km)
        orientation_bin_deg : float
            Bin size for orientation (degrees)
        n_stds : float
            Number of standard deviations for Gabor kernel
        Bf : float
            Frequency bandwidth parameter
        Btheta : float
            Angular bandwidth parameter (radians)
        max_workers : int
            Maximum parallel workers
        """
        self.analyzer = analyzer
        self.clustered_df = clustered_df.copy()
        self.wavelength_bin_km = wavelength_bin_km
        self.orientation_bin_deg = orientation_bin_deg
        self.n_stds = n_stds
        self.Bf = Bf
        self.Btheta = Btheta
        self.max_workers = max_workers
        
        # Check which column name is used for power/energy
        if 'energy' in self.clustered_df.columns:
            self.power_col = 'energy'
        elif 'power' in self.clustered_df.columns:
            self.power_col = 'power'
        else:
            raise ValueError("DataFrame must have either 'power' or 'energy' column")
        
        # Check for frame_idx or frame column
        if 'frame_idx' in self.clustered_df.columns:
            self.frame_col = 'frame_idx'
        elif 'frame' in self.clustered_df.columns:
            self.frame_col = 'frame'
        else:
            raise ValueError("DataFrame must have either 'frame' or 'frame_idx' column")
        
        # Get unique frames and clusters
        self.frame_indices = sorted(self.clustered_df[self.frame_col].unique())
        self.cluster_ids = sorted(self.clustered_df['cluster_id'].unique())
        
        # Compute cluster centroids (binned parameters)
        self._compute_cluster_centroids()

        if verbose:
            print(f"Initialized ClusterWavefieldReconstructor")
            print(f"  Frames: {len(self.frame_indices)}")
            print(f"  Clusters: {len(self.cluster_ids)}")
            print(f"  Unique waves: {len(self.wave_params)}")
            print(f"  Wavelength bins: {wavelength_bin_km} km")
            print(f"  Orientation bins: {orientation_bin_deg}°")
    
    def _compute_cluster_centroids(self, verbose: bool = False):
        """
        Compute representative parameters for each cluster
        
        For each cluster, compute:
        - Mean wavelength (binned)
        - Mean orientation (binned, using circular statistics)
        - Mean power (for weighting)
        """
        self.cluster_centroids = {}
        self.wave_params = {}  # Unique (wavelength, orientation) pairs
        self.wave_ids = []  # List of unique wave IDs
        
        wave_id_counter = 0
        
        for cluster_id in self.cluster_ids:
            cluster_data = self.clustered_df[
                self.clustered_df['cluster_id'] == cluster_id
            ]
            
            # Compute circular mean orientation
            orientations = cluster_data['orientation_deg'].values
            powers = cluster_data[self.power_col].values
            
            mean_orientation = circular_mean_0_180(orientations, weights=powers)
            mean_wavelength = np.average(cluster_data['wavelength_km'].values, 
                                        weights=powers)
            mean_power = np.mean(powers)
            
            # Bin the parameters
            binned_wavelength = np.round(mean_wavelength / self.wavelength_bin_km) * self.wavelength_bin_km
            binned_orientation = np.round(mean_orientation / self.orientation_bin_deg) * self.orientation_bin_deg
            
            # Ensure orientation is in [0, 180)
            binned_orientation = binned_orientation % 180
            
            # Create unique wave key
            wave_key = (binned_wavelength, binned_orientation)
            
            # Assign wave ID
            if wave_key not in self.wave_params:
                self.wave_params[wave_key] = {
                    'wave_id': wave_id_counter,
                    'wavelength_km': binned_wavelength,
                    'orientation_deg': binned_orientation,
                    'clusters': []
                }
                self.wave_ids.append(wave_id_counter)
                wave_id_counter += 1
            
            # Store cluster info
            wave_id = self.wave_params[wave_key]['wave_id']
            self.wave_params[wave_key]['clusters'].append(cluster_id)
            
            self.cluster_centroids[cluster_id] = {
                'wave_id': wave_id,
                'wavelength_km': binned_wavelength,
                'orientation_deg': binned_orientation,
                'mean_power': mean_power,
                'n_detections': len(cluster_data)
            }

        print(self.cluster_centroids)

        if verbose:
            print(f"\nCluster centroid summary:")
            print(f"  Clusters collapsed to {len(self.wave_params)} unique waves")
        
        for wave_key, wave_info in self.wave_params.items():
            wl, ori = wave_key
            wave_id = wave_info['wave_id']
            n_clusters = len(wave_info['clusters'])
            if verbose:
                print(f"    Wave {wave_id}: λ={wl:.0f}km, θ={ori:.0f}°, {n_clusters} cluster(s)")
    
    def reconstruct_frame(self, frame_idx: int, 
                         calibration_scale: float = 1.0,
                         verbose: bool = False) -> Dict:
        """
        Reconstruct a single frame using cluster centroids
        
        Parameters:
        -----------
        frame_idx : int
            Frame to reconstruct
        calibration_scale : float
            Overall amplitude scaling factor
        verbose : bool
            Print progress
            
        Returns:
        --------
        result : Dict
            Contains 'total', 'waves', 'original', 'frame_idx'
        """
        # Get original frame
        frame = self.analyzer.data.isel(time=frame_idx).values
        frame_preprocessed = self.analyzer.preprocess_frame(frame)
        
        # Get clusters present in this frame
        frame_clusters = self.clustered_df[
            self.clustered_df[self.frame_col] == frame_idx
        ]['cluster_id'].unique()
        
        if len(frame_clusters) == 0:
            if verbose:
                print(f"Frame {frame_idx}: No clusters present")
            return {
                'total': np.zeros_like(frame_preprocessed),
                'waves': {wave_id: np.zeros_like(frame_preprocessed) 
                         for wave_id in self.wave_ids},
                'original': frame_preprocessed,
                'frame_idx': frame_idx,
                'wave_params': {},
                'n_waves': 0
            }
        
        # Determine which waves are present
        waves_present = {}
        for cluster_id in frame_clusters:
            centroid = self.cluster_centroids[cluster_id]
            wave_id = centroid['wave_id']
            
            if wave_id not in waves_present:
                waves_present[wave_id] = {
                    'wavelength_km': centroid['wavelength_km'],
                    'orientation_deg': centroid['orientation_deg'],
                    'power': centroid['mean_power']
                }
            else:
                # Multiple clusters map to same wave - average power
                waves_present[wave_id]['power'] = (
                    waves_present[wave_id]['power'] + centroid['mean_power']
                ) / 2
        
        if verbose:
            print(f"Frame {frame_idx}: {len(waves_present)} wave(s) present")
        
        # Reconstruct each wave
        wave_components = {}
        total_reconstruction = np.zeros_like(frame_preprocessed)
        
        for wave_id, wave_info in waves_present.items():
            # Create Gabor kernel for this wave
            kernel = create_gabor_kernel(
                wavelength_km=wave_info['wavelength_km'],
                orientation_deg=wave_info['orientation_deg'],
                resolution_km=self.analyzer.resolution,
                n_stds=self.n_stds,
                Bf=self.Bf,
                Btheta=self.Btheta
            )
            
            # Apply filter
            filtered = signal.fftconvolve(frame_preprocessed, kernel, mode='same')
            filtered_real = np.real(filtered) * wave_info['power'] # JJM Added in normalization by the Gabor power

#            plt.pcolormesh(frame_preprocessed)
#            plt.show()
#            plt.pcolormesh(filtered_real)
#            plt.title(f'filtered, {wave_info['power']}')
#            plt.show()
            
            # Store component
            wave_components[wave_id] = filtered_real
            total_reconstruction += filtered_real
        
        # RMS calibration
        orig_rms = np.sqrt(np.mean(frame_preprocessed**2))
        recon_rms = np.sqrt(np.mean(total_reconstruction**2))
        scale_factor = (orig_rms / recon_rms * calibration_scale) if recon_rms > 0 else 1.0
        
        # Scale all components
        total_reconstruction *= scale_factor
        for wave_id in wave_components:
            wave_components[wave_id] *= scale_factor
        
        # Add zeros for waves not present
        for wave_id in self.wave_ids:
            if wave_id not in wave_components:
                wave_components[wave_id] = np.zeros_like(frame_preprocessed)
        
        return {
            'total': total_reconstruction,
            'waves': wave_components,
            'original': frame_preprocessed,
            'frame_idx': frame_idx,
            'wave_params': waves_present,
            'n_waves': len(waves_present),
            'scale_factor': scale_factor
        }
    
    def reconstruct_all_frames(self,
                               calibration_scale: float = 1.0,
                               verbose: bool = True) -> xr.Dataset:
        """
        Reconstruct all frames using cluster-based approach
        
        Parameters:
        -----------
        calibration_scale : float
            Overall amplitude scaling factor
        verbose : bool
            Print progress
            
        Returns:
        --------
        ds : xr.Dataset
            Complete reconstruction dataset compatible with RCP analysis
        """
        n_frames = len(self.analyzer.times)
        n_north = len(self.analyzer.north)
        n_east = len(self.analyzer.east)
        
        if verbose:
            print(f"Reconstructing {n_frames} frames using cluster-based method...")
            print(f"  Unique waves: {len(self.wave_ids)}")
            print(f"  Image size: {n_north} x {n_east}")
        
        # Initialize arrays
        total_reconstruction = np.zeros((n_frames, n_north, n_east))
        original_data = np.zeros((n_frames, n_north, n_east))
        wave_components = {wave_id: np.zeros((n_frames, n_north, n_east)) 
                          for wave_id in self.wave_ids}
        
        # Initialize parameter arrays for each wave
        wave_wavelengths = {wave_id: np.full(n_frames, np.nan) 
                           for wave_id in self.wave_ids}
        wave_orientations = {wave_id: np.full(n_frames, np.nan) 
                            for wave_id in self.wave_ids}
        wave_powers = {wave_id: np.full(n_frames, np.nan) 
                      for wave_id in self.wave_ids}
        
        # Process each frame
        for frame_idx in range(n_frames):
            if verbose and frame_idx % 10 == 0:
                print(f"  Frame {frame_idx}/{n_frames}...")
            
            # Reconstruct this frame
            result = self.reconstruct_frame(
                frame_idx,
                calibration_scale=calibration_scale,
                verbose=False
            )
            
            # Store results
            total_reconstruction[frame_idx] = result['total'].T ### JJM ADDED T
            original_data[frame_idx] = result['original'].T ### JJM ADDED T
            
            # Store wave components
            for wave_id, component in result['waves'].items():
                wave_components[wave_id][frame_idx] = component.T
            
            # Store parameters for waves present in this frame
            for wave_id, params in result['wave_params'].items():
                wave_wavelengths[wave_id][frame_idx] = params['wavelength_km']
                wave_orientations[wave_id][frame_idx] = params['orientation_deg']
                wave_powers[wave_id][frame_idx] = params['power']

        
        # Create xarray Dataset
        coords = {
            'time': self.analyzer.times,
            'north': self.analyzer.north,
            'east': self.analyzer.east
        }
        
        data_vars = {
            'original': (['time', 'north', 'east'], original_data,
                        {'long_name': 'Original preprocessed data'}),
            'total': (['time', 'north', 'east'], total_reconstruction,
                     {'long_name': 'Total reconstructed wavefield',
                      'method': 'Cluster-based Gabor filtering'}),
            'residual': (['time', 'north', 'east'], 
                        original_data - total_reconstruction,
                        {'long_name': 'Residual (original - reconstruction)'})
        }
        
        # Add individual wave components with zero-padded names
        n_digits = len(str(max(self.wave_ids))) if self.wave_ids else 2
        for wave_id in self.wave_ids:
            wave_name = f'wave_{wave_id:0{n_digits}d}'
            
            data_vars[wave_name] = (
                ['time', 'north', 'east'],
                wave_components[wave_id],
                {'long_name': f'Wave component from cluster bin {wave_id}',
                 'track_id': wave_id}
            )
            
            # Add wavelength, orientation, and power as 1D time series
            data_vars[f'{wave_name}_wavelength'] = (
                ['time'],
                wave_wavelengths[wave_id],
                {'long_name': f'Wavelength for wave {wave_id}',
                 'units': 'km',
                 'track_id': wave_id}
            )
            
            data_vars[f'{wave_name}_orientation'] = (
                ['time'],
                wave_orientations[wave_id],
                {'long_name': f'Orientation for wave {wave_id}',
                 'units': 'degrees',
                 'track_id': wave_id}
            )
            
            data_vars[f'{wave_name}_power'] = (
                ['time'],
                wave_powers[wave_id],
                {'long_name': f'Mean power for wave {wave_id}',
                 'units': 'arbitrary',
                 'track_id': wave_id}
            )
        
        # Create dataset
        ds = xr.Dataset(data_vars, coords=coords)
        
        # Add global attributes
        ds.attrs.update({
            'method': 'cluster_based_reconstruction',
            'wavelength_bin_km': self.wavelength_bin_km,
            'orientation_bin_deg': self.orientation_bin_deg,
            'n_stds': self.n_stds,
            'Bf': self.Bf,
            'Btheta': self.Btheta,
            'n_unique_waves': len(self.wave_ids),
            'track_ids': self.wave_ids,
            'total_clusters': len(self.cluster_ids)
        })
        
        if verbose:
            self._print_summary(ds, original_data, total_reconstruction)
        
        return ds
    
    def _print_summary(self, ds, original_data, total_reconstruction):
        """Print reconstruction summary"""
        print(f"\n{'='*70}")
        print("CLUSTER-BASED RECONSTRUCTION SUMMARY")
        print(f"{'='*70}")
        print(f"Method: Cluster centroids binned to resolution")
        print(f"  Wavelength bins: {self.wavelength_bin_km} km")
        print(f"  Orientation bins: {self.orientation_bin_deg}°")
        print(f"Frames processed: {len(ds.time)}")
        print(f"Total clusters: {len(self.cluster_ids)}")
        print(f"Unique waves: {len(self.wave_ids)}")
        
        # Variance analysis
        total_var = np.var(original_data)
        explained_var = np.var(total_reconstruction)
        residual_var = np.var(original_data - total_reconstruction)
        
        print(f"\nVariance Analysis:")
        print(f"  Original variance: {total_var:.2e}")
        print(f"  Explained variance: {explained_var:.2e} ({explained_var/total_var*100:.1f}%)")
        print(f"  Residual variance: {residual_var:.2e} ({residual_var/total_var*100:.1f}%)")
        
        # Wave info
        print(f"\nUnique Wave Parameters:")
        for wave_key, wave_info in self.wave_params.items():
            wl, ori = wave_key
            wave_id = wave_info['wave_id']
            n_clusters = len(wave_info['clusters'])
            print(f"  Wave {wave_id}: λ={wl:.0f}km, θ={ori:.0f}°, "
                  f"{n_clusters} cluster(s)")
    
    def get_wave_info(self) -> pd.DataFrame:
        """
        Get summary information about all waves
        
        Returns:
        --------
        wave_info : pd.DataFrame
            Information about each unique wave
        """
        data = []
        for wave_key, wave_data in self.wave_params.items():
            wl, ori = wave_key
            data.append({
                'wave_id': wave_data['wave_id'],
                'wavelength_km': wl,
                'orientation_deg': ori,
                'n_clusters': len(wave_data['clusters']),
                'cluster_ids': wave_data['clusters']
            })
        
        return pd.DataFrame(data).sort_values('wave_id')
