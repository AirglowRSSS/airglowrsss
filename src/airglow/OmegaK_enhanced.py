import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from scipy.ndimage import maximum_filter
from sklearn.cluster import DBSCAN

def analyze_and_plot_waves(ok_filter, filtered_ds, filter_mask,
                           power_threshold=90,
                           max_slowness=50,
                           max_wavelength=200,
                           save_prefix='wave_analysis',
                           use_peak_finding=True,
                           peak_min_distance=3,
                           azimuth_tolerance=25,
                           wavelength_tolerance=0.15,
                           min_power_ratio=0.01):
    """
    Complete workflow with improved statistics using circular mean and cluster detection.
    
    Parameters
    ----------
    use_peak_finding : bool
        If True, use peak finding instead of thresholding all bins.
        Recommended for discrete wave analysis.
    peak_min_distance : int
        Minimum distance between peaks (in bins). Higher = fewer, more distinct peaks.
        
    Returns
    -------
    fig : matplotlib figure
        Summary plots
    wave_params : dict
        All detected wave parameters
    filtered_ds : dataset
        Filtered data
    physical_waves : list of dict
        Identified distinct physical waves (harmonics grouped)
    """
    
    # Apply filter if needed
    if filtered_ds is None:
        print("Applying omega-k filter...")
        filtered_ds, fft_ds = ok_filter.apply_filter(filter_mask, return_fft=True)
    else:
        print("Using provided filtered data...")
        _, fft_ds = ok_filter.apply_filter(filter_mask, return_fft=True)
    
    filtered_fft = fft_ds['fft_filtered'].values
    
    # Create summary plot
    print("Creating wave analysis plots...")
    fig, wave_params = plot_wave_slowness_wavelength_summary(
        ok_filter, filtered_fft,
        power_threshold_percentile=power_threshold,
        max_slowness=max_slowness,
        max_wavelength=max_wavelength,
        show_aliasing_limits=True,
        use_peak_finding=use_peak_finding,
        peak_min_distance=peak_min_distance
    )
    
    # Save figure
    filename = f'{save_prefix}_summary.png'
    fig.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"Saved: {filename}")
    
    # Create aliasing report
    report_file = f'{save_prefix}_aliasing_report.txt'
    create_aliasing_report(wave_params, report_file)
    print(f"Saved: {report_file}")
    
    # Improved summary statistics
    good_mask = wave_params['reliability'] == 'good'
    if np.sum(good_mask) > 0:
        print("\n" + "="*70)
        print("SUMMARY STATISTICS (Reliable Waves)")
        print("="*70)
        
        azimuths_good = wave_params['azimuths'][good_mask]
        speeds_good = wave_params['phase_speeds'][good_mask]
        wavelengths_good = wave_params['wavelengths'][good_mask]
        weights_good = wave_params['weights'][good_mask]
        
        # Circular statistics for azimuth
        mean_azimuth = circular_mean(azimuths_good, weights_good)
        std_azimuth = circular_std(azimuths_good, weights_good)
        
        print(f"\nPropagation Direction:")
        print(f"  Circular mean:    {mean_azimuth:.1f}°")
        print(f"  Circular std dev: {std_azimuth:.1f}°")
        
        # Find dominant clusters
        print(f"\n  Dominant directional clusters:")
        clusters = find_wave_clusters(azimuths_good, weights_good, 
                                      n_bins=36, prominence=0.2)
        
        if len(clusters) > 0:
            for i, cluster in enumerate(clusters[:3]):  # Show top 3
                print(f"    {i+1}. {cluster['azimuth']:.1f}° "
                      f"(width: {cluster['width']:.1f}°, "
                      f"power: {cluster['fraction']*100:.1f}%)")
        else:
            print(f"    No strong clusters found (diffuse distribution)")
        
        # Standard statistics for speed and wavelength
        mean_speed = np.average(speeds_good, weights=weights_good)
        std_speed = np.sqrt(np.average((speeds_good - mean_speed)**2, weights=weights_good))
        
        mean_wavelength = np.average(wavelengths_good, weights=weights_good)
        std_wavelength = np.sqrt(np.average((wavelengths_good - mean_wavelength)**2, 
                                            weights=weights_good))
        
        print(f"\nPhase Speed:")
        print(f"  Mean:    {mean_speed:.1f} ± {std_speed:.1f} m/s")
        print(f"  Median:  {np.median(speeds_good):.1f} m/s")
        print(f"  Range:   {speeds_good.min():.1f} - {speeds_good.max():.1f} m/s")
        
        print(f"\nWavelength:")
        print(f"  Mean:    {mean_wavelength:.1f} ± {std_wavelength:.1f} km")
        print(f"  Median:  {np.median(wavelengths_good):.1f} km")
        print(f"  Range:   {wavelengths_good.min():.1f} - {wavelengths_good.max():.1f} km")
        
        print(f"\nSlowness:")
        mean_slowness = 1000.0 / mean_speed
        print(f"  Mean:    {mean_slowness:.1f} s/km")
        
        # Check for multimodal distribution
        if len(clusters) > 1:
            print(f"\n⚠️  NOTE: Multiple directional clusters detected!")
            print(f"   Wave field appears to have {len(clusters)} distinct populations.")
            print(f"   Mean direction may not be representative.")
            print(f"   See cluster analysis above for details.")
        
        print("="*70)
    
    # Identify physical waves by clustering harmonics
    physical_waves = identify_physical_waves(wave_params,
                                             azimuth_tolerance=azimuth_tolerance,
                                             wavelength_tolerance=wavelength_tolerance,
                                             min_power_ratio=min_power_ratio)
    print_physical_wave_summary(physical_waves)
    
    plt.show()
    
    return fig, wave_params, filtered_ds, physical_waves

def plot_wave_slowness_wavelength_summary(ok_filter, filtered_fft,
                                         power_threshold_percentile=90,
                                         nbins_azimuth=36,
                                         nbins_radial=20,
                                         max_slowness=50,
                                         max_wavelength=200,
                                         show_aliasing_limits=True,
                                         use_peak_finding=True,
                                         peak_min_distance=3):
    """
    Create a two-panel figure showing slowness and wavelength polar plots.
    
    Parameters
    ----------
    ok_filter : OmegaKFilterXarray
        The filter object
    filtered_fft : array
        Filtered FFT from apply_filter(return_fft=True)
    power_threshold_percentile : float
        Percentile threshold for significant power
    nbins_azimuth : int
        Number of azimuthal bins
    nbins_radial : int
        Number of radial bins
    max_slowness : float
        Maximum slowness (s/km)
    max_wavelength : float
        Maximum wavelength (km)
    show_aliasing_limits : bool
        Whether to show aliasing boundaries
    use_peak_finding : bool
        If True, use peak finding instead of thresholding
    peak_min_distance : int
        Minimum distance between peaks (in bins)
    """
    
    # Extract wave parameters with aliasing classification
    wave_params = analyze_wave_parameters_with_aliasing(
        ok_filter, filtered_fft, power_threshold_percentile,
        use_peak_finding=use_peak_finding,
        peak_min_distance=peak_min_distance
    )
    
    boundaries = wave_params['boundaries']
    
    # Separate by reliability (use only reliable data for histograms)
    good_mask = wave_params['reliability'] == 'good'
    
    # Create figure with two polar subplots
    fig = plt.figure(figsize=(16, 7))
    
    # ========== LEFT PLOT: SLOWNESS ==========
    ax1 = fig.add_subplot(121, projection='polar')
    
    if np.sum(good_mask) > 0:
        azimuths_good = wave_params['azimuths'][good_mask]
        speeds_good = wave_params['phase_speeds'][good_mask]
        weights_good = wave_params['weights'][good_mask]
        slowness_good = 1000.0 / speeds_good  # s/km
        
        # Create 2D histogram
        azimuth_bins = np.linspace(0, 360, nbins_azimuth + 1)
        slowness_bins = np.linspace(0, max_slowness, nbins_radial + 1)
        
        H_slowness = np.zeros((nbins_azimuth, nbins_radial))
        
        for i in range(len(azimuths_good)):
            if slowness_good[i] <= max_slowness:
                az_idx = np.digitize(azimuths_good[i], azimuth_bins) - 1
                slow_idx = np.digitize(slowness_good[i], slowness_bins) - 1
                
                if 0 <= az_idx < nbins_azimuth and 0 <= slow_idx < nbins_radial:
                    H_slowness[az_idx, slow_idx] += weights_good[i]
        
        # Plot histogram
        theta = np.deg2rad(azimuth_bins)
        r_slow = slowness_bins
        Theta, R = np.meshgrid(theta, r_slow)
        
        if np.any(H_slowness > 0):
            pcm1 = ax1.pcolormesh(Theta, R, H_slowness.T, cmap='viridis',
                                 norm=LogNorm(vmin=H_slowness[H_slowness>0].min(), 
                                            vmax=H_slowness.max()),
                                 shading='auto')
            cbar1 = plt.colorbar(pcm1, ax=ax1, pad=0.1)
            cbar1.set_label('Power', fontsize=11)
    
    # Add aliasing limit (approximate, for typical wavelength)
    if show_aliasing_limits:
        typical_wavelength_km = 50
        min_period_min = boundaries['recommended_period_min']
        max_reliable_speed = (typical_wavelength_km * 1000) / (min_period_min * 60)  # m/s
        aliasing_slowness = 1000.0 / max_reliable_speed  # s/km
        
        theta_circle = np.linspace(0, 2*np.pi, 100)
        r_circle = np.ones_like(theta_circle) * aliasing_slowness
        
        ax1.plot(theta_circle, r_circle, 'r--', linewidth=2, alpha=0.7,
                label=f'Approx. temporal aliasing\n(λ={typical_wavelength_km} km)')
        ax1.legend(loc='upper left', bbox_to_anchor=(1.3, 1.1), fontsize=9)
    
    # Set coordinate system with North=0°, clockwise (compass convention)
    ax1.set_theta_zero_location('N')
    ax1.set_theta_direction(-1)
    ax1.set_ylim([0, max_slowness])
    ax1.set_title('Phase Slowness\n(Reliable components only)', fontsize=13, pad=20)

    # Add text labels for cardinal directions
    label_r = max_slowness * 1.15
    ax1.text(np.deg2rad(0), label_r, 'N', ha='center', va='center',
            fontsize=14, weight='bold')
    ax1.text(np.deg2rad(90), label_r, 'E', ha='center', va='center',
            fontsize=14, weight='bold')
    ax1.text(np.deg2rad(180), label_r, 'S', ha='center', va='center',
            fontsize=14, weight='bold')
    ax1.text(np.deg2rad(270), label_r, 'W', ha='center', va='center',
            fontsize=14, weight='bold')
    
    # Add ylabel
    ax1.text(-0.15, 0.5, 'Slowness (s/km)', transform=ax1.transAxes,
            fontsize=11, rotation=90, va='center')
    
    # ========== RIGHT PLOT: WAVELENGTH ==========
    ax2 = fig.add_subplot(122, projection='polar')
    
    if np.sum(good_mask) > 0:
        wavelengths_good = wave_params['wavelengths'][good_mask]
        
        # Create 2D histogram
        wavelength_bins = np.linspace(0, max_wavelength, nbins_radial + 1)
        
        H_wavelength = np.zeros((nbins_azimuth, nbins_radial))
        
        for i in range(len(azimuths_good)):
            if wavelengths_good[i] <= max_wavelength:
                az_idx = np.digitize(azimuths_good[i], azimuth_bins) - 1
                wl_idx = np.digitize(wavelengths_good[i], wavelength_bins) - 1
                
                if 0 <= az_idx < nbins_azimuth and 0 <= wl_idx < nbins_radial:
                    H_wavelength[az_idx, wl_idx] += weights_good[i]
        
        # Plot histogram
        r_wl = wavelength_bins
        Theta, R = np.meshgrid(theta, r_wl)
        
        if np.any(H_wavelength > 0):
            pcm2 = ax2.pcolormesh(Theta, R, H_wavelength.T, cmap='viridis',
                                 norm=LogNorm(vmin=H_wavelength[H_wavelength>0].min(), 
                                            vmax=H_wavelength.max()),
                                 shading='auto')
            cbar2 = plt.colorbar(pcm2, ax=ax2, pad=0.1)
            cbar2.set_label('Power', fontsize=11)
    
    # Add spatial aliasing limits
    if show_aliasing_limits:
        theta_circle = np.linspace(0, 2*np.pi, 100)
        
        # Nyquist limit
        r_nyquist = np.ones_like(theta_circle) * boundaries['nyquist_wavelength_km']
        ax2.plot(theta_circle, r_nyquist, 'r-', linewidth=2.5, alpha=0.8,
                label=f'Nyquist limit\n({boundaries["nyquist_wavelength_km"]:.1f} km)')
        ax2.fill_between(theta_circle, 0, r_nyquist, alpha=0.15, color='red')
        
        # Recommended limit
        r_recommended = np.ones_like(theta_circle) * boundaries['recommended_wavelength_km']
        ax2.plot(theta_circle, r_recommended, 'orange', linestyle='--', 
                linewidth=2, alpha=0.7,
                label=f'Recommended limit\n({boundaries["recommended_wavelength_km"]:.1f} km)')
        
        ax2.legend(loc='upper left', bbox_to_anchor=(1.3, 1.1), fontsize=9)
    
    # Set coordinate system with North=0°, clockwise (compass convention)
    ax2.set_theta_zero_location('N')
    ax2.set_theta_direction(-1)
    ax2.set_ylim([0, max_wavelength])
    ax2.set_title('Horizontal Wavelength\n(Reliable components only)', fontsize=13, pad=20)

    # Add text labels for cardinal directions
    label_r = max_wavelength * 1.15
    ax2.text(np.deg2rad(0), label_r, 'N', ha='center', va='center',
            fontsize=14, weight='bold')
    ax2.text(np.deg2rad(90), label_r, 'E', ha='center', va='center',
            fontsize=14, weight='bold')
    ax2.text(np.deg2rad(180), label_r, 'S', ha='center', va='center',
            fontsize=14, weight='bold')
    ax2.text(np.deg2rad(270), label_r, 'W', ha='center', va='center',
            fontsize=14, weight='bold')
    
    # Add ylabel
    ax2.text(-0.15, 0.5, 'Wavelength (km)', transform=ax2.transAxes,
            fontsize=11, rotation=90, va='center')
    
    # Add overall title
    n_good = np.sum(good_mask)
    n_total = len(wave_params['azimuths'])
    method_str = "Peak Finding" if use_peak_finding else "Thresholding"
    fig.suptitle(f'Gravity Wave Characteristics ({n_good}/{n_total} components reliable) - {method_str}', 
                fontsize=14, weight='bold', y=0.98)
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    return fig, wave_params

def analyze_wave_parameters_with_aliasing(ok_filter, filtered_fft, 
                                         power_threshold_percentile=90,
                                         include_aliased=True,
                                         use_peak_finding=True,
                                         peak_min_distance=3):
    """
    Extract wave parameters and classify by aliasing reliability.
    
    Parameters
    ----------
    use_peak_finding : bool
        If True, find local maxima in power spectrum instead of thresholding all bins
    peak_min_distance : int
        Minimum distance between peaks (in bins). Higher = fewer peaks.
    """
    
    # Get aliasing boundaries
    boundaries = calculate_aliasing_boundaries(ok_filter)
    
    # Calculate power spectrum
    power = np.abs(filtered_fft)**2
    
    # Only look at positive frequencies
    n_omega_half = len(ok_filter.omega) // 2
    power_positive = power[:n_omega_half, :, :]
    omega_positive = ok_filter.omega[:n_omega_half]
    
    if use_peak_finding:
        print("\n=== Using PEAK FINDING method ===")
        significant_points = find_spectral_peaks(power_positive, 
                                                 power_threshold_percentile,
                                                 min_distance=peak_min_distance)
    else:
        print("\n=== Using THRESHOLDING method ===")
        threshold = np.percentile(power_positive[power_positive > 0], power_threshold_percentile)
        significant_points = power_positive > threshold
    
    azimuths = []
    phase_speeds = []
    wavelengths = []
    periods = []
    weights = []
    reliability = []
    
    for i in range(n_omega_half):
        if omega_positive[i] <= 0:
            continue
            
        for j in range(len(ok_filter.ky)):
            for k in range(len(ok_filter.kx)):
                if significant_points[i, j, k]:
                    omega = omega_positive[i]
                    kx = ok_filter.kx[k]
                    ky = ok_filter.ky[j]
                    kh = np.sqrt(kx**2 + ky**2)
                    
                    if kh > 0:
                        # Wave parameters
                        phase_speed = omega / kh
                        wavelength_km = (2 * np.pi / kh) / 1000  # km
                        period_min = (2 * np.pi / omega) / 60  # minutes
                        
                        # Azimuth with corrected sign
                        azimuth = np.rad2deg(np.arctan2(-kx, -ky))
                        if azimuth < 0:
                            azimuth += 360
                        
                        # Classify reliability
                        rel = classify_wave_reliability(wavelength_km, period_min, boundaries)
                        
                        # Store if including aliased or if not aliased
                        if include_aliased or rel != 'aliased':
                            azimuths.append(azimuth)
                            phase_speeds.append(phase_speed)
                            wavelengths.append(wavelength_km)
                            periods.append(period_min)
                            weights.append(power_positive[i, j, k])
                            reliability.append(rel)
    
    # Convert to arrays
    wave_params = {
        'azimuths': np.array(azimuths),
        'phase_speeds': np.array(phase_speeds),
        'wavelengths': np.array(wavelengths),
        'periods': np.array(periods),
        'weights': np.array(weights),
        'reliability': np.array(reliability),
        'boundaries': boundaries
    }
    
    # Print summary
    total = len(azimuths)
    n_good = np.sum(np.array(reliability) == 'good')
    n_marginal = np.sum(np.array(reliability) == 'marginal')
    n_aliased = np.sum(np.array(reliability) == 'aliased')
    
    print("\n=== WAVE RELIABILITY SUMMARY ===")
    if n_good > 0:
        print(f"Total components: {total}")
        print(f"  Good (reliable):     {n_good} ({100*n_good/total:.1f}%)")
        print(f"  Marginal:            {n_marginal} ({100*n_marginal/total:.1f}%)")
        print(f"  Aliased (unreliable): {n_aliased} ({100*n_aliased/total:.1f}%)")
        
        print(f"\nAliasing Boundaries:")
        print(f"  Nyquist limits:")
        print(f"    Wavelength > {boundaries['nyquist_wavelength_km']:.1f} km")
        print(f"    Period > {boundaries['nyquist_period_min']:.1f} min")
        print(f"  Recommended limits (for reliable direction):")
        print(f"    Wavelength > {boundaries['recommended_wavelength_km']:.1f} km")
        print(f"    Period > {boundaries['recommended_period_min']:.1f} min")
    else:
        print(f"Total componets: {total}")
    
    # Show top components by reliability
    if n_good > 0:
        good_mask = np.array(reliability) == 'good'
        good_weights = np.array(weights)[good_mask]
        good_azimuths = np.array(azimuths)[good_mask]
        good_speeds = np.array(phase_speeds)[good_mask]
        good_wavelengths = np.array(wavelengths)[good_mask]
        
        n_show = min(6, len(good_weights))
        top_good_idx = np.argsort(good_weights)[::-1][:n_show]
        print(f"\nTop {n_show} RELIABLE components:")
        for i, idx in enumerate(top_good_idx):
            print(f"  {i+1}. Az={good_azimuths[idx]:.1f}°, "
                  f"Speed={good_speeds[idx]:.1f} m/s, "
                  f"Slowness={1000./good_speeds[idx]:.1f} s/km, "
                  f"Wavelength={good_wavelengths[idx]:.1f} km, "
                  f"Weight={good_weights[idx]:.3e}")
    
    if n_aliased > 0:
        print(f"\n⚠️  WARNING: {n_aliased} components are aliased!")
        print(f"   Their propagation directions are UNRELIABLE.")
    
    return wave_params

def find_spectral_peaks(power, power_threshold_percentile, min_distance=3):
    """
    Find local maxima in the power spectrum.
    
    This reduces spectral leakage by only selecting peak bins rather than
    all bins above a threshold.
    
    Parameters
    ----------
    power : 3D array
        Power spectrum (omega, ky, kx)
    power_threshold_percentile : float
        Only consider peaks above this percentile
    min_distance : int
        Minimum distance between peaks (in bins). Higher = fewer, more distinct peaks.
        
    Returns
    -------
    peaks : 3D boolean array
        True at peak locations
    """
    # Apply threshold first to reduce computation
    # Safe threshold calculation
    if len(power[power > 0]) > 0:
        threshold = np.percentile(power[power > 0], power_threshold_percentile)
    else:
        print("Warning: No non-zero power in spectrum")
        return np.zeros_like(power, dtype=bool)  # Return empty mask
        
    above_threshold = power > threshold
    
    # Find local maxima using maximum filter
    # A point is a local maximum if it equals the max in its neighborhood
    footprint_size = 2 * min_distance + 1
    local_max = maximum_filter(power, size=footprint_size, mode='constant', cval=0.0)
    
    # Peak is where: (1) power equals local max AND (2) above threshold
    peaks = (power == local_max) & above_threshold
    
    n_peaks = np.sum(peaks)
    n_threshold = np.sum(above_threshold)
    print(f"Peak finding: {n_peaks} peaks found (vs {n_threshold} bins above threshold)")
    print(f"  Reduction factor: {n_threshold/max(n_peaks, 1):.1f}x")
    
    return peaks

def apply_3d_window(data, window_type='tukey', alpha=0.25):
    """
    Apply a 3D window function to reduce spectral leakage.
    
    Parameters
    ----------
    data : 3D array
        Data to window (time, y, x) or (omega, ky, kx)
    window_type : str
        'tukey', 'hann', 'hamming', or 'blackman'
    alpha : float
        For Tukey window: fraction of the window inside the cosine tapered region.
        alpha=0 is rectangular, alpha=1 is Hann.
        
    Returns
    -------
    windowed_data : 3D array
        Windowed data
    window : 3D array
        The window function used
    """
    nt, ny, nx = data.shape
    
    if window_type == 'tukey':
        from scipy.signal.windows import tukey
        win_t = tukey(nt, alpha=alpha)
        win_y = tukey(ny, alpha=alpha)
        win_x = tukey(nx, alpha=alpha)
    elif window_type == 'hann':
        win_t = np.hanning(nt)
        win_y = np.hanning(ny)
        win_x = np.hanning(nx)
    elif window_type == 'hamming':
        win_t = np.hamming(nt)
        win_y = np.hamming(ny)
        win_x = np.hamming(nx)
    elif window_type == 'blackman':
        win_t = np.blackman(nt)
        win_y = np.blackman(ny)
        win_x = np.blackman(nx)
    else:
        raise ValueError(f"Unknown window type: {window_type}")
    
    # Create 3D window
    window = win_t[:, np.newaxis, np.newaxis] * win_y[np.newaxis, :, np.newaxis] * win_x[np.newaxis, np.newaxis, :]
    
    # Apply window
    windowed_data = data * window
    
    # Report power loss
    power_original = np.sum(data**2)
    power_windowed = np.sum(windowed_data**2)
    power_ratio = power_windowed / power_original
    
    print(f"\nWindowing applied ({window_type}, alpha={alpha}):")
    print(f"  Power retained: {power_ratio*100:.1f}%")
    print(f"  Power lost: {(1-power_ratio)*100:.1f}%")
    
    return windowed_data, window

def calculate_aliasing_boundaries(ok_filter):
    """
    Calculate the aliasing boundaries for the dataset.
    
    Returns
    -------
    boundaries : dict
        Dictionary with aliasing limits
    """
    # Spatial sampling
    dx = ok_filter.dx / 1000  # km
    dy = ok_filter.dy / 1000  # km
    dx_mean = np.mean([dx, dy])
    
    # Temporal sampling
    dt = ok_filter.dt / 60  # minutes
    
    # Nyquist limits
    nyquist_wavelength_km = 2 * dx_mean
    nyquist_period_min = 2 * dt
    
    ## Recommended limits (4 samples per wavelength/period for reliable analysis)
    #recommended_wavelength_km = 4 * dx_mean
    #recommended_period_min = 4 * dt

    # Lower the reliability cutoff to true Nyquist (2×dt):
    # In calculate_aliasing_boundaries, change:
    recommended_wavelength_km = 2 * dx_mean   # was 4×
    recommended_period_min    = 2 * dt        # was 4×
        
    boundaries = {
        'nyquist_wavelength_km': nyquist_wavelength_km,
        'nyquist_period_min': nyquist_period_min,
        'recommended_wavelength_km': recommended_wavelength_km,
        'recommended_period_min': recommended_period_min,
        'dx_km': dx_mean,
        'dt_min': dt,
    }
    
    return boundaries

def classify_wave_reliability(wavelength_km, period_min, boundaries):
    """
    Classify wave reliability based on sampling.
    
    Returns
    -------
    reliability : str
        'good', 'marginal', or 'aliased'
    """
    if (wavelength_km >= boundaries['recommended_wavelength_km'] and 
        period_min >= boundaries['recommended_period_min']):
        return 'good'
    elif (wavelength_km >= boundaries['nyquist_wavelength_km'] and 
          period_min >= boundaries['nyquist_period_min']):
        return 'marginal'
    else:
        return 'aliased'

def create_aliasing_report(wave_params, output_file='aliasing_report.txt'):
    """
    Create a text report summarizing aliasing effects.
    """
    boundaries = wave_params['boundaries']
    
    # Separate by reliability
    good_mask = wave_params['reliability'] == 'good'
    marginal_mask = wave_params['reliability'] == 'marginal'
    aliased_mask = wave_params['reliability'] == 'aliased'
    
    with open(output_file, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("WAVE ANALYSIS ALIASING REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("SAMPLING PARAMETERS:\n")
        f.write(f"  Spatial resolution:  {boundaries['dx_km']:.2f} km\n")
        f.write(f"  Temporal resolution: {boundaries['dt_min']:.1f} minutes\n\n")
        
        f.write("ALIASING BOUNDARIES:\n")
        f.write(f"  Nyquist wavelength:  {boundaries['nyquist_wavelength_km']:.1f} km\n")
        f.write(f"  Nyquist period:      {boundaries['nyquist_period_min']:.1f} minutes\n")
        f.write(f"  Recommended wavelength (4× sampling): {boundaries['recommended_wavelength_km']:.1f} km\n")
        f.write(f"  Recommended period (4× sampling):     {boundaries['recommended_period_min']:.1f} minutes\n\n")
        
        f.write("WAVE RELIABILITY STATISTICS:\n")
        total = len(wave_params['azimuths'])
        n_good = np.sum(good_mask)
        n_marginal = np.sum(marginal_mask)
        n_aliased = np.sum(aliased_mask)
        
        f.write(f"  Total components detected: {total}\n")
        if total > 0:
            f.write(f"  Reliable (good):           {n_good} ({100*n_good/total:.1f}%)\n")
            f.write(f"  Marginal:                  {n_marginal} ({100*n_marginal/total:.1f}%)\n")
            f.write(f"  Aliased (unreliable):      {n_aliased} ({100*n_aliased/total:.1f}%)\n\n")
        
        # Power-weighted statistics (reliable only)
        if n_good > 0:
            total_power = np.sum(wave_params['weights'])
            good_power = np.sum(wave_params['weights'][good_mask])
            
            f.write(f"POWER DISTRIBUTION:\n")
            f.write(f"  Power in reliable components: {100*good_power/total_power:.1f}%\n")
            f.write(f"  Power in unreliable components: {100*(1-good_power/total_power):.1f}%\n\n")
            
            # Mean parameters for reliable waves
            mean_az = np.average(wave_params['azimuths'][good_mask], 
                                weights=wave_params['weights'][good_mask])
            mean_speed = np.average(wave_params['phase_speeds'][good_mask],
                                   weights=wave_params['weights'][good_mask])
            
            f.write(f"DOMINANT RELIABLE WAVE CHARACTERISTICS:\n")
            f.write(f"  Mean azimuth:     {mean_az:.1f}°\n")
            f.write(f"  Mean phase speed: {mean_speed:.1f} m/s\n\n")

            good_weights = wave_params['weights'][good_mask]
            good_azimuths = wave_params['azimuths'][good_mask]
            good_speeds = wave_params['phase_speeds'][good_mask]
            good_wavelengths = wave_params['wavelengths'][good_mask]
            
            n_show = min(6, len(good_weights))
            top_good_idx = np.argsort(good_weights)[::-1][:n_show]
            f.write(f"\nTop {n_show} RELIABLE components:\n")
            for i, idx in enumerate(top_good_idx):
                f.write(f"  {i+1}. Az={good_azimuths[idx]:.1f}°, Speed={good_speeds[idx]:.1f} m/s, Slowness={1000./good_speeds[idx]:.1f} s/km, Wavelength={good_wavelengths[idx]:.1f} km, Weight={good_weights[idx]:.3e}\n")
        
        f.write("INTERPRETATION NOTES:\n")
        f.write("  - 'Good' (Reliable): Direction and speed are well-sampled and trustworthy\n")
        f.write("  - 'Marginal': Meets Nyquist but may have some uncertainty\n")
        f.write("  - 'Aliased' (Unreliable): Direction is ambiguous or incorrect due to\n")
        f.write("                             undersampling. DO NOT TRUST propagation direction!\n\n")
        
        if n_aliased > 0:
            f.write("⚠️  WARNING:\n")
            f.write(f"  {n_aliased} components ({100*n_aliased/total:.1f}%) are aliased.\n")
            f.write(f"  These waves have periods < {boundaries['recommended_period_min']:.1f} min\n")
            f.write(f"  or wavelengths < {boundaries['recommended_wavelength_km']:.1f} km.\n")
            f.write(f"  Their propagation directions should NOT be trusted.\n")
        
        f.write("\n" + "=" * 70 + "\n")
    
    print(f"Aliasing report saved to: {output_file}")

def identify_physical_waves(wave_params,
                           azimuth_tolerance=25,
                           wavelength_tolerance=0.15,
                           min_power_ratio=0.01):
    """
    Cluster detected peaks into physical waves by grouping harmonics and artifacts.
    
    The key insight: A physical gravity wave has a specific k-vector (wavelength + direction).
    Multiple peaks with the same k but different omega (frequency) are harmonics.
    
    Parameters
    ----------
    wave_params : dict
        Output from analyze_wave_parameters_with_aliasing
    azimuth_tolerance : float
        Maximum azimuth difference (degrees) to consider peaks as same wave
    wavelength_tolerance : float
        Maximum fractional wavelength difference (0.25 = 25%) to consider peaks as same wave
    min_power_ratio : float
        Minimum power ratio (relative to strongest peak) to include in analysis
        
    Returns
    -------
    physical_waves : list of dict
        Each dict contains:
        - 'azimuth', 'wavelength', 'phase_speed', 'period': Primary wave parameters
        - 'power': Total power in this wave (including harmonics)
        - 'confidence': Quality score (0-1)
        - 'n_peaks': Number of peaks in this cluster
        - 'peak_indices': Indices into wave_params arrays
        - 'is_fundamental': Whether this is likely the fundamental frequency
    """
    
    # Filter to reliable waves only
    reliable_mask = wave_params['reliability'] == 'good'

    if not np.any(reliable_mask):
        print("No reliable waves found")
        return []

    azimuths = wave_params['azimuths'][reliable_mask]
    wavelengths = wave_params['wavelengths'][reliable_mask]
    speeds = wave_params['phase_speeds'][reliable_mask]
    periods = wave_params['periods'][reliable_mask]
    weights = wave_params['weights'][reliable_mask]
           
    # Filter by minimum power
    max_power = weights.max()
    power_mask = weights > min_power_ratio * max_power
    
    azimuths = azimuths[power_mask]
    wavelengths = wavelengths[power_mask]
    speeds = speeds[power_mask]
    periods = periods[power_mask]
    weights = weights[power_mask]
    original_indices = np.where(reliable_mask)[0][power_mask]
    
    if len(azimuths) == 0:
        print("No peaks above minimum power threshold")
        return []

    print(f"\n=== PHYSICAL WAVE IDENTIFICATION ===")
    print(f"Analyzing {len(azimuths)} peaks above {min_power_ratio*100:.1f}% of max power")
    
    # Cluster by k-space (wavelength and direction)
    # Convert to features: need to handle circular azimuth properly
    az_rad = np.deg2rad(azimuths)
    
    # Create feature matrix: [cos(az), sin(az), wavelength]
    # Normalize wavelength to similar scale as trig functions
    wl_normalized = wavelengths / wavelengths.max()
    wl_weight = 1.0 / wavelength_tolerance  # Scale wavelength importance
    
    features = np.column_stack([
        np.cos(az_rad),
        np.sin(az_rad), 
        wl_normalized * wl_weight
    ])
    
    # DBSCAN clustering
    # eps controls the neighborhood size
    eps = np.sqrt(2 * (1 - np.cos(np.deg2rad(azimuth_tolerance)))**2 + 
                  (wavelength_tolerance * wl_weight)**2)
    
    clustering = DBSCAN(eps=eps, min_samples=1).fit(features)
    labels = clustering.labels_
    
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print(f"Found {n_clusters} spatial clusters (k-vector groups)")
    
    # Process each cluster
    physical_waves = []
    
    for cluster_id in set(labels):
        if cluster_id == -1:  # Noise points in DBSCAN
            continue
            
        cluster_mask = labels == cluster_id
        cluster_indices = original_indices[cluster_mask]
        
        # Get cluster members
        c_azimuths = azimuths[cluster_mask]
        c_wavelengths = wavelengths[cluster_mask]
        c_speeds = speeds[cluster_mask]
        c_periods = periods[cluster_mask]
        c_weights = weights[cluster_mask]
        
        # Identify fundamental: usually highest power OR lowest frequency
        # For gravity waves, lowest frequency (longest period) is typically fundamental
        fundamental_idx = np.argmax(c_weights)  # Use highest power as primary criterion
        
        # Check if there are harmonics (frequencies ~2x, 3x, etc.)
        fundamental_period = c_periods[fundamental_idx]
        period_ratios = c_periods / fundamental_period
        has_harmonics = np.any((period_ratios < 0.6) & (period_ratios > 0.4))  # ~0.5 = 2nd harmonic
        
        # Calculate confidence based on cluster characteristics
        confidence = calculate_wave_confidence(
            c_weights, 
            c_azimuths, 
            c_wavelengths,
            has_harmonics
        )
        
        # Representative parameters (from fundamental)
        wave_dict = {
            'azimuth': c_azimuths[fundamental_idx],
            'wavelength': c_wavelengths[fundamental_idx],
            'phase_speed': c_speeds[fundamental_idx],
            'period': c_periods[fundamental_idx],
            'power': c_weights[fundamental_idx],
            'total_power': np.sum(c_weights),  # Include harmonics
            'confidence': confidence,
            'n_peaks': len(c_azimuths),
            'peak_indices': cluster_indices,
            'is_fundamental': True,
            'has_harmonics': has_harmonics,
            'azimuth_std': np.std(c_azimuths) if len(c_azimuths) > 1 else 0,
            'wavelength_std': np.std(c_wavelengths) if len(c_wavelengths) > 1 else 0,
        }
        
        physical_waves.append(wave_dict)
    
    # Sort by power (descending)
    physical_waves = sorted(physical_waves, key=lambda x: x['power'], reverse=True)
    
    # Print summary
    print(f"\n{'='*70}")
    print(f"IDENTIFIED {len(physical_waves)} PHYSICAL WAVES")
    print(f"{'='*70}")
    
    for i, wave in enumerate(physical_waves):
        print(f"\nWave {i+1}:")
        print(f"  Direction:   {wave['azimuth']:.1f}° (±{wave['azimuth_std']:.1f}°)")
        print(f"  Wavelength:  {wave['wavelength']:.1f} km (±{wave['wavelength_std']:.1f} km)")
        print(f"  Phase Speed: {wave['phase_speed']:.1f} m/s")
        print(f"  Period:      {wave['period']:.1f} min")
        print(f"  Power:       {wave['power']:.2e}")
        print(f"  Confidence:  {wave['confidence']:.2f}")
        print(f"  Peaks:       {wave['n_peaks']} (harmonics: {wave['has_harmonics']})")
        
        if wave['n_peaks'] > 1:
            # Show the peaks in this cluster
            print(f"  Cluster details:")
            for idx in wave['peak_indices'][:5]:  # Show up to 5
                print(f"    - Az={wave_params['azimuths'][idx]:.1f}°, "
                      f"λ={wave_params['wavelengths'][idx]:.1f}km, "
                      f"c={wave_params['phase_speeds'][idx]:.1f}m/s")
    
    print(f"{'='*70}\n")
    
    return physical_waves


def calculate_wave_confidence(weights, azimuths, wavelengths, has_harmonics):
    """
    Calculate confidence score for a detected wave.
    
    Higher confidence when:
    - Single isolated peak (not clustered with others)
    - Tight spatial clustering (small std in azimuth/wavelength)
    - High power
    - Clear harmonic structure (if multiple peaks)
    
    Returns
    -------
    confidence : float
        Score from 0 to 1
    """
    confidence = 1.0
    
    # Penalty for multiple peaks without clear harmonics
    if len(weights) > 1 and not has_harmonics:
        # Multiple peaks that aren't harmonics suggests ambiguity
        confidence *= 0.7
    
    # Penalty for spatial spread
    if len(azimuths) > 1:
        azimuth_spread = np.std(azimuths)
        wavelength_spread = np.std(wavelengths) / np.mean(wavelengths)
        
        # Penalize if spread is large
        if azimuth_spread > 15:
            confidence *= 0.8
        if wavelength_spread > 0.15:
            confidence *= 0.8
    
    # Bonus for single isolated peak
    if len(weights) == 1:
        confidence = 1.0
    
    # Bonus for clear harmonics
    if has_harmonics and len(weights) > 1:
        confidence *= 1.1  # Slight bonus
        confidence = min(confidence, 1.0)
    
    return confidence


def print_physical_wave_summary(physical_waves):
    """
    Print a clean summary of identified physical waves.
    """
    print("\n" + "="*70)
    print("PHYSICAL WAVE SUMMARY")
    print("="*70)
    print(f"\nDetected {len(physical_waves)} distinct physical waves:\n")
    
    print(f"{'#':<3} {'Direction':<12} {'Wavelength':<14} {'Speed':<12} {'Confidence':<12} {'Notes':<20}")
    print("-" * 70)
    
    for i, wave in enumerate(physical_waves):
        direction_str = f"{wave['azimuth']:.1f}°"
        wavelength_str = f"{wave['wavelength']:.1f} km"
        speed_str = f"{wave['phase_speed']:.1f} m/s"
        confidence_str = f"{wave['confidence']:.2f}"
        
        notes = []
        if wave['n_peaks'] > 1:
            notes.append(f"{wave['n_peaks']} peaks")
        if wave['has_harmonics']:
            notes.append("harmonics")
        notes_str = ", ".join(notes) if notes else "single peak"
        
        print(f"{i+1:<3} {direction_str:<12} {wavelength_str:<14} {speed_str:<12} "
              f"{confidence_str:<12} {notes_str:<20}")
    
    print("="*70 + "\n")


def circular_mean(angles_deg, weights=None):
    """
    Calculate circular mean of angles.
    
    Parameters
    ----------
    angles_deg : array
        Angles in degrees (0-360)
    weights : array, optional
        Weights for each angle
        
    Returns
    -------
    mean_angle : float
        Circular mean in degrees (0-360)
    """
    angles_rad = np.deg2rad(angles_deg)
    
    if weights is None:
        weights = np.ones_like(angles_deg)
    
    # Convert to unit vectors and compute weighted average
    sin_sum = np.sum(weights * np.sin(angles_rad))
    cos_sum = np.sum(weights * np.cos(angles_rad))
    
    # Convert back to angle
    mean_rad = np.arctan2(sin_sum, cos_sum)
    mean_deg = np.rad2deg(mean_rad)
    
    # Ensure 0-360 range
    if mean_deg < 0:
        mean_deg += 360
    
    return mean_deg


def circular_std(angles_deg, weights=None):
    """
    Calculate circular standard deviation.
    
    Returns
    -------
    std_deg : float
        Circular standard deviation in degrees
    """
    angles_rad = np.deg2rad(angles_deg)
    
    if weights is None:
        weights = np.ones_like(angles_deg)
    
    weights = weights / np.sum(weights)
    
    # Mean resultant length
    sin_sum = np.sum(weights * np.sin(angles_rad))
    cos_sum = np.sum(weights * np.cos(angles_rad))
    R = np.sqrt(sin_sum**2 + cos_sum**2)
    
    # Circular std
    std_rad = np.sqrt(-2 * np.log(R))
    std_deg = np.rad2deg(std_rad)
    
    return std_deg

def find_wave_clusters(azimuths, weights, n_bins=36, prominence=0.3):
    """
    Find dominant azimuthal clusters/peaks in wave distribution.
    
    Parameters
    ----------
    azimuths : array
        Azimuths in degrees
    weights : array
        Power weights for each component
    n_bins : int
        Number of azimuthal bins
    prominence : float
        Minimum prominence for peak detection (fraction of max)
        
    Returns
    -------
    clusters : list of dict
        Each dict contains: 'azimuth', 'power', 'width', 'fraction'
    """
    from scipy.signal import find_peaks
    
    # Create weighted histogram
    bins = np.linspace(0, 360, n_bins + 1)
    hist, _ = np.histogram(azimuths, bins=bins, weights=weights)
    bin_centers = (bins[:-1] + bins[1:]) / 2
    
    # Make circular by wrapping
    hist_extended = np.concatenate([hist, hist, hist])
    
    # Find peaks
    peaks, properties = find_peaks(hist_extended, 
                                   prominence=prominence * hist.max(),
                                   width=1)
    
    # Filter to middle section (avoid duplicates from wrapping)
    valid_peaks = (peaks >= n_bins) & (peaks < 2 * n_bins)
    peaks = peaks[valid_peaks] - n_bins  # Shift back to original indexing
    
    # Extract cluster information
    clusters = []
    total_power = np.sum(hist)
    
    for peak_idx in peaks:
        azimuth = bin_centers[peak_idx]
        power = hist[peak_idx]
        
        # Estimate width (FWHM)
        half_max = power / 2
        left = peak_idx
        right = peak_idx
        
        while left > 0 and hist[left] > half_max:
            left -= 1
        while right < n_bins - 1 and hist[right] > half_max:
            right += 1
        
        width = (right - left) * (360 / n_bins)
        fraction = power / total_power
        
        clusters.append({
            'azimuth': azimuth,
            'power': power,
            'width': width,
            'fraction': fraction
        })
    
    # Sort by power (descending)
    clusters = sorted(clusters, key=lambda x: x['power'], reverse=True)
    
    return clusters