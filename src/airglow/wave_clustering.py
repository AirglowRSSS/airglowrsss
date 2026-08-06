"""
Wave Clustering for refined_df

Clusters similar waves based on wavelength and orientation proximity.
Automatically adjusts Gabor filter bandwidths to match clustering resolution.
"""

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN, AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from typing import Optional, Tuple


def cluster_refined_waves(
    refined_df: pd.DataFrame,
    wavelength_resolution_km: float = 15.0,
    orientation_resolution_deg: float = 15.0,
    method: str = 'hierarchical',
    verbose: bool = False
) -> pd.DataFrame:
    """
    Cluster waves in refined_df by wavelength and orientation proximity
    
    This reduces the number of unique waves by grouping similar ones together,
    which is useful when you have many slightly different detections of the
    same physical wave.
    
    Parameters:
    -----------
    refined_df : pd.DataFrame
        Refined detections with columns: time, wavelength_km, orientation_deg,
        wave_id, frame_idx, energy
    wavelength_resolution_km : float
        Desired wavelength resolution (km) - waves within this range are grouped
    orientation_resolution_deg : float
        Desired orientation resolution (degrees) - waves within this range are grouped
    method : str
        Clustering method: 'hierarchical' or 'dbscan'
    verbose : bool
        Print progress and statistics
        
    Returns:
    --------
    clustered_df : pd.DataFrame
        Same as refined_df but with new 'cluster_id' column replacing wave_id
    """
    from scipy.cluster.hierarchy import linkage, fcluster
    from scipy.spatial.distance import squareform
    
    if verbose:
        print("="*70)
        print("CLUSTERING REFINED WAVES")
        print("="*70)
        print(f"Input: {len(refined_df)} detections, {refined_df['wave_id'].nunique()} unique waves")
        print(f"Wavelength resolution: {wavelength_resolution_km} km")
        print(f"Orientation resolution: {orientation_resolution_deg}°")
        print(f"Method: {method}")
    
    # Get unique waves (one row per wave_id)
    unique_waves = refined_df.groupby('wave_id').agg({
        'wavelength_km': 'first',
        'orientation_deg': 'first',
        'energy': 'mean',
        'frame_idx': 'count'  # Number of detections
    }).reset_index()
    unique_waves.rename(columns={'frame_idx': 'n_detections'}, inplace=True)
    
    if verbose:
        print(f"\nUnique waves to cluster: {len(unique_waves)}")
    
    # Handle edge case: empty dataframe (no waves)
    if len(unique_waves) == 0:
        if verbose:
            print("\nNo waves to cluster - returning empty dataframe")
        
        clustered_df = refined_df.copy()
        clustered_df['cluster_id'] = []
        
        # Create empty cluster summary
        cluster_summary = pd.DataFrame(columns=[
            'cluster_id', 'wavelength_km', 'orientation_deg', 
            'n_detections', 'n_original_waves', 'mean_energy',
            'first_frame', 'last_frame'
        ])
        clustered_df.cluster_summary = cluster_summary
        
        return clustered_df, cluster_summary
    
    # Handle edge case: only one unique wave (no clustering needed)
    if len(unique_waves) == 1:
        unique_waves['cluster_id'] = 1
        n_clusters = 1
        
        if verbose:
            print(f"\nOnly 1 unique wave - assigning to cluster_id=1 (no clustering needed)")
        
        # Create cluster summary
        cluster_params = [{
            'cluster_id': 1,
            'wavelength_km': unique_waves['wavelength_km'].iloc[0],
            'orientation_deg': unique_waves['orientation_deg'].iloc[0],
            'n_detections': unique_waves['n_detections'].iloc[0],
            'n_original_waves': 1,
            'mean_energy': unique_waves['energy'].iloc[0],
            'first_frame': refined_df['frame_idx'].min(),
            'last_frame': refined_df['frame_idx'].max()
        }]
        cluster_summary = pd.DataFrame(cluster_params)
        
        # Merge cluster_id back to full refined_df
        clustered_df = refined_df.copy()
        clustered_df['cluster_id'] = 1
        clustered_df.cluster_summary = cluster_summary
        
        if verbose:
            print(f"\n{'='*70}")
            print("CLUSTER SUMMARY")
            print(f"{'='*70}")
            print(f"{'Cluster':<10} {'λ (km)':<10} {'θ (°)':<10} {'Detections':<12} {'Orig Waves':<12} {'Frames'}")
            print("-"*70)
            row = cluster_params[0]
            print(f"{row['cluster_id']:<10} {row['wavelength_km']:<10.1f} "
                  f"{row['orientation_deg']:<10.1f} {row['n_detections']:<12} "
                  f"{row['n_original_waves']:<12} "
                  f"{row['first_frame']}-{row['last_frame']}")
            print(f"\nTotal detections: {len(clustered_df)}")
            print(f"Unique clusters: 1")
        
        return clustered_df, cluster_summary
    
    # Custom distance function for AXIAL data (orientations 0-180°)
    def axial_distance_matrix(wavelengths, orientations, wave_res, ori_res):
        """
        Compute distance matrix for wavelength and orientation (axial) data.
        Orientations are doubled to handle 0-180° wraparound properly.
        """
        n = len(wavelengths)
        dist_matrix = np.zeros((n, n))
        
        # Double the orientations to map to full circle
        orientations_doubled = orientations * 2
        
        for i in range(n):
            for j in range(i+1, n):
                # Wavelength distance normalized by resolution
                wave_dist = np.abs(wavelengths[i] - wavelengths[j]) / wave_res
                
                # Orientation distance (circular on DOUBLED angles) 
                # Normalize by 2*resolution since we doubled the angles
                orient_diff = np.abs(orientations_doubled[i] - orientations_doubled[j])
                orient_dist = np.minimum(orient_diff, 360 - orient_diff) / (2 * ori_res)
                
                # Euclidean distance in normalized space
                dist = np.sqrt(wave_dist**2 + orient_dist**2)
                dist_matrix[i, j] = dist
                dist_matrix[j, i] = dist
        
        return dist_matrix
    
    # Compute distance matrix
    wavelengths = unique_waves['wavelength_km'].values
    orientations = unique_waves['orientation_deg'].values
    
    if verbose:
        print(f"\nComputing axial distance matrix...")
    
    dist_matrix = axial_distance_matrix(
        wavelengths, orientations, 
        wavelength_resolution_km, orientation_resolution_deg
    )
    
    # Perform clustering
    if method == 'hierarchical':
        # Convert to condensed distance matrix for linkage
        condensed_dist = squareform(dist_matrix)
        
        # Use 'average' linkage which works well with custom distances
        # Distance threshold of 1.0 means clusters are separated by ~resolution
        linkage_matrix = linkage(condensed_dist, method='complete')
        labels = fcluster(linkage_matrix, t=1.25, criterion='distance')
        
        # fcluster returns labels starting at 1
        
    elif method == 'dbscan':
        # For DBSCAN with precomputed distances
        eps = 1.0  # Distance threshold
        min_samples = 1  # Allow single-wave clusters
        
        clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='precomputed')
        labels = clustering.fit_predict(dist_matrix)
        
        # DBSCAN labels start at 0 (or -1 for noise)
        if labels.min() == -1:
            labels = labels + 1  # Shift so noise becomes 0
        else:
            labels = labels + 1  # Shift to start at 1
        
    else:
        raise ValueError(f"Unknown method: {method}. Use 'hierarchical' or 'dbscan'")
    
    # Assign cluster IDs
    unique_waves['cluster_id'] = labels
    n_clusters = unique_waves['cluster_id'].nunique()
    
    if verbose:
        print(f"\nClustering results:")
        print(f"  Number of clusters: {n_clusters}")
        print(f"  Reduction: {len(unique_waves)} → {n_clusters} waves")
        print(f"  Compression ratio: {len(unique_waves)/n_clusters:.2f}x")
    
    # Merge cluster_id back to full refined_df
    wave_to_cluster = dict(zip(unique_waves['wave_id'], unique_waves['cluster_id']))
    clustered_df = refined_df.copy()
    clustered_df['cluster_id'] = clustered_df['wave_id'].map(wave_to_cluster)
    
    # Compute representative parameters for each cluster
    # Use energy-weighted mean with proper axial averaging
    cluster_params = []
    for cluster_id in sorted(clustered_df['cluster_id'].unique()):
        cluster_data = clustered_df[clustered_df['cluster_id'] == cluster_id]
        
        # Get unique wave_ids in this cluster
        wave_ids_in_cluster = cluster_data['wave_id'].unique()
        
        # Energy-weighted mean parameters
        total_energy = 0
        weighted_wavelength = 0
        weighted_cos = 0
        weighted_sin = 0
        
        for wave_id in wave_ids_in_cluster:
            wave_data = cluster_data[cluster_data['wave_id'] == wave_id]
            energy = wave_data['energy'].mean()
            wavelength = wave_data['wavelength_km'].iloc[0]
            orientation = wave_data['orientation_deg'].iloc[0]
            
            total_energy += energy
            weighted_wavelength += energy * wavelength
            
            # Use doubled angle for proper axial averaging
            ori_rad = np.radians(orientation)
            weighted_cos += energy * np.cos(2 * ori_rad)
            weighted_sin += energy * np.sin(2 * ori_rad)
        
        # Compute weighted means
        mean_wavelength = weighted_wavelength / total_energy
        mean_ori_doubled = np.arctan2(weighted_sin, weighted_cos)
        mean_orientation = np.degrees(mean_ori_doubled / 2)
        
        # Ensure orientation in [0, 180)
        if mean_orientation < 0:
            mean_orientation += 180
        if mean_orientation >= 180:
            mean_orientation -= 180
        
        cluster_params.append({
            'cluster_id': int(cluster_id),
            'wavelength_km': mean_wavelength,
            'orientation_deg': mean_orientation,
            'n_detections': len(cluster_data),
            'n_original_waves': len(wave_ids_in_cluster),
            'mean_energy': cluster_data['energy'].mean(),
            'first_frame': cluster_data['frame_idx'].min(),
            'last_frame': cluster_data['frame_idx'].max()
        })
    
    cluster_summary = pd.DataFrame(cluster_params)
    
    if verbose:
        print(f"\n{'='*70}")
        print("CLUSTER SUMMARY")
        print(f"{'='*70}")
        print(f"{'Cluster':<10} {'λ (km)':<10} {'θ (°)':<10} {'Detections':<12} {'Orig Waves':<12} {'Frames'}")
        print("-"*70)
        
        for idx, row in cluster_summary.head(20).iterrows():
            print(f"{row['cluster_id']:<10} {row['wavelength_km']:<10.1f} "
                  f"{row['orientation_deg']:<10.1f} {row['n_detections']:<12} "
                  f"{row['n_original_waves']:<12} "
                  f"{row['first_frame']}-{row['last_frame']}")
        
        if len(cluster_summary) > 20:
            print(f"... ({len(cluster_summary)-20} more clusters)")
        
        print(f"\nTotal detections: {len(clustered_df)}")
        print(f"Unique clusters: {n_clusters}")
    
    # Store cluster summary as attribute (for later use)
    clustered_df.cluster_summary = cluster_summary
    
    return clustered_df, cluster_summary


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


def plot_clustering_results(
    clustered_df: pd.DataFrame,
    original_refined_df: pd.DataFrame,
    figsize: Tuple[int, int] = (16, 10),
    save_path: Optional[str] = None
):
    """
    Visualize clustering results
    
    Parameters:
    -----------
    clustered_df : pd.DataFrame
        Clustered detections (with cluster_id)
    original_refined_df : pd.DataFrame
        Original refined detections (with wave_id)
    figsize : tuple
        Figure size
    save_path : str, optional
        Path to save figure
    """
    
    fig, axes = plt.subplots(2, 2, figsize=figsize)
    
    # Get cluster summary
    if hasattr(clustered_df, 'cluster_summary'):
        cluster_summary = clustered_df.cluster_summary
    else:
        # Compute on the fly
        cluster_summary = clustered_df.groupby('cluster_id').agg({
            'wavelength_km': 'first',
            'orientation_deg': 'first',
            'energy': 'mean',
            'frame_idx': 'count'
        }).reset_index()
        cluster_summary.rename(columns={'frame_idx': 'n_detections'}, inplace=True)
    
    # Panel 1: Wavelength vs Orientation scatter (BEFORE)
    orig_unique = original_refined_df.groupby('wave_id').agg({
        'wavelength_km': 'first',
        'orientation_deg': 'first',
        'energy': 'mean',
        'frame_idx': 'count'
    }).reset_index()
    
    scatter1 = axes[0, 0].scatter(
        orig_unique['wavelength_km'],
        orig_unique['orientation_deg'],
        s=orig_unique['frame_idx'],
        c=orig_unique['wave_id'],
        cmap='tab20',
        alpha=0.6,
        edgecolors='black',
        linewidth=0.5
    )
    axes[0, 0].set_xlabel('Wavelength (km)', fontsize=11)
    axes[0, 0].set_ylabel('Orientation (Â°)', fontsize=11)
    axes[0, 0].set_title(f'Before Clustering\n({len(orig_unique)} waves)',
                        fontsize=12, fontweight='bold')
    axes[0, 0].grid(True, alpha=0.3)
    axes[0, 0].set_ylim(0, 180)
    
    # Panel 2: Wavelength vs Orientation scatter (AFTER)
    scatter2 = axes[0, 1].scatter(
        cluster_summary['wavelength_km'],
        cluster_summary['orientation_deg'],
        s=cluster_summary['n_detections'],
        c=cluster_summary['cluster_id'],
        cmap='tab20',
        alpha=0.6,
        edgecolors='black',
        linewidth=0.5
    )
    axes[0, 1].set_xlabel('Wavelength (km)', fontsize=11)
    axes[0, 1].set_ylabel('Orientation (Â°)', fontsize=11)
    axes[0, 1].set_title(f'After Clustering\n({len(cluster_summary)} waves)',
                        fontsize=12, fontweight='bold')
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].set_ylim(0, 180)
    
    # Panel 3: Cluster size distribution
    axes[1, 0].hist(cluster_summary['n_original_waves'], bins=20,
                   edgecolor='black', alpha=0.7, color='steelblue')
    axes[1, 0].set_xlabel('Original Waves per Cluster', fontsize=11)
    axes[1, 0].set_ylabel('Number of Clusters', fontsize=11)
    axes[1, 0].set_title('Cluster Size Distribution', fontsize=12, fontweight='bold')
    axes[1, 0].grid(True, alpha=0.3, axis='y')
    
    # Panel 4: Detection count comparison
    orig_counts = orig_unique['frame_idx'].values
    cluster_counts = cluster_summary['n_detections'].values
    
    axes[1, 1].hist([orig_counts, cluster_counts],
                   bins=30, label=['Before', 'After'],
                   alpha=0.6, edgecolor='black')
    axes[1, 1].set_xlabel('Detections per Wave', fontsize=11)
    axes[1, 1].set_ylabel('Number of Waves', fontsize=11)
    axes[1, 1].set_title('Detection Distribution', fontsize=12, fontweight='bold')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"\nâœ“ Saved clustering visualization to {save_path}")
    
    plt.show()


def export_clustered_catalog(
    clustered_df: pd.DataFrame,
    output_prefix: str = 'clustered_waves'
):
    """
    Export clustered wave catalog
    
    Parameters:
    -----------
    clustered_df : pd.DataFrame
        Clustered detections
    output_prefix : str
        Prefix for output files
    """
    
    # Get cluster summary
    if hasattr(clustered_df, 'cluster_summary'):
        cluster_summary = clustered_df.cluster_summary
    else:
        cluster_summary = clustered_df.groupby('cluster_id').agg({
            'wavelength_km': 'first',
            'orientation_deg': 'first',
            'energy': 'mean',
            'frame_idx': ['count', 'min', 'max']
        }).reset_index()
        cluster_summary.columns = ['cluster_id', 'wavelength_km', 'orientation_deg',
                                   'mean_energy', 'n_detections', 'first_frame', 'last_frame']
    
    # Export full detections
    detections_file = f'{output_prefix}_detections.csv'
    clustered_df.to_csv(detections_file, index=False)
    print(f"âœ“ Exported clustered detections to {detections_file}")
    
    # Export cluster summary
    summary_file = f'{output_prefix}_summary.csv'
    cluster_summary.to_csv(summary_file, index=False)
    print(f"âœ“ Exported cluster summary to {summary_file}")
    
    # Create report
    report_file = f'{output_prefix}_report.txt'
    with open(report_file, 'w') as f:
        f.write("="*70 + "\n")
        f.write("CLUSTERED WAVE CATALOG\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"Total clusters: {len(cluster_summary)}\n")
        f.write(f"Total detections: {len(clustered_df)}\n")
        f.write(f"Frame range: {clustered_df['frame_idx'].min()} to {clustered_df['frame_idx'].max()}\n\n")
        
        f.write("="*70 + "\n")
        f.write("CLUSTER DETAILS\n")
        f.write("="*70 + "\n\n")
        
        for idx, row in cluster_summary.iterrows():
            f.write(f"Cluster {row['cluster_id']}:\n")
            f.write(f"  Wavelength: {row['wavelength_km']:.1f} km\n")
            f.write(f"  Orientation: {row['orientation_deg']:.1f}Â°\n")
            f.write(f"  Detections: {row['n_detections']}\n")
            if 'n_original_waves' in row:
                f.write(f"  Original waves merged: {row['n_original_waves']}\n")
            f.write(f"  Mean energy: {row['mean_energy']:.2f}\n")
            f.write(f"  Frame range: {row['first_frame']} to {row['last_frame']}\n")
            f.write("\n")
    
    print(f"âœ“ Exported report to {report_file}")


# Example usage
if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*70)
    print("EXAMPLE USAGE")
    print("="*70)
    print("""
# Step 1: Cluster waves in refined_df
clustered_df = cluster_refined_waves(
    refined_df,
    wavelength_resolution_km=15.0,   # Group waves within Â±15 km
    orientation_resolution_deg=15.0, # Group waves within Â±15Â°
    method='hierarchical',
    verbose=True
)

# Step 2: Compute adaptive Gabor parameters
Bf, Btheta = compute_adaptive_gabor_parameters(
    wavelength_resolution_km=15.0,
    orientation_resolution_deg=15.0,
    resolution_km_per_pixel=real_analyzer.resolution,
    verbose=True
)

# Step 3: Visualize clustering results
plot_clustering_results(
    clustered_df,
    refined_df,  # Original for comparison
    save_path='clustering_results.png'
)

# Step 4: Export clustered catalog
export_clustered_catalog(clustered_df, output_prefix='clustered_waves')

# Step 5: Create reconstructor with adaptive parameters
from refined_wavefield_reconstruction import RefinedWavefieldReconstructor

reconstructor = RefinedWavefieldReconstructor(
    analyzer=real_analyzer,
    refined_df=clustered_df,  # Use clustered data!
    n_stds=3.0,
    Bf=Bf,        # Use adaptive Bf
    Btheta=Btheta, # Use adaptive Btheta
    max_workers=48
)

# Step 6: Create animation with clustered waves
from refined_animation import create_refined_wavefield_animation

success = create_refined_wavefield_animation(
    reconstructor=reconstructor,
    analyzer=real_analyzer,
    video_file='clustered_wavefield.mp4',
    fps=10,
    dpi=150,
    verbose=True
)

# Now you have fewer, cleaner waves with appropriate Gabor filtering!
    """)