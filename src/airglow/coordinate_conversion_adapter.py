"""
coordinate_conversion_adapter.py
----------------------------------
`convert_angle` translates between the two angle conventions in play
around Gabor kernel construction: standard math angle (0deg=East, 90deg=North,
counterclockwise) and compass bearing (0deg=North, 90deg=East, clockwise -- the
convention used for orientation_deg/azimuth throughout the rest of this
codebase, e.g. `phase_speed_kalman.py`). `cluster_reconstruction.create_gabor_kernel`
is this function's one live caller, converting a wave's compass-bearing
orientation into the math-angle rotation its kernel construction needs.

Deliberately NOT wrapped into [0, 180): an earlier version wrapped negative
results back with `np.where(result < 0, 180 + result, result)`, which is
mathematically unnecessary (`theta_new = 90 - theta_old` is already its own
inverse without it) and was actively harmful -- the wrap introduced a
genuine discontinuity exactly at `theta_old = 90` (a ~180 deg jump for two
inputs that should give nearly-identical kernels), flipping the kernel to
its complex conjugate and inverting the sign of every downstream
omega/direction estimate for any wave whose orientation exceeds 90 deg.
"""

import numpy as np


def convert_angle(angles):
    """
    Convert angles between two reference systems.
    This transformation is reversible using the same function
    (theta_new = 90 - theta_old is its own inverse).

    System A (geographic/math): 0 deg = East, 90 deg = North, counterclockwise
    System B (compass bearing): 0 deg = North, 90 deg = East, clockwise --
        the k-vector/propagation-direction convention used elsewhere in this
        codebase (e.g. orientation_deg/azimuth in phase_speed_kalman.py)

    Deliberately NOT wrapped into [0, 180) -- see the module docstring's
    "BUG FIX" note for why that wrap is both unnecessary and harmful here.
    Callers that pass angles already in [0, 180) get a result in (-90, 90];
    the reversibility property holds regardless of range.

    Parameters:
    -----------
    angles : float or array-like
        Input angle(s) in degrees

    Returns:
    --------
    float or ndarray
        Converted angle(s) in degrees
    """
    angles = np.asarray(angles)
    return 90 - angles




# Example usage and testing
if __name__ == "__main__":
    print("Coordinate Conversion Testing")
    print("="*70)
    
    # Test the transformation
    test_angles_A = np.array([0, 45, 90, 135])
    test_angles_B = convert_angle(test_angles_A)
    test_angles_back = convert_angle(test_angles_B)
    
    print("\nTransformation test:")
    print(f"Convention A (0°=East): {test_angles_A}")
    print(f"Convention B (0°=North): {test_angles_B}")
    print(f"Back to A: {test_angles_back}")
    print(f"Reversible: {np.allclose(test_angles_A, test_angles_back)}")
    
    print("\n" + "="*70)
    print("INTERPRETATION GUIDE")
    print("="*70)
    print("""
    Convention A (NEW code default): 0° = East, 90° = North
         North ↑ (90°)
              |
              |
              └──────→ East (0°)

    Convention B (YOUR old Gabor): 0° = North, 90° = East, clockwise
         North ↑ (0°)
              |
              |
              └──────→ East (90°)
    
    RECOMMENDATION:
    ---------------
    Keep results in Convention A (geographic standard) unless you need
    backward compatibility with old results. Convention A is:
    - More intuitive (0° points East/right)
    - Matches FFT convention
    - Standard in atmospheric science
    - Used by other analysis tools
    
    If you need Convention B for comparison with old results:
    - Call apply_conversion_to_results(results_df, convert=True)
    - This will match your old Gabor output format
    """)