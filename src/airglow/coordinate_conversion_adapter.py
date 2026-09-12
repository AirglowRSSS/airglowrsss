"""
Coordinate Conversion Adapter

This module provides compatibility with your existing convert_angle() function
and explains when/how to use it with the new Gabor code.

KEY INSIGHT:
-----------
convert_angle() does: theta_new = 90 - theta_old

This swaps between two conventions:
- Convention A: 0 deg = East, 90 deg = North, counterclockwise (standard math angle)
- Convention B: 0 deg = North, 90 deg = East, clockwise (geographic compass bearing --
  matches the k-vector/propagation-direction convention used throughout the rest of
  this codebase, e.g. phase_speed_kalman.py's orientation_deg/azimuth)

In your current workflow:
- FFT uses Convention A (0° = East)
- Your Gabor code uses Convention B (0° = North)
- You call convert_angle() to translate between them

In the NEW code:
- Both FFT and Gabor use Convention A (0° = East)
- NO conversion needed between them!
- Results are directly comparable

However, if you want results in Convention B (to match your old outputs):
- Apply convert_angle() to ALL output orientations

BUG FIX (see CaseStudy_RealData_OneNight.ipynb history): the previous
implementation wrapped negative results back into [0, 180) via
`np.where(result < 0, 180 + result, result)`. That wrap is mathematically
unnecessary (theta_new = 90 - theta_old is already its own inverse without
it) and actively harmful for create_gabor_kernel's use of this function's
output as a raw angle fed to cos/sin: the wrap introduces a genuine
discontinuity exactly at theta_old = 90 deg (convert_angle(89.99) ~ 0 vs
convert_angle(90.01) ~ 180 -- a ~180 deg jump for two inputs that should
give nearly-identical kernels), which flips the kernel to its complex
conjugate and inverts the sign of every downstream omega/direction estimate
for any wave whose orientation_deg (axis) exceeds 90 deg. Verified fixed
against a synthetic wave with known ground-truth direction across all four
quadrants (0-360 deg) -- see the investigation in that notebook's history
for the empirical before/after comparison.
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