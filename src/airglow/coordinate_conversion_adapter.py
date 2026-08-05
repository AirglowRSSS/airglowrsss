"""
Coordinate Conversion Adapter

This module provides compatibility with your existing convert_angle() function
and explains when/how to use it with the new Gabor code.

KEY INSIGHT:
-----------
Your convert_angle() does: θ_new = (90 - θ_old) % 180

This swaps between two conventions:
- Convention A: 0° = East, 90° = North  (geographic/standard)
- Convention B: 0° = North, 90° = West  (90° counterclockwise rotation)

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
"""

import numpy as np


def convert_angle(angles):
    """
    Convert angles between two reference systems.
    Both systems constrained to 0-180 degrees.
    This transformation is reversible using the same function.
    
    System A (geographic): 0° = East, 90° = North
    System B (rotated):    0° = North, 90° = West
    
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
    result = 90 - angles
    result = np.where(result < 0, 180 + result, result)
    return result


def apply_conversion_to_results(results_df, convert=True):
    """
    Apply coordinate conversion to Gabor results DataFrame
    
    Use this if you want results in Convention B (0° = North) to match
    your old workflow output format.
    
    Parameters:
    -----------
    results_df : pd.DataFrame
        Results from FFTGaborReconstructor.analyze_all_frames()
    convert : bool
        If True, convert orientations to Convention B
        If False, keep in Convention A (no conversion)
        
    Returns:
    --------
    results_df : pd.DataFrame
        DataFrame with orientations converted if requested
    """
    if convert:
        results_df['fft_orientation_deg'] = convert_angle(results_df['fft_orientation_deg'])
        results_df['gabor_orientation_deg'] = convert_angle(results_df['gabor_orientation_deg'])
    
    return results_df


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
    
    Convention B (YOUR old Gabor): 0° = North, 90° = West
         North ↑ (0°)
              |
              |
         West ←──────
              (90°)
    
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