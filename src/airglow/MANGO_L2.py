import os
import re
import urllib.request
import matplotlib.pyplot as plt
import xarray as xr
import h5py
import pandas as pd
import pytz
from scipy.signal import medfilt2d
import subprocess
import airglow.fpiinfo as fpiinfo
import airglow.BoltwoodSensor as BoltwoodSensor
import airglow.asiinfo as asiinfo
import ephem
import airglow.gabor as gabor
import multiprocessing as mp
import numpy.fft as fft
from skimage.feature import peak_local_max
import numpy as np
import datetime

MANGO_ARCHIVE_URL = 'https://data.mangonetwork.org/data/transport/mango/archive'

def runcmd(cmd, verbose=False):
# Runs a command line command (useful for downloading data)
# From https://www.scrapingbee.com/blog/python-wget/
#
# OUTPUTS:
#   (stdout, stderr) of the command; stdout is None if the command failed

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if verbose:
            print(result.stdout.strip())
            print(result.stderr.strip())
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command '{cmd}' failed with error: {e.stderr}")
        return None, e.stderr

def download_mango_L1(site, emission, date, asi_directory=None):
# Download the MANGO level-1 files for one site, emission and night from the MANGO archive
#
# INPUTS:
#   site - MANGO site abbreviation of interest (e.g., "low")
#   emission - emission of interest ('green' or 'red')
#   date - datetime object giving the date of interest
#   asi_directory - (optional) directory the data are downloaded to, in {site}/{year}/{doy}/ below it
#                   (default: the current directory)
#
# OUTPUTS:
#   downloaded_files - list of the .hdf5 files in the target directory (helpful for later cleanup).
#                      Files already present are not downloaded again.

    if asi_directory is None:
        asi_directory = './'

    emission_path = 'greenline' if emission == 'green' else 'redline'
    target_directory = os.path.join(asi_directory, site, str(date.year), date.strftime('%j'))
    os.makedirs(target_directory, exist_ok=True)

    # The archive serves a directory listing for each night; download every .hdf5 file it links to
    url = f'{MANGO_ARCHIVE_URL}/{site}/{emission_path}/level1/{date.year}/{date.strftime("%j")}/'
    print(url)
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            listing = response.read().decode('utf-8', errors='replace')
    except OSError as e:
        print(f'Could not read {url}: {e}')
        listing = ''

    for name in sorted(set(re.findall(r'href="([^"/]+\.hdf5)"', listing))):
        path = os.path.join(target_directory, name)
        if not os.path.exists(path):
            try:
                urllib.request.urlretrieve(url + name, path + '.part')
                os.replace(path + '.part', path)
            except OSError as e:
                print(f'Could not download {url + name}: {e}')

    downloaded_files = []
    for root, dirs, files in os.walk(target_directory):
        for file in files:
            if file.endswith('.hdf5'):
                downloaded_files.append(os.path.join(root, file))

    return downloaded_files

def load_hdf5_to_xarray(file_path, kernel_size=7):
# Function to load data from HDF5 file into xarray DataArrays
# 
# Input:
#   file_path: complete path to an MANGO HDF5 file
#   kernel_size: (optional) size of 2D median filter to apply to each image
#
# Output:
#   ds: xarray data structure containing information from the HDF5 file
#       The data structure has the following format:
#       Coordinates:
#           time (datetime64) - time stamps of each image in the data set
#           east (float) - east km distance from the center of the image. This is a uniform step.
#           north (float) - north km distance from the center of the image. This is a uniform step.
#       Data Variable:
#           integration_time (float) - integration time for each image in sec (dim: time)
#           ImageData (int) - intensity values in the image (dims: time, north, east)
#                             the image data has had a 7x7 median filter applied to remove stars
#           Azimuth (float) - azimuth for each pixel, in deg (dims: north, east)
#           Elevation (float) - elevation for each pixel, in deg (dims: north, east)
#           Latitude (float) - latitude for each pixel, in deg N (dims: north, east)
#           Longitude (float) - longtitude for each pixel, in deg E (dims: north, east)
#           CCDTemperature (float) - CCD temperature, in C (dim: time); only if the file has it

    # Read in the HDF5 file into a temporary array
    data_arrays = {}
    with h5py.File(file_path, 'r') as hdf:
        # Detect format version by checking if 'Elevation' exists at root
        is_old_format = 'Elevation' in hdf
    
        if is_old_format:
            # Old format: keys at root level
            key_mapping = {
                'ImageData': 'ImageData',
                'UnixTime': 'UnixTime',
                'Elevation': 'Elevation',
                'Azimuth': 'Azimuth',
                'Latitude': 'Latitude',
                'Longitude': 'Longitude',
                'CCDTemperature': 'CCDTemperature',
                'PixelCoordinates': 'PixelCoordinates'
            }
        else:
            # New format: keys nested in groups
            key_mapping = {
                'ImageData': 'ImageData',
                'UnixTime': 'UnixTime',
                'Elevation': 'Coordinates/Elevation',
                'Azimuth': 'Coordinates/Azimuth',
                'Latitude': 'Coordinates/Latitude',
                'Longitude': 'Coordinates/Longitude',
                'CCDTemperature': 'DataQuality/CCDTemperature',
#                'PixelCoordinates': 'Coordinates/XYGrid'
                'PixelCoordinates': 'Coordinates/PixelCoordinates'
            }
    
        # Load datasets using the appropriate paths. CCDTemperature is optional; the rest are required.
        for logical_name, hdf_path in key_mapping.items():
            if hdf_path in hdf and isinstance(hdf[hdf_path], h5py.Dataset):
                data_arrays[logical_name] = xr.DataArray(hdf[hdf_path][...], name=logical_name)
            elif logical_name != 'CCDTemperature':
                raise KeyError(f"Required dataset '{hdf_path}' not found in {file_path}")

    # Convert Unix times to datetime objects
    if is_old_format:
        # Old format: [2, n_times] - first index is start/stop
        unix_times_start = data_arrays['UnixTime'][0].values
        unix_times_stop = data_arrays['UnixTime'][1].values
    else:
        # New format: [n_times, 2] - second index is start/stop
        unix_times_start = data_arrays['UnixTime'][:, 0].values
        unix_times_stop = data_arrays['UnixTime'][:, 1].values

    start_times = pd.to_datetime(unix_times_start, unit='s', utc=True)
    stop_times = pd.to_datetime(unix_times_stop, unit='s', utc=True)

    # Calculate integration time and ensure it's a float
    integration_time_values = (stop_times - start_times).total_seconds()
    integration_time_values = integration_time_values.astype(float)

    # Ensure 'time' is numpy.datetime64
    start_times_np = np.array(start_times, dtype='datetime64[ns]')

    # Rename the dimensions of the ImageData
    image_data = data_arrays['ImageData'].rename({'dim_0': 'time', 'dim_1': 'north', 'dim_2': 'east'})
    image_data.attrs['description'] = 'Pixel values of image at each time step'

    # Process other variables
    azimuth = data_arrays['Azimuth'].rename({'dim_0': 'north', 'dim_1': 'east'})
    azimuth.attrs['units'] = 'Degrees'
    azimuth.attrs['description'] = 'Azimuth of each pixel'

    elevation = data_arrays['Elevation'].rename({'dim_0': 'north', 'dim_1': 'east'})
    elevation.attrs['units'] = 'Degrees'
    elevation.attrs['description'] = 'Elevation of each pixel'

    latitude = data_arrays['Latitude'].rename({'dim_0': 'north', 'dim_1': 'east'})
    latitude.attrs['units'] = 'Degrees N'
    latitude.attrs['description'] = 'Geodetic latitude of each pixel'

    longitude = data_arrays['Longitude'].rename({'dim_0': 'north', 'dim_1': 'east'})
    longitude.attrs['units'] = 'Degrees E'
    longitude.attrs['description'] = 'Geodetic longitude of each pixel'

    # PixelCoordinates is (2, north, east): [0] holds X (east) and varies along the east axis,
    # [1] holds Y (north) and varies along the north axis (attribute 'Size': '2 (X,Y) x Ipixels x Jpixels')
    pixel_coords = data_arrays['PixelCoordinates'].values

    north_array = xr.DataArray(pixel_coords[1, :, 0], dims=('north'))
    north_array.attrs['units'] = 'km'
    north_array.attrs['description'] = 'Distance in north direction from site at airglow layer'

    east_array = xr.DataArray(pixel_coords[0, 0, :], dims=('east'))
    east_array.attrs['units'] = 'km'
    east_array.attrs['description'] = 'Distance in east direction from site at airglow layer'

    # Apply median filter to each image and replace NaN values with 0.
    # TODO: zero-filling NaN pixels (e.g., outside the field of view) is a choice to investigate;
    # keeping them as NaN may be better for later steps that average or Fourier transform the images.
    im_ds = []
    for id in image_data:
        filtered_img = np.nan_to_num(medfilt2d(id, kernel_size=kernel_size), nan=0.0)
        temp = xr.DataArray(filtered_img, dims=('north','east'))
        im_ds.append(temp)

    combined = xr.concat(im_ds, dim=('time'))
    combined.attrs['description'] = (f'Pixel values of image at each time step after {kernel_size}x{kernel_size} '
                                     'median filter (NaN values replaced with 0)')

    integration_time = xr.DataArray(integration_time_values, coords={'time': start_times_np}, dims=('time'))
    integration_time.attrs['units'] = 's'
    integration_time.attrs['description'] = 'Integration time of the image'

    # Create xarray Dataset
    variables = {'integration_time': integration_time,
                 'ImageData': combined,
                 'Azimuth': azimuth,
                 'Elevation': elevation,
                 'Latitude': latitude,
                 'Longitude': longitude,
                 'east': east_array,
                 'north': north_array}

    if 'CCDTemperature' in data_arrays:
        CCDTemperature = data_arrays['CCDTemperature'].rename({'dim_0': 'time'})
        CCDTemperature.attrs['units'] = 'Celsius'
        CCDTemperature.attrs['description'] = 'Temperature of CCD'
        variables['CCDTemperature'] = CCDTemperature

    ds = xr.Dataset(variables)

    return ds

def load_cloud_and_moon(analysis_parameters, times):
# Load in the cloud data, if it exists, and calculate the moon angle for requested times.
#
# INPUTS:
#   analysis_parameters - dictionary containing at least the "site" entry with the site abbreviation to process and "date" entry with the datenum to process
#   times - list of times, generally corresponding to the times of images for which we want to check if the moon is up
#
# OUTPUTS:
#   results_df: Pandas dataframe structure containing times, cloud temperature, and moon angle
#       The data structure has the following format:
#           UT (datetime64) - universal time of each estimate
#           Cloud Temperature (float) - temperature from the cloud sensor in C
#           Moon Altitude (float) - angle above the horizon of the moon (deg)
    
    if analysis_parameters['site'] in ['cvo','low','blo']:
        my_site = fpiinfo.get_site_info(analysis_parameters['site'],analysis_parameters['date'])

        # Because of timezones and how the cloud senor data files are broken up, 
        # read on day earlier and later just to be sure we capture the data needed
        dns0, sky_temp0, amb_temp0 = BoltwoodSensor.ReadTempLog('/rdata/airglow/templogs/cloudsensor/%s/Cloud_%s_%s.txt' % (analysis_parameters['site'], analysis_parameters['site'], (analysis_parameters['date']-datetime.timedelta(days=1)).strftime('%Y%m%d')),my_site['Timezone'])
        dns1, sky_temp1, amb_temp1 = BoltwoodSensor.ReadTempLog('/rdata/airglow/templogs/cloudsensor/%s/Cloud_%s_%s.txt' % (analysis_parameters['site'], analysis_parameters['site'], analysis_parameters['date'].strftime('%Y%m%d')),my_site['Timezone'])
        dns2, sky_temp2, amb_temp2 = BoltwoodSensor.ReadTempLog('/rdata/airglow/templogs/cloudsensor/%s/Cloud_%s_%s.txt' % (analysis_parameters['site'], analysis_parameters['site'], (analysis_parameters['date']+datetime.timedelta(days=1)).strftime('%Y%m%d')),my_site['Timezone'])
        dns = np.concatenate((dns0,dns1,dns2))
        sky_temp = np.concatenate((sky_temp0, sky_temp1, sky_temp2))
        amb_temp = np.concatenate((amb_temp0, amb_temp1, amb_temp2))
    else:
        # No cloud data
        dns = times.tz_localize(pytz.utc)
        sky_temp = np.full(times.shape, np.nan)
        amb_temp = np.full(times.shape, np.nan)

    # find the moon
    s_info = asiinfo.get_site_info(analysis_parameters['site'])
    moon = ephem.Moon
    obs = ephem.Observer()
    obs.lat = str(s_info['Location'][0])
    obs.lon = str(s_info['Location'][1])

    moon_alt = []
    for t in times:
        obs.date = t
        moon = ephem.Moon(obs)
        moon_alt.append(moon.alt.real * 180./np.pi)

    # Create dataframes to merge data and interpolate easily
    image_df = pd.DataFrame(times, columns=['UT'])
    image_df.index = pd.to_datetime(times)
    image_df.index = image_df.index.tz_localize(pytz.utc)

    # Create the timzone aware cloud data frame
    if len(dns) > 0:
        cloud_df = pd.DataFrame(np.array([dns,sky_temp]).T, columns=['LT','Cloud Temperature'])
        cloud_df.index = pd.to_datetime(dns)
        if cloud_df.index.tz is None:
            # Naive times are taken to be UTC
            cloud_df.index = cloud_df.index.tz_localize(pytz.utc)
        cloud_df.index = cloud_df.index.tz_convert(pytz.utc)
    else:
        # No cloud data
        dns = times.tz_localize(pytz.utc)
        sky_temp = np.full(times.shape, np.nan)
        amb_temp = np.full(times.shape, np.nan)

        cloud_df = pd.DataFrame(np.array([dns,sky_temp]).T, columns=['LT','Cloud Temperature'])
        cloud_df.index = pd.to_datetime(dns)
        cloud_df.index = cloud_df.index.tz_convert(pytz.utc)

    # Merge the cloud data onto the image data. This is done by finding the previous measurement
    # in cloud_df. May want to look into some sort of interpolation scheme?
    
    # merge_asof needs both indexes in the same time unit (pandas >= 3 may infer seconds for one of them)
    image_df.index = image_df.index.as_unit('ns')
    cloud_df.index = cloud_df.index.as_unit('ns')
    results_df = pd.merge_asof(image_df, cloud_df.sort_values('LT'), left_index=True, right_index=True,
                                    tolerance=pd.Timedelta("60s"),)
    results_df = results_df.drop(['LT'],axis=1)
    results_df['Moon Altitude'] = moon_alt

    return results_df

def run_gabor(subset, real_lam, theta, n_stds, Bf, Btheta, NUM_PROCESSORS = 32):
# Run the Gabor filter on a dataset
# INPUTS:
#   subset - xarray data structure containing information on the dataset to be processed with at least the following format:
#       Coordinates:
#           time (datetime64) - time stamps of each image in the data set
#           east (float) - east km distance from the center of the image. This is a uniform step.
#           north (float) - north km distance from the center of the image. This is a uniform step.
#       Data Variable:
#           FilteredImageData (float) - intensity values in the image (dims: time, north, east)
#                                       the image data has had a 7x7 median filter applied to remove stars and has been temporally filtered 
#   real_lam - array of wavelengths to run the Gabor filtering through (meters)
#   theta - array of orientations to run the Gabor filtering through (radians)
#   n_stds - width of the Gabor filter kernel (see Ch 3.2 of Grawe thesis)
#   Bf - frequency bandwidth for the Gabor filter kernel (see Ch 3.2 of Grawe thesis)
#   Btheta - orientation bandwidth for the Gabor filter kernel (see Ch 3.2 of Grawe thesis)
#   NUM_PROCESSORS - (optional) number of cores to use in the processing
# OUTPUTS:
#   energies_data - size (time, len(real_lam), len(theta)) array containing the energy in a specified Gabor filter kernel at each time step
#   LAM - size (len(real_lam), len(theta)) array containing the meshgrid of wavelengths
#   THETA - size (len(real_lam), len(theta)) array containing the meshgrid of orientations

    # Set up the Gabor filter
    lam        = real_lam/subset['dr'].values # This dr must be what gets the spatial information!
    u          = 1./lam
    g          = gabor.Create_FilterBank_Gabor(n_stds, u, theta, Bf, Btheta)

    gabor.g_kernels = g

    # Need to run filter on data with the background removed
    background_subtract = subset.FilteredImageData - subset.FilteredImageData.mean(dim=['east','north'])
    gabor.data_frames = background_subtract.transpose('time','north','east')

    print("largest kernel size: " + str(g[-1][0].real.shape[1]) + " x " + str(g[-1][0].real.shape[0]) + " pixels")

    # Setup the grid of wavelengths and orientations
    THETA, LAM = np.meshgrid(np.degrees(theta), real_lam/1000)

    energies_data = []

    # Setup multiprocessing and run the analysis
    pool = mp.Pool(processes = NUM_PROCESSORS)
    energies_data = pool.map(gabor.Energies_Direct_PLL, np.arange(0,len(background_subtract.time)))

    return energies_data, LAM, THETA

def run_rcp(subset, mask):
# Run the rolling cross periodogram process (see Ch 3.3 of Grawe thesis)
# INPUTS:
#   subset - xarray data structure containing information on the dataset to be processed with at least the following format:
#       Coordinates:
#           time (datetime64) - time stamps of each image in the data set
#           east (float) - east km distance from the center of the image. This is a uniform step.
#           north (float) - north km distance from the center of the image. This is a uniform step.
#       Data Variable:
#           GaborFilteredImageData (float) - intensity values in the image (dims: time, north, east)
#                                       which has been run through specific Gabor filters to isolate a wave of interest
#           dr (float) - resolution in meters of the projected image data
#   mask - array (of length corresponding to time in subset) containing True/False for whether a specific image should be analyzed
# OUTPUTS:
#   periods_output - array (of length corresponding to time in subset) of estimated periods (minutes)
#   wavelengths_output - array (of length corresponding to time in subset) of estimated wavelengths (km)
#   orientations_output - array (of length corresponding to time in subset) of estimate orientations (deg)
#   wavespeeds_output - array (of length corresponding to time in subset) of estimate wavespeeds (m/s)

    dx = subset['dr'].values
    dy = subset['dr'].values


    periods_output = []
    wavelengths_output = []
    orientations_output = []
    wavespeeds_output = []

    # First entry is nan
    periods_output.append(np.nan)
    wavelengths_output.append(np.nan)
    orientations_output.append(np.nan)
    wavespeeds_output.append(np.nan)

    # Now run the cross periodogram on consecutive images
    for i in np.arange(0,len(subset['time'])-1):
        if mask[i]:
            # The two frames we are using
            frame1 = subset['GaborFilteredImageData'].isel(time=i)
            frame2 = subset['GaborFilteredImageData'].isel(time=i+1)
        
            # Time step
            dt    = pd.to_timedelta((subset['time'].isel(time=i+1) - subset['time'].isel(time=i)).values)
        
            # Windowing
            sx, sy = frame1.shape[1], frame1.shape[0]
            windowx, windowy = np.hamming(sx), np.hamming(sy) ## hamming window
            win2D  = np.outer(windowy, windowx) # this may be backwards
            
            frame1 *= win2D ## smooth with hamming window
            frame2 *= win2D ## size = 599*602
        
            P2 = fft.fftshift(fft.fft2(frame2, (1024, 1024))) ## Y2[kx,ky]
            P1 = fft.fftshift(fft.fft2(frame1, (1024, 1024))) ## Y1[kx,ky]
            nx, ny = P1.shape[1], P1.shape[0] ## n-point DFT, nx=ny=1024
            ns = float(nx*ny)
            
            C  = P1*P2.conj()/ns ## Cross-spectural density SY1Y2[k] (3.20), size = 1024*1024
            
            kx = np.linspace(-np.pi/(dx), np.pi/(dx), nx) ## normalize in angular space
            ky = np.linspace(-np.pi/(dy), np.pi/(dy), ny)
            KX, KY = np.meshgrid(kx*1000, ky*1000)
            absC = np.abs(C)
        
            # # kx and ky index of the peak in the cross-periodogram
            # kym, kxm = peak_local_max(absC[512:,:], min_distance = 1, num_peaks = 1)[0] # was [512:,:512]
        
            # # kx and ky values of the peak in the cross-periodogram
            # kym=kym+512
            
            kym, kxm = peak_local_max(absC, min_distance = 1, num_peaks = 1)[0]
            kxmax, kymax = KX[kym, kxm], KY[kym, kxm]
        
            # Get the mirror image location
            kym2 = ny - kym
            kxm2 = nx - kxm
            kxmax2, kymax2 = KX[kym2-1, kxm2-1], KY[kym2-1, kxm2-1]
        
            phase = np.angle(C)[kym, kxm]
            k    = np.sqrt(kxmax**2 + kymax**2)
            ori  = np.degrees(np.arctan2(kymax,kxmax))
            if phase < 0:
                phase = np.angle(C)[kym2, kxm2]
                k    = np.sqrt(kxmax2**2 + kymax2**2)
                ori  = np.degrees(np.arctan2(kymax2,kxmax2))
                # Need to use the other peak
                kxmax = kxmax2
                kymax = kymax2
        
            lamb = 2*np.pi/k*1000
        
            delr = lamb*phase/(2*np.pi) ## distance @ peak
            ws   = delr/dt.total_seconds() ## wavespeed @ peak
        
            # note: potential issues with 360 ambiguity resolution
            # and the use of pi phase unwrapping
        
            T = lamb/ws/60. ## period @ peak

            periods_output.append(T)
            wavelengths_output.append(lamb/1000.)
            orientations_output.append(np.degrees(np.arctan2(kymax,kxmax)))
            wavespeeds_output.append(ws)
        else:
            periods_output.append(np.nan)
            wavelengths_output.append(np.nan)
            orientations_output.append(np.nan)
            wavespeeds_output.append(np.nan)

    return periods_output, wavelengths_output, orientations_output, wavespeeds_output
