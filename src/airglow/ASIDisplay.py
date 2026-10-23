import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import numpy.ma as ma
from matplotlib import dates
from PIL import Image
from scipy import ndimage
from scipy.spatial import Delaunay

import airglow.TifImagePlugin  # noqa: F401  (registers the CDF TIF reader)


def _load(f, dark=None):
    """Read an ASI TIF image.

    Returns (im, info) where im is a float64 array of shape (rows, cols) with
    the dark (if given) subtracted, and info is the header dictionary.
    Working in float avoids uint16 wrap-around when subtracting darks.
    """
    with Image.open(f) as d:
        info = dict(d.info)
        im = np.asarray(d, dtype=float)
    if dark is not None:
        im = im - dark
    return im, info


def MeanDark(darks):
    """Return the mean of a list of dark image files (float array)."""
    total = None
    for dk in darks:
        im, _ = _load(dk)
        total = im if total is None else total + im
    return total / len(darks)


def DisplayRaw(f, cmin=None, cmax=None, dark=None, flips=None, info=True, sitename=None, filt=None, kernel_size=1):
    # Function to display information about a raw ASI TIF image and
    # plot the raw data
    #
    # INPUTS:
    #   f - full path name to image to display
    #
    # OPTION INPUTS:
    #   cmin, cmax - color limits (min, max) to scale the resultant image by
    #   dark - a dark image (array) to subtract from the requested image
    #   flips - list of flips to get the orientation of the images right.
    #           lr : fliplr,  ud : flipud
    #   info - flag on whether to print info or not
    #   sitename, filt - sitename and filter to put in image title.  If not given, reads from header
    #   kernel_size - size of the median filter kernel used (default = 1, i.e., no filtering).
    #                 Only applied when no dark image is given.
    #
    # OUTPUT:
    #   f - a reference to the figure created
    #
    # HISTORY:
    #   Written by Jonathan J. Makela on 2 July 2013

    # Load the image
    im, hdr = _load(f, dark)

    # Should we plot out header info?
    if info:
        print('Filename: %s' % f)
        print('Exposure time: %.1f s' % hdr['ExposureTime'])
        print('CCD Temperature: %.1f C' % hdr['CCDTemperature'])

    # Define the figure and plot
    fig = plt.figure(num=None, figsize=(6, 6), dpi=80)
    p = fig.add_subplot(111)

    if dark is None:
        # No Dark image used.  Run a small median filter to remove any
        # noise spikes from the CCD
        im = ndimage.median_filter(im, size=kernel_size)

    # Do we need to flip the image?
    if flips is not None:
        for flip in flips:
            if flip == 'lr':
                im = np.fliplr(im)
            if flip == 'ud':
                im = np.flipud(im)

    # Create the image display
    a = p.matshow(im, cmap=cm.gray)

    # Set color limits
    if cmin is not None and cmax is not None:
        a.set_clim([cmin, cmax])
    else:
        a.set_clim(np.percentile(im, (5, 95)))

    # Create the title and colorbar
    if sitename is None:
        sitename = hdr['Observatory'][0]
    if filt is None:
        filt = hdr['Filter']

    p.set_title('%s (%s): %s' % (sitename, filt, hdr['LocalTime'].strftime('%d %b %Y %H:%M:%S LT')))
    fig.colorbar(a, orientation='vertical', shrink=0.8)

    # Turn off the axis
    p.axes.get_xaxis().set_visible(False)
    p.axes.get_yaxis().set_visible(False)

    # Improve display
    fig.tight_layout()

    plt.close(fig)

    return fig


def DisplayMap(f, m, lat, lon, mask, cmin=None, cmax=None, dark=None, sitename=None, filt=None, kernel_size=5, displayUT=False, displayColorbar=True):
    # Function to display an image on a provide map projection.
    #
    # INPUTS:
    #   f - full path name to the image to display
    #   m - a fully defined Basemap onto which the image will be displayed.
    #       m = Basemap(llcrnrlon=275.,llcrnrlat=-30.,urcrnrlon=300.,urcrnrlat=-13.,
    #            projection='merc', area_thresh=1000,
    #            resolution='i',lat_1=35.,lat_2=55,lat_0=40,lon_0=-85.)
    #   lat, lon - the lat, lon for each pixel in the image (must be the same shape as the
    #              imaging data defined in f).
    #   mask - boolean array (same shape as the image); True pixels are not displayed.
    #
    # OPTIONAL INPUTS:
    #   cmin, cmax - color limits (min, max) to scale the resultant image by
    #   dark - a dark image (array) to subtract from the requested image
    #   sitename, filt - sitename and filter to put in image title.  If not given, reads from header
    #   kernel_size - size of median filter kernel used (default = 5)
    #
    # HISTORY:
    #   Written by Jonathan J. Makela on 9 July 2013

    # Load the image and median filter it to remove noise spikes from the CCD
    im, hdr = _load(f, dark)
    if im.shape != np.shape(lat):
        raise ValueError('Image shape %s does not match calibration shape %s'
                         % (im.shape, np.shape(lat)))
    im = ndimage.median_filter(im, size=kernel_size)

    # pcolormesh can't take NaN coordinates. Mask any pixel with (or next to)
    # a NaN coordinate, then give those coordinates a harmless finite value.
    bad = ~(np.isfinite(lat) & np.isfinite(lon))
    if bad.any():
        mask = np.asarray(mask) | ndimage.binary_dilation(bad)
        lat = np.where(bad, np.nanmean(lat), lat)
        lon = np.where(bad, np.nanmean(lon), lon)

    # Mask the image data
    zdata = ma.masked_where(mask, im)

    # Convert to the map's coordinates
    xpt, ypt = m(lon, lat)

    # Create the plot. Matplotlib >= 3.5 requires the data to be one smaller
    # than the coordinates for shading='flat'; dropping the last row/column
    # reproduces what older matplotlib did implicitly. ('nearest' would
    # need monotonic coordinates, which a fisheye projection doesn't have.)
    a = m.pcolormesh(xpt, ypt, zdata[:-1, :-1], shading='flat', cmap=cm.gray)

    # Set the colorlimits
    if cmin is not None and cmax is not None:
        a.set_clim([cmin, cmax])
    else:
        a.set_clim(np.percentile(im, (5, 95)))

    # Create the title and colorbar
    if sitename is None:
        sitename = hdr['Observatory'][0]
    if filt is None:
        filt = hdr['Filter']

    if displayUT:
        plt.title('%s (%s): %s' % (sitename, filt, hdr['UniversalTime'].strftime('%d %b %Y %H:%M:%S UT')))
    else:
        plt.title('%s (%s): %s' % (sitename, filt, hdr['LocalTime'].strftime('%d %b %Y %H:%M:%S LT')))

    if displayColorbar:
        plt.colorbar(a, orientation='vertical', shrink=0.9)


def _cut(lat, lon, sel, grid_x, grid_y):
    """Precompute a linear interpolator for a cut through the image.

    sel - boolean mask of the pixels near the cut
    Returns a function mapping a filtered image to values on (grid_x, grid_y).
    """
    # Equivalent to griddata(..., method='linear'), but the triangulation and
    # the barycentric weights are computed only once for the whole night.
    tri = Delaunay(np.column_stack((lon[sel], lat[sel])))
    gx, gy = np.meshgrid(grid_x, grid_y)
    q = np.column_stack((gx.ravel(), gy.ravel()))
    simplex = tri.find_simplex(q)
    outside = simplex < 0
    T = tri.transform[simplex]
    b = np.einsum('ijk,ik->ij', T[:, :2], q - T[:, 2])
    weights = np.column_stack((b, 1 - b.sum(axis=1)))
    verts = tri.simplices[simplex]

    def interp(img):
        out = np.einsum('ij,ij->i', img[sel][verts], weights)
        out[outside] = np.nan
        return out
    return interp


def Keogram(files, lat, lon, target_lat, target_lon, darks=None, sitename=None, filt=None, cmin=None, cmax=None, kernel_size=5):
    # Function to generate keograms through the requested images at the requested lat/lon.
    #
    # INPUTS:
    #   files - list of files to generate keogram from (Assumed a glob list)
    #   lat, lon - the lat, lon for each pixel in the image (must be the same size as the imaging
    #             data defined in files.  These values should have NaN in pixels outside of the
    #             field of view of the imager
    #   target_lat, target_lon - the lat/lon at which the cuts will be made through the images to
    #                            generate the keograms.
    #   cmin, cmax - the min/max values for the color map
    #
    # OPTIONAL INPUTS:
    #   darks - list of files to be considered for dark image subtraction.
    #   sitename, filt - sitename and filter to put in image title.  If not given, reads from header
    #   kernel_size - size of the kernel for the median filter applied to the data (default = 5)
    #
    # OUTPUT:
    #   f - a reference to the figure generated
    #
    # HISTORY:
    #   Written by Jonathan J. Makela on 10 July 2013

    # Create a master dark image if requested
    dark = MeanDark(darks) if darks else None

    # The cuts are the same for every image, so set up the interpolation once.
    # Pixels with NaN lat/lon compare False and are excluded automatically.
    with np.errstate(invalid='ignore'):
        sel_lat = np.abs(lat - target_lat) < 0.5    # east-west cut
        sel_lon = np.abs(lon - target_lon) < 0.5    # north-south cut
    lo = np.arange(lon[sel_lat].min(), lon[sel_lat].max(), 0.1)
    la = np.arange(lat[sel_lon].min(), lat[sel_lon].max(), 0.1)
    cut_lat = _cut(lat, lon, sel_lat, lo, [target_lat])
    cut_lon = _cut(lat, lon, sel_lon, [target_lon], la)

    # Variables to hold the generated keograms
    keo_lat = []
    keo_lon = []
    ut = []
    all_min = []
    all_max = []

    for f in files:
        im, hdr = _load(f, dark)
        if im.shape != lat.shape:
            raise ValueError('%s: image shape %s does not match calibration shape %s'
                             % (f, im.shape, lat.shape))

        # Grab the sitename and filter if it wasn't provided
        if sitename is None:
            sitename = hdr['Observatory'][0]
        if filt is None:
            filt = hdr['Filter']

        if cmin is None:
            all_min.append(hdr['pmin'])
            all_max.append(hdr['pmax'])

        # Get the Universal Time of image
        ut.append(hdr['UniversalTime'])

        # Median filter the data and cut through it
        im = ndimage.median_filter(im, size=kernel_size)
        keo_lon.append(cut_lat(im))
        keo_lat.append(cut_lon(im))

    # Cast the outputs as arrays
    keo_lon = np.array(keo_lon)
    keo_lat = np.array(keo_lat)
    ut = np.array(ut)

    # Calculate a guess for cmin/cmax
    if cmin is None:
        cmin = np.median(all_min)
        cmax = np.median(all_max)
        if dark is not None:
            cmin -= np.median(dark)
            cmax -= np.median(dark)
        m = np.nanmedian(keo_lat)
        s = np.nanstd(keo_lat)
        cmin = np.max([cmin, m - 2. * s, 0])
        cmax = np.min([cmax, m + 2. * s])

    # Generate the image and plot the first keogram
    f = plt.figure(num=None)
    p = f.add_subplot(211)
    a = p.imshow(keo_lon.transpose(), aspect='auto', extent=(dates.date2num(ut[0]), dates.date2num(ut[-1]), lo[-1], lo[0]), cmap=cm.gray)
    p.xaxis_date()

    # Format the axis
    hfmt = dates.DateFormatter('%H:%M')
    p.xaxis.set_major_locator(dates.HourLocator(interval=1))
    p.xaxis.set_major_formatter(hfmt)
    plt.ylabel('Longitude')

    # Format the title
    if ut[0].date() == ut[-1].date():
        # Data does not span a day
        tstr = '%s (%s): %s' % (sitename, filt, ut[0].strftime('%d %b %Y'))
    elif ut[0].date().month == ut[-1].date().month:
        # Date spans a day, but not a month:
        tstr = '%s (%s): %s-%s' % (sitename, filt, ut[0].strftime('%d'), ut[-1].strftime('%d %b %Y'))
    elif ut[0].date().year == ut[-1].date().year:
        # Date spans a month, but not a year:
        tstr = '%s (%s): %s-%s' % (sitename, filt, ut[0].strftime('%d %b'), ut[-1].strftime('%d %b %Y'))
    else:
        # Date spans a year
        tstr = '%s (%s): %s-%s' % (sitename, filt, ut[0].strftime('%d %b %Y'), ut[-1].strftime('%d %b %Y'))

    plt.title(tstr)
    a.set_clim([cmin, cmax])

    # Generate the second keogram
    p = f.add_subplot(212)
    a = p.imshow(keo_lat.transpose(), aspect='auto', extent=(dates.date2num(ut[0]), dates.date2num(ut[-1]), la[-1], la[0]), cmap=cm.gray)
    p.xaxis_date()

    # Format the axis
    hfmt = dates.DateFormatter('%H:%M')
    p.xaxis.set_major_locator(dates.HourLocator(interval=1))
    p.xaxis.set_major_formatter(hfmt)
    plt.xlabel('UT')
    plt.ylabel('Latitude')
    a.set_clim([cmin, cmax])
    plt.gca().invert_yaxis()

    return f
