# PIL/Pillow plugin for the custom "CDF" TIF format (Garcia Image Format).
# See https://pillow.readthedocs.io/en/stable/handbook/writing-your-own-image-plugin.html
#
# File layout:
#   bytes   0-199 : standard TIFF header (so generic viewers can display it)
#   bytes 200-599 : custom "CDF" header (parsed below, little-endian)
#   bytes 600-    : raw 16-bit little-endian pixel data

import datetime
import struct

from PIL import Image, ImageFile

_HEADER_OFFSET = 200
_DATA_OFFSET = 600


def _cstr(b):
    """Convert a NUL-terminated byte field to a str."""
    return b.split(b'\x00', 1)[0].decode('latin-1').strip()


def _tm(fp):
    """Read a C 'struct tm' stored as 9 x 32-bit ints (read as 18 shorts)."""
    t = struct.unpack('<18h', fp.read(2 * 18))
    return datetime.datetime(t[10] + 1900, t[8] + 1, t[6], t[4], t[2], t[0])


def _read_header(fp):
    """Parse the CDF header. Returns (rows, cols, info) or raises SyntaxError."""
    fp.seek(_HEADER_OFFSET)
    if fp.read(3) != b'CDF':
        raise SyntaxError("not a CDF TIF file")

    def rd(fmt):
        fmt = '<' + fmt
        return struct.unpack(fmt, fp.read(struct.calcsize(fmt)))

    rows, cols = rd('2h')
    pmin, pmax = rd('2h')
    LocalTime = _tm(fp)
    UniversalTime = _tm(fp)
    Filter = _cstr(fp.read(12))
    EmissionHeight, = rd('f')
    ExposureTime, = rd('f')
    gain, XBinning, YBinning = rd('3B')
    CCDTemperature, = rd('f')
    FWTemperature, = rd('f')
    XSpacing, YSpacing = rd('2f')
    Calibration = rd('6d')  # a0, a1, a2, b0, b1, b2
    lat, lon, alt = rd('3f')
    Observatory = (_cstr(fp.read(30)), lat, lon, alt)
    Comment = _cstr(fp.read(100))
    isAllSky, = rd('B')
    CenterAz, CenterEl = rd('2d')
    ProjectionAltitude, = rd('f')
    ProjectionLon, = rd('f')

    info = {'LocalTime': LocalTime, 'UniversalTime': UniversalTime,
            'pmin': pmin, 'pmax': pmax, 'Filter': Filter,
            'EmissionHeight': EmissionHeight, 'ExposureTime': ExposureTime,
            'gain': gain, 'XBinning': XBinning, 'YBinning': YBinning,
            'CCDTemperature': CCDTemperature, 'FWTemperature': FWTemperature,
            'XSpacing': XSpacing, 'YSpacing': YSpacing,
            'Calibration': Calibration, 'Observatory': Observatory,
            'Comment': Comment, 'isAllSky': isAllSky, 'CenterAz': CenterAz,
            'CenterEl': CenterEl, 'ProjectionAltitude': ProjectionAltitude,
            'ProjectionLon': ProjectionLon}
    return rows, cols, info


def _accept(prefix):
    # These files start with a normal TIFF signature; the real check
    # (b'CDF' at offset 200) happens in _open.
    return prefix[:4] in (b'II*\x00', b'MM\x00*')


class TifImageFile(ImageFile.ImageFile):

    format = "TIF"
    format_description = "Garcia Image Format"

    def _open(self):
        rows, cols, info = _read_header(self.fp)
        self.info.update(info)

        # size in pixels (width, height) = (cols, rows).
        # (The old code used (rows, cols), which only works for square images.)
        self._size = (cols, rows)
        self._mode = "I;16L"   # 'mode' is a read-only property in Pillow >= 10

        args = (self.mode, 0, 1)
        if hasattr(ImageFile, '_Tile'):          # Pillow >= 11
            self.tile = [ImageFile._Tile("raw", (0, 0) + self.size, _DATA_OFFSET, args)]
        else:
            self.tile = [("raw", (0, 0) + self.size, _DATA_OFFSET, args)]


def TifImageMetadata(filename):
    """Return just the header dictionary without reading pixel data."""
    with open(filename, 'rb') as fp:
        return _read_header(fp)[2]


def register():
    """Register the plugin so it is tried BEFORE Pillow's built-in TIFF reader.

    Because these files carry a valid TIFF signature, Pillow's own TIFF
    plugin would otherwise claim them first.
    """
    Image.init()  # make sure built-in plugins are loaded, then jump the queue
    Image.register_open(TifImageFile.format, TifImageFile, _accept)
    if TifImageFile.format in Image.ID:
        Image.ID.remove(TifImageFile.format)
    Image.ID.insert(0, TifImageFile.format)


register()
