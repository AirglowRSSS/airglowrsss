#!/usr/bin/python
"""Script to Zip, Split and Move recent data to Sending Folder.

CLI flags:
    -s SITE        3-letter site code
    -i INSTRUMENT  instrument code: fpi, bwc, nfi, asi, pic, sky, scn, tec, x3t
    -n NUMBER      instrument number/letter (zero-padded to 2 chars)
    -p DAYSPRIOR   number of prior days to process (default 1)
    -y YEAR        specific year (used with -d)
    -d DOY         specific day-of-year (used with -y)
    -S SEARCH_DIR  directory to search for all .img files

History: 2 Oct 2012 - initial script written
        17 Jul 2012 - new server update
        12 Feb 2014 - Updated to v3.0 - txtcheck
        31 Mar 2016 - Added email addresses so emails can be sent directly
        31 Aug 2020 - Added search function by L. Navarro

Written by Daniel J. Fisher (dfisher2@illinois.edu)
Refactored by Jonathan J. Makela (jmakela@illinois.edu) with CLAUDE on Apr 6, 2026
"""

# Dependencies:
#   stdlib:     argparse, datetime, logging, os, shutil, subprocess, sys,
#               tarfile, time, urllib.request, collections, dataclasses,
#               glob, itertools, pathlib
#   third-party: pytz, Pillow (PIL) [optional, searcher() only]
#   local:      Emailer, X300Sensor [optional, x3t only],
#               ImgImagePlugin [optional, searcher() only]

import argparse
import datetime as dt
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from glob import glob
from itertools import groupby
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

import pytz
import Emailer

# PIL/Pillow — optional; only needed by searcher()
try:
    from PIL import Image
    import ImgImagePlugin
except ImportError:
    Image = None
    ImgImagePlugin = None

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MIN_FILE_SIZE_BYTES: int = 1_000
SPLIT_CHUNK_BYTES: int = 5 * 1024 * 1024  # 5 MB chunks
FPI_DAY_START_HOUR: int = 12              # local noon — FPI night starts here


# ---------------------------------------------------------------------------
# DateBundle
# ---------------------------------------------------------------------------
@dataclass
class DateBundle:
    """Pre-computed date strings for a single processing day.

    Attributes:
        date: The data day (yesterday, or specified).
        next_date: Data day + 1.
        today: Actual today.
    """

    date: dt.date
    next_date: dt.date
    today: dt.date

    @property
    def year(self) -> str:
        return self.date.strftime('%Y')

    @property
    def yr(self) -> str:
        return self.date.strftime('%y')

    @property
    def month(self) -> str:
        return self.date.strftime('%m')

    @property
    def mon(self) -> str:
        return self.date.strftime('%b')

    @property
    def day(self) -> str:
        return self.date.strftime('%d')

    @property
    def doy(self) -> str:
        return self.date.strftime('%j')

    @property
    def nday(self) -> str:
        return self.next_date.strftime('%d')

    @property
    def tyear(self) -> str:
        return self.today.strftime('%Y')

    @property
    def tmonth(self) -> str:
        return self.today.strftime('%m')

    @property
    def tday(self) -> str:
        return self.today.strftime('%d')


# ---------------------------------------------------------------------------
# activeinstruments — content frozen, do not modify
# ---------------------------------------------------------------------------
def activeinstruments():
    '''
    Summary:
        code = activeinstruments()
        Is the dictionary for use in DATAMANAGEMENT ONLY of all active instruments (site-instr-num) with:
            send_dir:  Sending Directory
            local_dir: Data Directory
            email:   Email list who wants warnings
            split:   Location of split command [for FPI and ASI]
            url:       Address of temp logs [for X3T]

    Outputs:
        code =  dictionary of all active site/instrument/num info

    History:
        2/25/14 -- Written by DJF (dfisher2@illinois.edu)
        3/31/16 -- Added emails DJF
    '''

    # Set up site dictionary: Comment out or delete non-active instrument/sites
    # This dictionary has 2 functions:
    # 1. for data-taking computer list the path to the data directories and special functions
    # 2. for remote2 server for emailing person of interest.

    # Email list
    UIemail = ['jmakela@illinois.edu','bharding@ssl.berkeley.edu']
    BZemail = ['rburiti.ufcg@gmail.com']
    AKemail = []
    MOemail = ['zouhair@uca.ac.ma']

    # The dictionary!
    code = defaultdict(lambda: defaultdict(dict))

#    code['uao']['fpi']['05'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin64/bin/split','email':UIemail}
#    code['uao']['fpi']['05'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/collected-data/', 'split':'/usr/bin/split','email':UIemail}
    code['uao']['fpi']['05'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/mnt/data/', 'split':'/usr/bin/split','email':UIemail}
    #code['uao']['sky']['01'] = {'send_dir':'D:/Sending/', 'local_dir':'D:/Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
#    code['uao']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/MiniME/Documents/Interactiveastronomy/SkyAlert/','email':UIemail}
    code['uao']['bwc']['00'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/skyalert-logger/skyalert-logs/', 'email':UIemail}
    code['uao']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.1.23/log.txt','email':UIemail}

    # EKU is being moved as of 2-17-2016
    #code['eku']['fpi']['07'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    #code['eku']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/meriwej/My Documents/ClarityII/','email':UIemail}
    #code['eku']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://157.89.43.12/log.txt','email':UIemail}

    # ANN offline ATM
    #code['ann']['fpi']['08'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    #code['ann']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/fpi/My Documents/ClarityII/','email':UIemail}
    #code['ann']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.0.102/log.txt','email':UIemail}

    # PAR deconstructed and being sent to S Africa 2 Aug 2017
    #code['par']['fpi']['06'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    #code['par']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/meriwej/My Documents/ClarityII/','email':UIemail}
    #code['par']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://10.20.1.2/log.txt','email':UIemail}
    #code['par']['fpi']['09'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}

    # SAO site is a copy of PAR
    code['sao']['fpi']['06'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    code['sao']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/staff/My Documents/ClarityII/','email':UIemail}
    code['sao']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.0.110/log.txt','email':UIemail}

    # VTI offline
    #code['vti']['fpi']['09'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    #code['vti']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/meriwej/My Documents/ClarityII/','email':UIemail}
#   code['vti']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://10.20.1.2/log.txt','email':UIemail}

#   code['caj']['fpi']['02'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail+BZemail}
#   code['caj']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/MiniME/My Documents/ClarityII/','email':UIemail+BZemail}
#   code['caj']['pic']['05'] = {'send_dir':'/data/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail+BZemail}
#   code['caj']['scn']['0U'] = {'send_dir':'/home/scintmon/Sending/', 'local_dir':'/home/scintmon/cascade-1.62/', 'split':'/usr/bin/split','email':UIemail+BZemail}
#   code['caj']['tec']['02'] = {'send_dir':'/home/gps/Sending/', 'local_dir':'/home/gps/data/', 'split':'/usr/bin/split','email':UIemail+BZemail}
#   code['caj']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.1.2/log.txt','email':UIemail+BZemail}

    # Car offline due to water damage Feb 5 2018
    #code['car']['fpi']['01'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail+BZemail}
    #code['car']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/MiniME/My Documents/ClarityII/','email':UIemail+BZemail}
    # code['car']['scn']['0S'] = {'send_dir':'F:/Sending/', 'local_dir':'F:/Data/ScintmonS/', 'split':'/usr/bin/split','email':UIemail+BZemail}
    # code['car']['scn']['0T'] = {'send_dir':'F:/Sending/', 'local_dir':'F:/Data/ScintmonT/', 'split':'/usr/bin/split','email':UIemail+BZemail}
    #code['car']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.1.2/log.txt','email':UIemail+BZemail}

    # Morocco is back online 17 July 2018
    code['mor']['fpi']['03'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    code['mor']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/admin/Documents/ClarityII/','email':UIemail}
#   code['mor']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.1.204/log.txt','email':UIemail}
#   code['mor']['pic']['04'] = {'send_dir':'/data/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail}

    #code['bog']['scn']['0O'] = {'send_dir':'/home/gps/Sending/', 'local_dir':'/home/gps/cascade/', 'split':'usr/bin/split','email':UIemail}

    # CTO is broken, CCD overheats and Filterwheel does not work
    #code['cto']['pic']['02'] = {'send_dir':'/home/airglow/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail}
    #code['cto']['scn']['0L'] = {'send_dir':'/home/gps/Sending/', 'local_dir':'/home/gps/cascade/', 'split':'/usr/bin/split','email':UIemail}
    #code['cto']['scn']['0M'] = {'send_dir':'/home/gps/Sending/', 'local_dir':'/home/gps2/cascade/', 'split':'/usr/bin/split','email':UIemail}

    # CASI stopped sending data 2/10/2019
#    code['hka']['asi']['01'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    code['hka']['nfi']['01'] = {'send_dir':'/home/airglow/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail}
    #code['hka']['scn']['0K'] = {'send_dir':'/home/gps/Sending', 'local_dir':'/home/gps/cascade', 'split':'/usr/bin/split','email':UIemail}
#   code['hka']['cas']['01'] = {'send_dir':'/mnt/data/Sending/', 'local_dir':'mnt/data/','split':'/usr/bin/split','email':UIemail}

    #REMOVED Feb 27, 2017
    #code['tht']['pic']['06'] = {'send_dir':'/home/airglow/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail}

    #NSO now in Texas April 10, 2017
    #code['nso']['pic']['03'] = {'send_dir':'/home/airglow/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail}

    # pic03 currently being shipped to UTA
#   code['uta']['pic']['03'] = {'send_dir':'/home/airglow/Sending/', 'local_dir':'/data/', 'split':'/usr/bin/split','email':UIemail}


    #code['kaf']['fpi']['04'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    #code['kaf']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/???/My Documents/ClarityII/','email':UIemail}


    # BDR has been offline for a while -- originally turned off for rainy season (7 Sep 2017 BJH)
    # It seems BDR came back online for a few days. Turning back on to process these. (16 Jan 2019 BJH)
    # Disabling BDR once more (20 jan 2019)
    #code['bdr']['fpi']['94'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/FPIData/', 'split':'/usr/bin/split','email':UIemail}
    #code['bdr']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Documents and Settings/User/My Documents/ClarityII/','email':UIemail}
    #code['bdr']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.1.204/log.txt','email':UIemail}

#   code['arg']['fpi']['93'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/FPIData/', 'split':'/usr/bin/split','email':UIemail}
#   code['arg']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/meriwej/Documents/ClarityII/','email':UIemail}
    #code['arg']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.1.204/log.txt','email':UIemail}
    #code['xxx']['xxx']['00'] = {'local_dir':'.','email':UIemail}

    # Kwaj FPI
    code['kwj']['fpi']['09'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'/usr/bin/split','email':UIemail}
    code['kwj']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/User/My Documents/ClarityII/','email':UIemail}
    #code['kwj']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Scripts/Python/modules/', 'url':'http://192.168.0.2/log.txt','email':UIemail}

    # Leo FPI
    code['leo']['fpi']['80'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin/bin/split','email':UIemail}
    code['leo']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'Z:/','email':UIemail}

    # DASI FPI at Lowell Observatory, Arizona starting from 12 Aug 2021
#    code['low']['fpi']['11'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin64/bin/split','email':UIemail}
#    code['low']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/DASI03/Documents/Interactiveastronomy/SkyAlert/','email':UIemail}
#    code['low']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Scripts/Python/modules/', 'url':'http://raspberryfpi.local/log.txt','email':UIemail}
#    code['low']['x3t']['01'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Scripts/Python/modules/','url':'http://homeassistant.local:8123/api/history/period','email':UIemail}
    code['low']['fpi']['11'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/data/', 'split':'/usr/bin/split','email':UIemail}
    code['low']['bwc']['00'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/skyalert-logger/skyalert-logs/', 'email':UIemail}


    # DASI FPI at Bear Lake Observatory, Utah starting from 19 Aug 2021
#    code['blo']['fpi']['12'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin64/bin/split','email':UIemail}
#    code['blo']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/dasi02/Documents/Interactiveastronomy/SkyAlert/','email':UIemail}
#    code['blo']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Scripts/Python/modules/', 'url':'http://raspberryfpi.local/log.txt','email':UIemail}
#    code['blo']['x3t']['01'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Scripts/Python/modules/','url':'http://homeassistant.local:8123/api/history/period','email':UIemail}
    code['blo']['fpi']['12'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/data/', 'split':'/usr/bin/split','email':UIemail}
    code['blo']['bwc']['00'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/skyalert-logger/skyalert-logs/', 'email':UIemail}

    # DASI FPI at Christmas Valley, Oregon starting from 18? Nov 2021
#    code['cvo']['fpi']['10'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/FPI_Data/', 'split':'C:/cygwin64/bin/split','email':UIemail}
#    code['cvo']['bwc']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'C:/Users/dasi01/Documents/Interactiveastronomy/SkyAlert/','email':UIemail}
#    code['cvo']['x3t']['00'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Scripts/Python/modules/', 'url':'http://raspberryfpi.local/log.txt','email':UIemail}
#    code['cvo']['x3t']['01'] = {'send_dir':'C:/Sending/', 'local_dir':'/cygdrive/c/Scripts/Python/modules/','url':'http://homeassistant.local:8123/api/history/period','email':UIemail}
    code['cvo']['fpi']['10'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/data/', 'split':'/usr/bin/split','email':UIemail}
    code['cvo']['bwc']['00'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/skyalert-logger/skyalert-logs/', 'email':UIemail}

    # DASI FPI at UAO (testing for ALO) starting from April 1, 2026
    code['uao']['fpi']['13'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/data/', 'split':'/usr/bin/split','email':UIemail}
#    code['uao']['bwc']['00'] = {'send_dir':'/home/airglow/airglow/Sending/', 'local_dir':'/home/airglow/airglow/skyalert-logger/skyalert-logs/', 'email':UIemail}

    # Last index is for remote2 admin related issues (not site based)
    code['ADMIN']={'email':['jmakela@illinois.edu','bharding@ssl.berkeley.edu']}

    return(code)


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------
def zipper(name: Union[str, List[str]], filename: str) -> None:
    """Create a gzip-compressed tar archive.

    Args:
        name: Glob pattern string or list of file paths to include.
        filename: Output archive path (e.g. ``fpi05_uao_20240115.tar.gz``).
    """
    tar = tarfile.open(filename, "w:gz")
    files = glob(name) if isinstance(name, str) else name
    for oscar in files:
        tar.add(oscar)
    tar.close()


def _run_split(split_bin: str, filename: str, send_dir: str) -> None:
    """Run the system ``split`` command to chunk an archive.

    Args:
        split_bin: Path to the split binary.
        filename: Source archive filename (relative to cwd).
        send_dir: Destination directory; split pieces are placed here.

    Raises:
        RuntimeError: If split exits with a non-zero return code.
    """
    cmd = [
        split_bin, '-a', '6', '-b', str(SPLIT_CHUNK_BYTES), '-d',
        filename, str(Path(send_dir) / filename),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"split failed: {result.stderr}")


def splitter(site: str, instr: str, num: str, code: dict,
             filename: str, checkname: str, mfs: int) -> None:
    """Split a tar archive into chunks and write a checkfile to the sending directory.

    The checkfile uses a frozen five-line format parsed by the receiving end:
    line 1: first split-piece filename, line 2: number of pieces,
    line 3: archive size in bytes, line 4: UTC creation time, line 5: disk free (GB).

    Args:
        site: 3-letter site code.
        instr: Instrument code.
        num: Instrument number/letter.
        code: Active-instruments dictionary from ``activeinstruments()``.
        filename: Archive filename; must exist in the current working directory.
        checkname: Checkfile filename to write into ``send_dir``.
        mfs: Minimum file size (bytes); below this is treated as a collection error.
    """
    send_dir = code[site][instr][num]['send_dir']
    local_dir = code[site][instr][num]['local_dir']
    now = dt.datetime.utcnow()
    file_size = Path(filename).stat().st_size

    if file_size > mfs:
        _run_split(code[site][instr][num]['split'], filename, send_dir)
        # Disk space free in GB
        sv = os.statvfs(local_dir)
        df = round(float(sv.f_bavail * sv.f_frsize) / 1024 ** 3, 2)
        pieces = sorted(Path(send_dir).glob(filename + '*'))
        with open(Path(send_dir) / checkname, 'w') as check:
            check.write(
                pieces[0].name + '\n' + str(len(pieces)) + '\n' +
                str(file_size) + '\n' + str(now) + '\n' + str(df) + '\n'
            )
            check.write(
                '1: 1st file name\n2: Number of parts\n3: Size of tar.gz\n'
                '4: Time of Creation\n5: GB Disk Free'
            )
    else:
        log.warning('COLLECTION ERROR!!!')
        with open(Path(send_dir) / checkname, 'w') as check:
            check.write(filename + '\n0\n0\n' + str(now) + '\n999\n')
            check.write(
                '1: 1st file name\n2: Number of parts\n3: Size of tar.gz\n'
                '4: Time of Creation\n5: GB Disk Free'
            )


# ---------------------------------------------------------------------------
# Per-instrument handlers
# ---------------------------------------------------------------------------
def _zip_fpi(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Zip, split, and stage FPI data.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'fpi'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed (unused here, for signature uniformity).
    """
    local_dir = code[site][instr][num]['local_dir']
    filename = "%03s%02s_%s_%04s%02s%02s.tar.gz" % (instr, num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    os.chdir(local_dir)
    start = dt.datetime(int(dates.year), int(dates.month), int(dates.day), FPI_DAY_START_HOUR)
    end = start + dt.timedelta(1)

    name = [f for f in glob(os.path.join('*', '*.img'))
            if start < dt.datetime.fromtimestamp(os.path.getmtime(f)) < end]
    if not name:
        # Try the new format
        name = [f for f in glob(os.path.join('*', '*.hdf5'))
                if start < dt.datetime.fromtimestamp(os.path.getmtime(f)) < end]

    log.debug('%s', name)
    dirs_seen: List[str] = []
    for nn in name:
        d = os.path.dirname(nn)
        if d not in dirs_seen:
            dirs_seen.append(d)
    for d in dirs_seen:
        log.debug('Zipping from %s/%s', local_dir, d)

    zipper(name, filename)
    splitter(site, instr, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
    (Path(local_dir) / filename).unlink()


def _zip_asi(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Zip, split, and stage ASI data.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'asi'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    filename = "%03s%02s_%s_%04s%02s%02s.tar.gz" % (instr, num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    name = "%02s-%02s" % (dates.day, dates.nday)
    year_dir = Path(local_dir) / dates.year
    os.chdir(year_dir)

    tar = tarfile.open(filename, "w:gz")
    try:
        # Rename folder to doy before archiving, then rename back
        src = str(year_dir / (dates.mon + name))
        dst = str(year_dir / dates.doy)
        os.rename(src, dst)
        tar.add(dates.doy)
        os.rename(dst, src)
    except Exception as e:
        log.warning('!!! No Data: %s', e)
    tar.close()

    splitter(site, instr, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
    (year_dir / filename).unlink()


def _zip_x3t(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Download and stage X300 temperature sensor data.

    Supports both homeassistant (HTTP API) and direct X300/raspberry Pi URLs.
    X300Sensor is imported dynamically here since it is only available on x3t
    instrument computers.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'x3t'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    send_dir = code[site][instr][num]['send_dir']
    url = code[site][instr][num]['url']
    filename = "TempL%02s_%s_%04s%02s%02s.txt" % (num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    if local_dir not in sys.path:
        sys.path.append(local_dir)
    try:
        import X300Sensor
    except ImportError as e:
        log.error('Could not import X300Sensor from %s: %s', local_dir, e)
        return

    localpath = Path(local_dir) / 'X300_temp_log.txt'
    if localpath.exists():
        localpath.unlink()

    if 'api/history' in url:
        log.info('homeassistant starts...')
        sys.path.append('C:\\Scripts')
        try:
            from read_history_hass import query_sensor
            d0 = (dt.datetime.now() - dt.timedelta(days=30)).replace(tzinfo=pytz.UTC)
            data = query_sensor('sensor.lumi_lumi_weather_temperature', begin=d0)
            with open(localpath, 'w') as h:
                h.write("date,temp#hass\n")
                for _date, _temp in data:
                    h.write("%s,%.2f\n" % (_date.strftime("%Y/%m/%d %H:%M:%S"), _temp))
        finally:
            sys.path.pop()
    else:
        log.info('try reading from rpi or x300')
        for _ in range(5):
            try:
                urllib.request.urlretrieve(url, str(localpath))
                if localpath.stat().st_size > 100:
                    log.info('success!')
                    break
            except Exception as e:
                log.debug('urlretrieve attempt failed: %s', e)
            time.sleep(1)

    localpath.chmod(0o777)

    args = [
        dt.datetime(int(dates.year), 1, 1) + dt.timedelta(days=int(dates.doy) - 1),
        local_dir,
        send_dir,
        site,
    ]
    if 'api/history' in url or 'raspberry' in url:
        kwargs: dict = {'num': num, 'dtfmt': "%Y/%m/%d"}
    else:
        kwargs = {}
    X300Sensor.WriteLogFromRaw(*args, **kwargs)

    # Check filesize for offline X300
    send_file = Path(send_dir) / filename
    if send_file.stat().st_size < 100:
        log.warning('No Data ERROR!!!')
        now = dt.datetime.utcnow()
        with open(Path(send_dir) / checkname, 'w') as check:
            check.write(filename + '\n0\n0\n' + str(now) + '\n999\n')
            check.write(
                '1: 1st file name\n2: Number of parts\n3: Size of tar.gz\n'
                '4: Time of Creation\n5: GB Disk Free'
            )

    # Get as much current day as possible to cover all night
    if prior == 1:
        args[0] = dt.datetime(int(dates.year), 1, 1) + dt.timedelta(days=int(dates.doy))
        X300Sensor.WriteLogFromRaw(*args, **kwargs)


def _zip_bwc(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Copy cloud-cover (BWC/SkyAlert) data to the sending directory.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'bwc'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    send_dir = code[site][instr][num]['send_dir']
    filename = "Cloud_%s_%04s%02s%02s.txt" % (site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)
    name = "%04s-%02s-%02s.txt" % (dates.year, dates.month, dates.day)

    is_skyalert = "SkyAlert" in local_dir
    if is_skyalert:
        name = "/Weatherdata Files/%02s-%02s-%04s.txt" % (dates.month, dates.day, dates.year)

    def send_skyalert_rows(y: str, m: str, d: str, send_name: str = filename) -> None:
        """Filter and copy SkyAlert rows for a given date to the sending directory."""
        compiledfile = local_dir + "weatherdatafiles.txt"
        with open(compiledfile, 'r') as h:
            lines = h.readlines()
        selected = [line for line in lines if "%04s-%02s-%02s" % (y, m, d) in line]
        if selected:
            temporal = local_dir + 'temporal.txt'
            if os.path.exists(temporal):
                os.remove(temporal)
            with open(temporal, 'w') as h:
                h.write(''.join(selected))
            shutil.copy2(temporal, send_dir + send_name)
            os.remove(temporal)

    if os.path.isfile(local_dir + name):
        shutil.copy2(local_dir + name, send_dir + filename)
        if prior == 1:
            filename = "Cloud_%s_%04s%02s%02s.txt" % (site, dates.tyear, dates.tmonth, dates.tday)
            name = "%04s-%02s-%02s.txt" % (dates.tyear, dates.tmonth, dates.tday)
            if is_skyalert:
                send_skyalert_rows(dates.tyear, dates.tmonth, dates.tday, send_name=filename)
            else:
                shutil.copy2(local_dir + name, send_dir + filename)
    elif is_skyalert:
        send_skyalert_rows(dates.year, dates.month, dates.day, send_name=filename)
        if prior == 1:
            filename = "Cloud_%s_%04s%02s%02s.txt" % (site, dates.tyear, dates.tmonth, dates.tday)
            name = "%04s-%02s-%02s.txt" % (dates.tyear, dates.tmonth, dates.tday)
            if is_skyalert:
                send_skyalert_rows(dates.tyear, dates.tmonth, dates.tday, send_name=filename)
    elif os.path.isfile(local_dir + filename):
        log.debug('here3')
        # Linux-version of the SkyAlert file
        shutil.copy2(local_dir + filename, send_dir + filename)
        if prior == 1:
            filename = "Cloud_%s_%04s%02s%02s.txt" % (site, dates.tyear, dates.tmonth, dates.tday)
            name = "%04s-%02s-%02s.txt" % (dates.tyear, dates.tmonth, dates.tday)
            shutil.copy2(local_dir + filename, send_dir + filename)
    else:
        log.warning('No Data Error!')
        now = dt.datetime.utcnow()
        with open(Path(send_dir) / checkname, 'w') as check:
            check.write(filename + '\n0\n0\n' + str(now) + '\n999\n')
            check.write(
                '1: 1st file name\n2: Number of parts\n3: Size of tar.gz\n'
                '4: Time of Creation\n5: GB Disk Free'
            )


def _zip_pic(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Zip, split, and stage PIC or NFI imager data.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'pic'`` or ``'nfi'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    filename = "%03s%02s_%s_%04s%02s%02s.tar.gz" % (instr, num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    year_dir = Path(local_dir) / dates.year
    os.chdir(year_dir)
    start = dt.datetime(int(dates.year), int(dates.month), int(dates.day), FPI_DAY_START_HOUR)
    end = start + dt.timedelta(1)
    name = [f for f in glob(os.path.join('*', '*.tif'))
            if start < dt.datetime.fromtimestamp(os.path.getmtime(f)) < end]
    zipper(name, filename)
    splitter(site, instr, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
    (year_dir / filename).unlink()


def _zip_sky(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Zip, split, and stage sky camera data.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'sky'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    filename = "%03s%02s_%s_%04s%02s%02s.tar.gz" % (instr, num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    os.chdir(local_dir)
    name = "%04s%02s%02s/" % (dates.year, dates.month, dates.day)
    zipper(name, filename)
    splitter(site, instr, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
    (Path(local_dir) / filename).unlink()


def _zip_scn(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Zip, split, and stage scintillation monitor data.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'scn'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    filename = "%03s%02s_%s_%04s%02s%02s.tar.gz" % (instr, num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    os.chdir(local_dir)
    name = "%02s%02s%02s*" % (dates.yr, dates.month, dates.day)
    zipper(name, filename)
    splitter(site, instr, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
    (Path(local_dir) / filename).unlink()


def _zip_tec(site: str, instr: str, num: str, code: dict,
             dates: DateBundle, prior: int) -> None:
    """Zip, split, and stage TEC/GPS data.

    Args:
        site: 3-letter site code.
        instr: Instrument code (``'tec'``).
        num: Instrument number/letter.
        code: Active-instruments dictionary.
        dates: Date bundle for the processing day.
        prior: Number of prior days being processed.
    """
    local_dir = code[site][instr][num]['local_dir']
    filename = "%03s%02s_%s_%04s%02s%02s.tar.gz" % (instr, num, site, dates.year, dates.month, dates.day)
    checkname = "%03s%02s_%s_%04s%02s%02s.txt" % (instr, num, site, dates.year, dates.month, dates.day)

    os.chdir(local_dir)
    name = glob("%02s%02s%02s*" % (dates.yr, dates.month, dates.day))
    zipper(name, filename)
    splitter(site, instr, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
    (Path(local_dir) / filename).unlink()


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------
HANDLERS: Dict[str, Callable] = {
    'fpi': _zip_fpi,
    'nfi': _zip_pic,   # nfi uses the same handler as pic
    'asi': _zip_asi,
    'pic': _zip_pic,
    'sky': _zip_sky,
    'scn': _zip_scn,
    'tec': _zip_tec,
    'bwc': _zip_bwc,
    'x3t': _zip_x3t,
}


# ---------------------------------------------------------------------------
# doer
# ---------------------------------------------------------------------------
def doer(site: str, instr: str, num: str,
         prior: int = 1, pyear: int = 0, pdoy: int = 0) -> None:
    """Zip, split, and move all data for one instrument/site to the sending folder.

    Args:
        site: 3-letter site code.
        instr: Instrument code.
        num: Instrument number/letter.
        prior: Number of prior days to process.
        pyear: Specific year to process (used with ``pdoy``).
        pdoy: Specific day-of-year to process (used with ``pyear``).
    """
    code = activeinstruments()
    handler = HANDLERS.get(instr)
    if handler is None:
        log.warning(
            'Unknown instrument %r. Usable keys: fpi,asi,nfi,pic,sky,scn,tec,bwc,x3t\n'
            'Please check your input: site -s  instrument -i  number -n', instr
        )
        return

    for dback in range(prior, 0, -1):
        today = dt.date.today()
        if pyear > 0:
            data_date = dt.date.fromordinal(dt.datetime(pyear, 1, 1).toordinal() - 1 + pdoy)
            next_date = dt.date.fromordinal(dt.datetime(pyear, 1, 1).toordinal() - 1 + pdoy + 1)
        else:
            data_date = (dt.datetime.today() - dt.timedelta(dback)).date()
            next_date = (dt.datetime.today() - dt.timedelta(dback - 1)).date()

        dates = DateBundle(date=data_date, next_date=next_date, today=today)
        log.info('%s: %s-%s-%s', instr, dates.day, dates.mon, dates.year)
        handler(site, instr, num, code, dates, prior)


# ---------------------------------------------------------------------------
# searcher
# ---------------------------------------------------------------------------
def searcher(site: str, instrument: str, num: str, local_dir: str) -> None:
    """Search all IMG files within ``local_dir``, sort, zip, and split for sending.

    Avoids repeated files using acquisition time; avoids repeated archive names by
    adding suffixes. Groups files into nightly bundles (15 LT to next day 15 LT).

    Args:
        site: 3-letter site code.
        instrument: Instrument code.
        num: Instrument number/letter.
        local_dir: Directory to recursively search for ``.img`` files.

    Raises:
        ImportError: If PIL/Pillow or ImgImagePlugin are not available.
    """
    if Image is None:
        raise ImportError(
            "PIL/Pillow is required for searcher(). Install with: pip install Pillow"
        )
    try:
        import ImgImagePlugin as _imp  # noqa: F401 — registers .img handler with PIL
    except ImportError as e:
        raise ImportError("ImgImagePlugin is required for searcher()") from e

    code = activeinstruments()

    def get_dt(path: str) -> Optional[dt.datetime]:
        """Return the LocalTime embedded in an .img file header, or None."""
        try:
            img = Image.open(path)
            return img.info['LocalTime']
        except Exception:
            return None

    # Collect all .img files and their acquisition datetimes
    imgpaths: List[str] = [
        os.path.join(root, name)
        for root, _, files in os.walk(local_dir)
        for name in files
        if '.img' in name
    ]
    dts = list(map(get_dt, imgpaths))
    data: List[tuple] = list(zip(dts, imgpaths))

    log.info('Total IMG found %d', len(data))
    bad = [(d, p) for d, p in data if d is None]
    good = [(d, p) for d, p in data if d is not None]
    log.info('\tIMG with non-readable headers: %d', len(bad))
    log.info('\tIMG OK: %d', len(good))

    good.sort()

    def grouper(row: tuple) -> str:
        """Group key: day boundary at 15 LT."""
        dtlocal, _ = row
        dt0 = dtlocal if dtlocal.hour > 15 else dtlocal - dt.timedelta(hours=12)
        return dt0.strftime('%j%Y')

    for doyyear, daily in groupby(good, grouper):
        lista: List[str] = []
        # Deduplicate by acquisition datetime; keep the largest file when multiple exist
        for _dt_key, items_iter in groupby(daily, lambda row: row[0]):
            items = list(items_iter)
            if len(items) > 1:
                sizes = [os.stat(item[1]).st_size for item in items]
                lista.append(items[sizes.index(max(sizes))][1])
            else:
                lista.append(items[0][1])

        _dt_obj = dt.datetime.strptime(doyyear, '%j%Y')
        log.info(
            'Zipping and Splitting %d non-repeated files from %s',
            len(lista), _dt_obj.strftime('%d %h %Y'),
        )

        local_dir_path = Path(code[site][instrument][num]['local_dir'])
        os.chdir(local_dir_path)
        filename = "%03s%02s_%s_%s.tar.gz" % (instrument, num, site, _dt_obj.strftime('%Y%m%d'))
        checkname = "%03s%02s_%s_%s.txt" % (instrument, num, site, _dt_obj.strftime('%Y%m%d'))

        # Build unique archive member names
        arcnames: List[str] = []
        suffix_counter = 0
        for item in lista:
            arcname = "/%s/%s" % (_dt_obj.strftime('%Y%m%d'), os.path.basename(item))
            if arcname in arcnames:
                arcname = arcname[:-4] + "__%05i.img" % suffix_counter
                suffix_counter += 1
            arcnames.append(arcname)

        with tarfile.open(filename, 'w:gz') as archive:
            for idx, item in enumerate(lista):
                with open(item, 'rb') as f:
                    info = archive.gettarinfo(name=item, arcname=arcnames[idx])
                    archive.addfile(info, f)

        splitter(site, instrument, num, code, filename, checkname, MIN_FILE_SIZE_BYTES)
        (local_dir_path / filename).unlink()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    parser = argparse.ArgumentParser(
        description='Zip, split, and stage science data for transfer to the repository.',
        usage='Zipper -s SITE -i INSTRUMENT -n NUMBER -p DAYSPRIOR -y YEAR -d DOY',
    )
    parser.add_argument('-s', '--site', dest='site', metavar='SITE',
                        type=str, default='XXX', help='Site to be run')
    parser.add_argument('-i', '--instrument', dest='instr', metavar='INSTRUMENT',
                        type=str, default='XXX', help='Instrument to be run')
    parser.add_argument('-n', '--number', dest='num', metavar='NUMBER',
                        type=str, default='00', help='Number/Letter of instrument')
    parser.add_argument('-p', '--prior', dest='prior', metavar='PRIORDAYS',
                        type=int, default=1, help='Days Prior to be run')
    parser.add_argument('-y', '--year', dest='pyear', metavar='YEAR',
                        type=int, default=0, help='Year to be run')
    parser.add_argument('-d', '--doy', dest='pdoy', metavar='DOY',
                        type=int, default=0, help='Doy to be run')
    parser.add_argument('-S', '--search', dest='psearch', metavar='SRCH',
                        type=str, default='None', help='Search all IMG files within local_dir')

    args = parser.parse_args()
    site = args.site.lower()
    instr = args.instr.lower()
    num = args.num.zfill(2)
    prior = args.prior
    pyear = args.pyear
    pdoy = args.pdoy
    path2search = args.psearch

    if prior > 1 and pyear > 0:
        d0 = dt.datetime(int(pyear), 1, 1) + dt.timedelta(days=pdoy - 1)
        for i in range(prior):
            ddt = d0 + dt.timedelta(days=i)
            doer(site, instr, num, 1, ddt.year, ddt.timetuple().tm_yday)
        sys.exit()

    if len(instr) > 3:
        log.warning('Usable Instrument Keys: fpi,asi,nfi,pic,sky,swe,cas,tec,scn,bwc,x3t')

    if 'None' in path2search:
        doer(site, instr, num, prior, pyear, pdoy)
    else:
        log.info('Searching within %s...', path2search)
        searcher(site, instr, num, path2search)

    log.info('Zip Complete...')
