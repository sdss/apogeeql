#!/usr/bin/env python
"""
To fix the files in MJD 56532-56540 where the bzero field wasn't written
during the annotation, due to the pyfits 2.4->3.1 change.
"""
import os
import sys
import getpass
import socket
import glob
import pyfits

suffix = 'badheaders'

def fix_header(hdulist):
    """
    Replace the missing BZERO/BSCALE values in the header,
    in the correct place.
    """
    bscale = int(hdulist[0].header.get('BSCALE',1))
    bzero = int(hdulist[0].header.get('BZERO',32768))
    del hdulist[0].header['BSCALE']
    del hdulist[0].header['BZERO']
    hdulist[0].header.update('BSCALE',bscale,after='GCOUNT')
    hdulist[0].header.update('BZERO',bzero,after='BSCALE')
    return hdulist

def fix_headers(files,moved,force=False):
    """Fix the headers of moved, writing them to files."""
    for f,m in zip(files,moved):
        print "Fixing header: %s -> %s"%(m,f)
        if not force: m = f
        hdulist = pyfits.open(m,uint16=True,do_not_scale_image_data=True,checksum=True)
        hdulist = fix_header(hdulist)
        if force:
            hdulist.writeto(f,clobber=False,output_verify='warn',checksum=True)
            os.chmod(f,0o444)

def move_files(files,force=False):
    """Move files to FILEDIR/badheaders/."""
    newdir = os.path.join(os.path.split(files[0])[0],suffix)
    moved = []
    if not os.path.exists(newdir):
        os.mkdir(newdir)
    for f in files:
        newfile = os.path.join(newdir,os.path.split(f)[-1])
        print "Moving: %s -> %s"%(f,newfile)
        if force:
            os.rename(f,newfile)
            os.chmod(newfile,0o444)
        moved.append(newfile)
    return moved

def main(argv=None):
    from optparse import OptionParser
    if argv is None: argv = sys.argv[1:]

    usage = "%prog [OPTIONS] DIR1 [DIR2 [DIR3 [...]]]"
    usage += "\n\nRepair BZERO/BSCALE headers in the apRaw files in DIR, after"
    usage += "\nmoving them to a backup directory."
    usage += "\nJust print, don't do it, unless -f is specified."
    usage += "\n\nDIR example (on apogee-ql): /data-ql/data/56535"
    parser = OptionParser(usage)
    parser.add_option('-f','--force',dest='force',action='store_true',default=False,
                      help='Actually move and fix the files (%default).')
    (opts,args) = parser.parse_args(args=argv)
    
    if opts.force and getpass.getuser() != 'sdss3' and 'apogee-ql' not in socket.gethostname():
        print "If 'force', we must be run as sdss3@apogee-ql!"
        sys.exit(-2)
    
    if not opts.force:
        print "NOT DOING ANYTHING, JUST PRINTING."
    if len(args) == 0:
        print "Need at least one directory as an argument."
        print usage
        sys.exit(-1)
    for directory in args:
        print "Processing:",directory
        files = sorted(glob.glob(os.path.join(directory,'apRaw*.fits')))
        moved = move_files(files,opts.force)
        fix_headers(files,moved,opts.force)

if __name__ == "__main__":
    main()
