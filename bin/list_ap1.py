#!/usr/bin/env python

""" Search a list of apogee files in mjd directory

list_ap1.py  (for current mjd)
list_ap1.py -m  <mjd>  (for other mjd)

Testing:
list_ap1.py -m  56494  # before  summer shakedown
list_ap1.py -m  56531  # reference  
list_ap1.py -m  56553  # after summer shakedown 

History 2013: 
06/22  created by EM
06/25  fixed mjd calculation
06/26  added type of arc lamps 
09/09  added relative position of spectral line #1
        average middle section of raw data to get spe, set  ref position 
09/18  changed spectral relative position to line #2  [941.1758, 941.6995] 
09/24  contacted with JH, he recommended to use quick-reduction data 
      /data/apogee/quickred/56531/ap1D-a-09690003.fits.fz, select one fiber
      (I took 150),  and I use average A and B central position as reference, 939.646. 
      Ref: int=44941.0,  x=939.402,  wg=1.287
      Fit-A: int=44942.1,  x=939.402,  wg=1.287
      Fit-B: int=62787.1,  x=939.890,  wg=1.262
      file-A= /data/apogee/quickred/56531/ap1D-a-09690003.fits.fz 
      file-B= /data/apogee/quickred/56531/ap1D-a-09690005.fits.fz
         
"""

import glob
import pyfits, numpy, scipy
import sys, os.path
import argparse
import datetime as dt
import time
import scipy.optimize

p0 = scipy.c_[44941, 939.646, 1.287] #   A and B average for Line 2

#...
def getOffset(qrfile1):
    hdulist=pyfits.open(qrfile1,'readonly')
    hdr = hdulist[0].header
    data1=hdulist[1].data
    hdulist.close()

    x1=930; x2=950  # define pix range for line #2
    spe=data1[150,x1:x2]  #read spectrum at y=150 and in line range 
    x=numpy.arange(data1.shape[1])[x1:x2]  #  x-axis array in pix

    # fit gaussian function
    fitfunc = lambda p0, x: p0[0]*scipy.exp(-(x-p0[1])**2/(2.0*p0[2]**2))
    errfunc = lambda p, x, y: fitfunc(p,x)-y
    p1, success= scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(x,spe))
#    print "Ref: int=%7.1f,  x=%7.3f,  wg=%5.3f"  % (p0[0][0],p0[0][1],p0[0][2] )
#    print "Fit: int=%7.1f,  x=%7.3f,  wg=%5.3f"  % (p1[0],p1[1], p1[2])
#    print "success of fitting(0-4 ok) =",success
#    print "offset = =%5.2f" %  (p1[1]-p0[0][1])    
    return "%5.2f" % (p1[1] - p0[0][1])
    
#...
def  list_one_file(i,f,mjd):
    fexp=f[33:41]
    qrfile1="/data/apogee/quickred/%s/ap1D-a-%s.fits.fz" % (mjd,fexp)
    qrfile2="/data/apogee/quickred/%s/ap2D-a-%s.fits.fz" % (mjd,fexp)
    if os.path.exists(qrfile2): ff=qrfile2
    else: ff=f
    hdulist=pyfits.open(ff,'readonly')
    hdr = hdulist[0].header
    hdulist.close()

    ct=hdr.get('CARTID'); plate=hdr.get('PLATEID'); 
    if ct == None: ct="--"
    if plate==None: plate="----"

    dth= float(hdr['DITHPIX'])
    if dth==12.994: sdth="A"
    elif dth==13.499: sdth="B"
    else: sdth="?"    

    imtype= hdr.get('IMAGETYP')
    if imtype=="ArcLamp":
      if hdr.get('LAMPUNE')==1:  imtype=imtype+" (une)"
      if hdr.get('LAMPTHAR')==1:  imtype=imtype+" (thar)"
    imtype=imtype.center(14)
    
    offset="-"
    if imtype=="ArcLamp (thar)":
         offset=getOffset(qrfile1)
        
    ss1="%3i "% (i+1)  #i
    ss1=ss1+"%s  " % (hdr['DATE-OBS'][11:16]) # UT time
    ss1=ss1+"%s  " % (f[33:41])  # exp number
    ss1=ss1+"%s " % (imtype)  # image type
    ss1=ss1+"%2i  " %  hdr.get('NFRAMES')  # nframes
    ss1=ss1+"%s  " % (sdth)  # dither            
    ss1=ss1+" %2s-%4s   " % (ct, plate)

    arcA="/data/apogee/archive/%s/apR-%s-%s.apz"%(mjd,"a",f[33:41])
    arcB="/data/apogee/archive/%s/apR-%s-%s.apz"%(mjd,"b",f[33:41])
    arcC="/data/apogee/archive/%s/apR-%s-%s.apz"%(mjd,"c",f[33:41])    
    arc=list("x-x-x")
    if os.path.exists(arcA): arc[0]="a" 
    if os.path.exists(arcB): arc[2]="b" 
    if os.path.exists(arcC): arc[4]="c" 
    ss1=ss1+"%s  "%"".join(arc)    # archive file existence

    ss1=ss1+"%5s   " % (offset)  # offset 

    ss1=ss1+ "%s" % hdr["OBSCMNT"][0:20]  # comment       
     
    print ss1
#...

if __name__ == "__main__":
    TAI_UTC =34
    sjd1=(time.time() + TAI_UTC) / 86400.0 + 40587.3
    sjd= int (sjd1)
        
    desc = 'list of files for one night of apogee observations'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-m', '--mjd', 
           help='enter mjd, default is current mjd',    
     #      help='enter mjd, default is current mjd=%s' % int(sjd),
           default=int(sjd), type=int)
    args = parser.parse_args()    
    mjd=args.mjd
    
    pp="/data/apogee/utr_cdr/"
    fNames="%s%s/apRaw-%s.fits"%(pp,mjd,"*")
    print "   raw_data: ", fNames
    ppqr="/data/apogee/quickred/%s/ap2D-a-*.fits.fz" % (mjd)
    print "   quick_red:", ppqr
    pparc="/data/apogee/archive/%s/apR-[a,b,c]-*.apz" % (mjd)
    print "   archive:  ", pparc
    
    files = glob.glob(fNames)
    
    line="-"*80
    print line
    header=" i   UT   File/Exp   Imtype     Nread Dth  Ct-Plate Archiv  Offset Comment"
    print header  
    print line 
    nfiles=len(files)
    if nfiles==0: 
        print " - no files found -- " 
    else: 
        for i,f in enumerate(sorted(files)):
             list_one_file(i,f, mjd) 
    print line    
    ss="    THAR Line Centers reference (the average A and B, mjd=56531) = %6.2f pix" % (p0[0][1])
    print ss, "\n"
#...
