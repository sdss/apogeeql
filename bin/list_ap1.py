#!/usr/bin/env python

""" Print list of apogee files in <mjd> directory (or current). 

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

10/03  added _try_ statement while read fits file  if it is not available
10/04  I stopped errro messages, but I still got pyfits warnings, 
        I stopped them by  setting warnings.filterwarnings('ignore')
11/12  fixed bug with format message if function cannot read file   fits file     
12/10 fixed bug  fitting gaussian function if offset large  (copied function from apogeeThar);
    reformat - repalce ArcLams t Arc,  QuaFlat, and IntFlat; added dither set, format offset 
    to 4.1f (it was  5.2f). 

01/09/2014 Added column with  normed flux for all three flats.  
"""

import glob
import pyfits, numpy, scipy
import sys, os.path
import argparse
import datetime as dt
import time
import scipy.optimize
import re

import warnings
warnings.filterwarnings('ignore')

import apogeeThar

#p0 = scipy.c_[44941, 939.646, 1.287] #   A and B average for Line 2

zone=20
p0=apogeeThar.p0a.copy()

#------------------------
def curSjd():
# current mjd
  TAI_UTC =34; sjd1=(time.time() + TAI_UTC) / 86400.0 + 40587.3;  
  sjd= int (sjd1)
  return sjd
#----------------------- 
def getMorningSequence(mjd): 
    pp="/data/apogee/utr_cdr/"
    fNames="%s%s/apRaw-%s.fits"%(pp,mjd,"*")
    files = glob.glob(fNames)
    files=sorted(files)
    nfiles=len(files)
    map=''
    if nfiles==0: 
        print " - no files found -- " 
        return None
    else: 
        for i,f in enumerate(sorted(files)):
            hdr = pyfits.getheader(f)
            imtype=hdr.get('IMAGETYP')
            nreads=hdr.get('NFRAMES')
            if imtype=="Dark": map=map+"d"
            elif imtype=="QuartzFlat": map=map+"q"
            elif imtype=="InternalFlat": map=map+"i"
            elif imtype=="DomeFlat": map=map+"m"
            elif imtype=="ArcLamp" and nreads==12: map=map+"t" # Thar
            elif imtype=="ArcLamp" and nreads==40: map=map+"u" # Une
            elif imtype=="Object" : map=map+"o" 
            else:
               print "wrong image type" 
               return None 
        regex="dddqqqtutudiiid"
        m = re.search(regex, map)
        if m==None:
            print "  no morning sequence found, exit"
            return None
        else:
            print m.start(), m.end()
            return files[m.start():m.end()]


def getOffset(qrfile1):
    try :
        data1 = pyfits.getdata(qrfile1,0)
    except  :
        return None 
    success, p1, x, spe, ref, fit =apogeeThar.OneFileFitting(data1, 150, p0)
    if success==5:
        return " err"
    else:
        return "%5.2f" % (p1[1] - p0[0][1])

# ......
def getFlux(f):
    hdr = pyfits.getheader(f)
    nreads=hdr.get('NFRAMES')
    imtype=hdr.get('IMAGETYP')
    if imtype not in ["QuartzFlat","InternalFlat","DomeFlat"]:
        return " - "
    dat = pyfits.getdata(f,0)/nreads
    dat=numpy.array(dat)
    y=1024;  x= 1024
    dat=dat[(y-200):(y+200),(x-100):(x+100)] 
    med=numpy.mean(dat)
    if imtype=="QuartzFlat":  nrm=167.05
    elif imtype=="InternalFlat":  nrm=94.05
    elif imtype=="DomeFlat":  nrm=81.525
    else: nrm=None
    if nrm != None:  
        return "%3i" % (med/nrm*100)
    else:  return " ? "

                    
#...
def  list_one_file(i,f,mjd):
    path="/data/apogee/quickred/%s" % mjd
    fexp=f[33:41]
    qrfile1="%s/ap1D-a-%s.fits.fz" % (path,fexp)
    qrfile2="%s/ap2D-a-%s.fits.fz" % (path,fexp)

    ff=f
    if os.path.exists(qrfile2):  
        ff=qrfile2

    q=False
    for j in range(3):
      try :
        hdr = pyfits.getheader(ff)
        q=True
        break
      except :    # it was except IOError:
        continue 
    if not q:
       print "    cannot read file : %s" % ff 
       return
    
    ct=hdr.get('CARTID'); plate=hdr.get('PLATEID'); 
    if ct == None: ct="--"
    if plate==None: plate="----"

    dth= float(hdr['DITHPIX'])
    if dth==12.994: sdth="A(%4.1f)" % dth
    elif dth==13.499: sdth="B(%4.1f)" % dth
    else: sdth="?(%4.1f)"% dth    

    imtype= hdr.get('IMAGETYP')
    offset=" - "
    if imtype=="ArcLamp":
      imtype="Arc"
      if hdr.get('LAMPUNE')==1:  imtype=imtype+"-Une"
      elif hdr.get('LAMPTHAR')==1:
          imtype=imtype+"-Thar"
          offs=getOffset(qrfile1)
          if offs != None: 
              offset=offs
      else: imtype=imtype+"----"
    if imtype=="QuartzFlat":  imtype="QuarFlat"
    if imtype=="InternalFlat": imtype="InteFlat"    
    imtype=imtype.center(10)

    arc=list("x-x-x")    
    for k,l in enumerate(["a","b","c"]):
        pp="/data/apogee/archive/%s/apR-%s-%s.apz"%(mjd,l,f[33:41])
        if os.path.exists(pp): arc[2*k]=l 
    
    flux=" - "    
    if hdr.get('IMAGETYP') in ["QuartzFlat","InternalFlat","DomeFlat"]:
            flux=getFlux(f)
    
# print information
    ss1="%3i "% (i+1)  #i
    ss1=ss1+"%s  " % (hdr['DATE-OBS'][11:16]) # UT time
    ss1=ss1+"%s " % (f[33:41])  # exp number
    ss1=ss1+"%s " % (imtype)  # image type
    ss1=ss1+"%2i  " %  hdr.get('NFRAMES')  # nframes
    ss1=ss1+"%s " % (sdth)  # dither            
    ss1=ss1+" %2s-%4s   " % (ct, plate)
    ss1=ss1+"%s "%"".join(arc)    # archive file existence
    ss1=ss1+"%5s " % (offset)  # offset
    ss1=ss1+" %3s " % (flux)  # flux
    
    comm=hdr["OBSCMNT"]
#    if comm=="None": comm=" -*"
    ss1=ss1+ " %s" % comm[0:9] # comment
    print ss1 #  ,len(ss1)
#...

if __name__ == "__main__":

    sjd=curSjd()
    desc = 'list of files for one night of apogee observations'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-m', '--mjd', 
           help='enter mjd, default is current mjd',    
           default=int(sjd), type=int)

    parser.add_argument('-morning', '--morning', action="store_true") 
           
    args = parser.parse_args()    
    mjd=args.mjd
    
#    bs=os.path.basename(sys.argv[0])
#    bs=os.path.abspath(sys.argv[0])
#    print  "# %s -m1 %s" % (bs, mjd)
        
    print "APOGEE data list,   mjd=%s" % mjd    
    pp="/data/apogee/utr_cdr/"
    fNames="%s%s/apRaw-%s.fits"%(pp,mjd,"*")
    print "   raw_data: ", fNames
    ppqr="/data/apogee/quickred/%s/ap2D-a-*.fits.fz" % (mjd)
    print "   quick_red:", ppqr
    pparc="/data/apogee/archive/%s/apR-[a,b,c]-*.apz" % (mjd)
    print "   archive:  ", pparc
    
    files = glob.glob(fNames)
    files = sorted(files)
    nfiles=len(files)
    if nfiles==0: 
        print " - no files found -- " 
        sys.exit(0)

    if args.morning:
        files=getMorningSequence(mjd)        
        # add check result
        nfiles=len(files)
    
    line="-"*80
    print line
    prc="%"
    header=" i   UT   File/Exp   Imtype  Nread  Dth    Ct-Plate Archiv Offset %sFlux Comment" % prc
    print header  
    print line 
    nfiles=len(files)
    if nfiles==0: 
        print " - no files found -- " 
    else: 
        for i,f in enumerate(sorted(files)):
         #    if (i)/15.0 == int((i)/15) and i!=0:
         #         print line, "\n", header, "\n",line 
             list_one_file(i,f, mjd) 
                  
    print line    
    ss="    THAR offset=X0-%5.2f, pix " % (p0[0][1])
    
    print ss, "\n"
#...
