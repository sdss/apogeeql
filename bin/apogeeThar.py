#!/usr/bin/env python

'''apogeeThar.py  check apogee Thar arcs (EM)

Program to check the stability of APOGEE instrument:
 - take ArcLamp THAR quick-red data 
 - takes 1-3 spectra  which is optional
 - do fitting of gaussian function to one spectral line on each chip.
 - compare results with reference, average for mjd=56531, Aug 26,2013, 
             ff=09690003, 09690005, 09690014, 09690016.
  
EM 09/01/2013
Usage:  ./apogeeThar.py <-m1 mjd1> <-m2 mjd2> 

examples:
 ./apogeeThar.py -m1 56531    # reference
 
 ./apogeeThar.py -m1 56560 -m2  56567

 ./apogeeThar.py -m1 56445  -m2 56537    # 06/01/2013 (56445) -- 09/01/2013 (56537)

History: 
09/11/2013: EM added to apogeeql svn bin repository
09/16/2013: EM reference is the first calibration set after 2013 summer
   mjd=56531, Aug 26,2013, ff=09690003, 09690005, 09690014, 09690016     
09/30/2013: EM:   use quick-red instead of instead of raw data; 
   reorganized output for differences only, 3-lines and 3 rows output, 
   design for night log. 
   
 '''
import argparse
import pyfits, numpy, scipy
from pylab import *
import scipy.optimize
import sys, os.path, glob
import time

#  Constants
dthM= 12.994
p0a = scipy.c_[53864, 939.646, 1.2745]
p0b = scipy.c_[46184.2, 924.366, 1.071]
p0c = scipy.c_[31715, 1776.62, 0.803]


pathData="/data/apogee/quick_red/"
mjdMaster="56531"
masterFile="09690003";
masterPath="%s%s/ap1D-a-%s.fits.fz" % (pathData, mjdMaster,masterFile)

#---------
  
def sdth(dth): 
    if dth==12.994:  return "A"
    elif dth==13.499:  return "B"
    else: return "?"    
 
  
def checkOneMjd(mjd):
  mask="/data/apogee/quickred/%s/ap1D-a-*.fits.fz" % (mjd)      
  files = sorted(glob.glob(mask))
  if len(files)==0:
     return False
  for i,ff in enumerate(files):   #   check One File

#  read fits data
     hdulist=pyfits.open(ff,'readonly')
     hdr = hdulist[0].header
 #    data1=hdulist[1].data
     hdulist.close()

#  check is file  ArcLamp and Thar? 
     q1=hdr.get('IMAGETYP')=="ArcLamp"
     q2=hdr.get('LAMPTHAR')==1
     if not(q1 and q2):
          continue
          
     dth=float(hdr['DITHPIX'])
     rows= [150]
    #    rows=[270, 150, 30]
     ss="%5s %8s %s" %  (mjd,ff[35:43],sdth(dth) )   
     for row in rows:      
         ss="%s %3i" %  (ss,  row)  # row
         lines=["a","b","c"]
         for line in lines: 
            s1="ap1D-a"; s2="ap1D-%s" % line;   
            ff1=ff.replace(s1, s2)  
    
            hdulist=pyfits.open(ff1,'readonly')
            data1=hdulist[1].data
            hdulist.close()

            pp="p0%s" % line
            p0=eval(pp)    

            x1=p0[0][1]-10;   x2=p0[0][1]+10
            x=numpy.arange(data1.shape[1])[x1:x2]  #  x-axis array in pix
            spe=data1[row,x1:x2]  #read spectrum at y=150 and in line range 
 
            fitfunc = lambda p0, x: p0[0]*scipy.exp(-(x-p0[1])**2/(2.0*p0[2]**2))
            errfunc = lambda p, x, y: fitfunc(p,x)-y
            p1, success= scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(x,spe))
        
            ss="%s   %5.2f" % (ss, (p1[0]/p0[0][0]))  # intensity
            ss="%s %5.2f" % (ss,  p1[1] - p0[0][1])  # profile center
            ss="%s %5.2f" % (ss, p1[2] - p0[0][2])  # width
         print "%s " % ss
    #     print "fitting params = ",p1[0],p1[1], p1[2] 

  return

  
if __name__ == "__main__":

# current mjd
  TAI_UTC =34; sjd1=(time.time() + TAI_UTC) / 86400.0 + 40587.3;  sjd= int (sjd1)

  desc = 'apogee arc Thar check for mjd range'
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('-m1', '--mjd1', help='start mjd range, default current mjd', \
        default=sjd,  type=int)
  parser.add_argument('-m2', '--mjd2',  help='end of mjd range, default is mjd1', \
         type=int)
  parser.add_argument('-r', '--ref',  action="store_true",  help='end of mjd range, default is mjd1')
                 
#  parser.add_argument('-v', '--ver',  help='full ver of tests, rows=50,150,250; \
#      default row=150', 
#     \   - full  True ? 
  args = parser.parse_args()    
  
  mjd1=args.mjd1
  mjd2=args.mjd2
  if mjd2==None:  mjd2=mjd1
  mjds=range(mjd1, mjd2+1)

  ver=False
  if ver: sver="full"
  else:  sver="short"   
  print "# ./apogeeThar.py -m1 %s -m2 %s" % (mjd1, mjd2) 
  outfile="apThar_%5i-%5i_%s.outfile" % (mjd1, mjd2, sver)   
#  print outfile
     
  kl=79;   print "#%s" % ("-"*kl)
  header= "# mjd  file  A/B row"+" "*11+"a"+" "*19+"b"+" "*19+"c"
  print header 
  spHed="I/Io  X-Xo  W-Wo"
  print "#%s" % (" "*23+spHed+" "*4+spHed+" "*4+spHed)
  print "#%s" % ("-"*kl)
 
  for m,mjd in  enumerate(mjds):     
      checkOneMjd(mjd)
      print "#%s" % ("-"*kl)
  
  if  args.ref:
     for l in ['a', 'b', 'c']:
        pp="p0%s" % l;   p0=eval(pp)     
        print "# ref-%s:  [%6i,  %7.2f,  %4.2f]"% (l, p0[0][0], p0[0][1],p0[0][2]) 
  print "" 
