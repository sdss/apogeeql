#!/usr/bin/env python

'''
EM: Program to check the stability of APOGEE instrument. 
It uses ArcLamp THAR data  and fits a gaussian function
to one spectral line x-center=43 and output fitting parameters
relative to reference data apRaw-09000004.fits fitting. 
 
EM 09/01/2013
Usage:  ./apogeeThar.py <-m1 "start mjd"> <-m2 enf mjd > 
    the output might be redirected to file   

History: 
09/11/2013: EM added to apogeeql svn bin repository

09/16/2013: EM reference changed to the first completed set of calibration after 
2013 summer shakedown,  
mjd=56531, Aug 26,2013, ff=09690003, 09690005, 09690014, 09690016

examples:
The first completed calibration night  after 
 ./apogeeThar.py -m1 56531
 
 check for range from 06/01/2013  (56445)  - 09/01/2013  (56537)
 ./apogeeThar.py -m1 56445  -m2 56537
 
 09/17/2013: EM tested second spectral line
# p0 = scipy.c_[7912, 940, 1.3]  # initial fitting params, Line 2, VM, dither A
p0 = scipy.c_[7300, 941.17, 1.36]  # initial fitting params, Line 2, mjd=56531, dither A
I got the same results with dither one pix jump for second line, as I got before
for the first line.  
 '''
import argparse
import pyfits, numpy, scipy
from pylab import *
import scipy.optimize
import sys, os.path, glob
import time

#  Constants
dthM= 12.994

pathData="/data/apogee/utr_cdr/"
mjdMaster="56531"
masterFile="09690003";
masterPath="%s%s/apRaw-%s.fits" % (pathData, mjdMaster,masterFile)

# define a gaussian fitting function where, 
# p0[0] = amplitude, p0[1] = center, p0[2] = fwhm

#p0 = scipy.c_[36750, 44.004, 2.29]  # initial fitting params, Line 1   for mjd=56531
# p0 = scipy.c_[7912, 940, 1.3]  # initial fitting params, Line 2, VM, dither A
p0 = scipy.c_[7300, 941.17, 1.36]  # initial fitting params, Line 2, mjd=56531, dither A

#---------

def getFullName(ff):
  ss="apRaw-%s.fits" % ff
  mask="%s*/%s" % (pathData, ss)
  files = glob.glob(mask)
  nfiles= len(files)
  return files[0]

def readFile(ff): 
  hdulist=pyfits.open(ff,'readonly')
  hdr = hdulist[0].header
  data=hdulist[0].data
  dth=float(hdr['DITHPIX'])
  hdulist.close()
  return hdr,data     

def getSpe(data, l1,l2): 
  sz1=data.shape 
  def ds9topy(p):  return [p[1],p[0]]
  p1=[0,l1];  p1conv=ds9topy(p1)
  p2=[sz1[1], l2];  p2conv=ds9topy(p2)
  dat1=data[p1conv[0]:p2conv[0],:]  # select lime of spectra
  spe=numpy.average(dat1, axis=0)    # average all spectra in that line
  return spe

def plotSpe(spe, fit, dx): 
  pix=scipy.linspace(0, spe.shape[0], num=spe.shape[0])
  plot(pix,spe)
  plot(pix,spe, "bo")
  plot(pix,fit, color="red")
  plot(pix,fit, "ro")
  grid(True)
  xlabel('pix')
  ylabel('intensity')
  ymax=spe.max()+spe.max()*0.1
  title('Spectrum arc APOGEE')
  #  xlim([0,8192])  # full range of pixels
  xlim([dx[0],dx[1]])
  ylim([0,ymax])
  plot([2047,2047],[0,ymax], color="green") 
  plot([4097,4097],[0,ymax], color="green") 
  plot([6145,6145],[0,ymax], color="green") 
    #savefig("test.png")
  show()  
  return 
  
def sdth(dth): 
    if dth==12.994:  return "A"
    elif dth==13.499:  return "B"
    else: return "?"    
 
def ifThar(hdr): 
  # check if this arc1?   
  imtype= hdr.get('IMAGETYP')
  q1=hdr.get('IMAGETYP') == "ArcLamp"
  q2=hdr.get('NFRAMES') ==12
  if (q1 and q2):  return True
  else:  return False

def checkOneMjd(mjd):
  ss="apRaw-%s.fits" % "*"  
  pathData="/data/apogee/utr_cdr/%5s" %  mjd  
  mask="%s/%s" % (pathData, ss)
  files = sorted(glob.glob(mask))
  if len(files)==0:
     return False
  for i,ff in enumerate(files): 
      hdr,data=readFile(ff)  #   read data file
      if not ifThar(hdr):    # check if file thar arc? 
         continue
      spe= getSpe(data,1200, 1500) # select swap ad average to one spectrum

      # fit gaussian function
      pix=scipy.linspace(0, spe.shape[0], num=spe.shape[0])
      fitfunc = lambda p0, x: p0[0]*scipy.exp(-(x-p0[1])**2/(2.0*p0[2]**2))
      errfunc = lambda p, x, y: fitfunc(p,x)-y
      p1, success = scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(pix,spe))

      # print the result of fitting  
      if success==1:
         dth=float(hdr['DITHPIX'])
    #    use this print for params
    #     print "fitting params = ",p1[0],p1[1], p1[2] 
         offset=p1[1] - p0[0][1]
         ff1=ff[33:41]  
         mm=ff[21:26]
         intrel=(p1[0]/p0[0][0])*100
         widthrel=(p1[2] - p0[0][2])
         print " %8s  %s  %5.2f  %4.2f %7i  %3i    %5.2f  %5.2f    %4.2f %5.2f   %5s" % \
             (ff1, sdth(dth), dth,  dth-dthM, p1[0], intrel,  p1[1], offset, p1[2], widthrel, mm)
      else: 
           print "Fitting was not successful, success code =",success 
  return

  
if __name__ == "__main__":

# mjd
  TAI_UTC =34; sjd1=(time.time() + TAI_UTC) / 86400.0 + 40587.3;  sjd= int (sjd1)

  desc = 'apogee arc Thar check for mjd range'
  parser = argparse.ArgumentParser(description=desc)
#  parser.add_argument('-f', '--datafile', help='enter filename')
  parser.add_argument('-m1', '--mjd1', help='start mjd range, default current mjd', \
        default=sjd,  type=int)
  parser.add_argument('-m2', '--mjd2',  help='end of mjd range, default is mjd1', \
         type=int)
  args = parser.parse_args()    
  mjd1=args.mjd1
  mjd2=args.mjd2
  
  #if mjd1==None:  mjd1=sjd
  #sys.exit("mjd1 is not set, exit")
  if mjd2==None:  mjd2=mjd1
  mjds=range(mjd1, mjd2+1)
  
# print master fitting parameners  
  print "# ./apogeeThar.py -m1 %s -m2 %s > apogeeThar.outfile" % (mjd1, mjd2)
  
  print "# Master exposure path = %s" %  (masterPath)
  kl=75;   print "#%s" % ("-"*kl)
  header= "#  file   A/B   D    D-Do     I   I/Io,%    X     X-Xo     W    W-Wo    mjd"
  print header 
  print "#%s" % ("-"*kl)
  
  print "# master   %s  %5.2f  0.00 %7i  100    %5.2f   0.00    %4.2f  0.00   %5s" % \
     (sdth(dthM), dthM,   p0[0][0],  p0[0][1], p0[0][2], 56531)
  print "#%s" % ("-"*kl)

  for m,mjd in  enumerate(mjds):     
      checkOneMjd(mjd)
      print "#%s" % ("-"*kl)
  