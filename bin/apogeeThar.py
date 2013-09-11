#!/usr/bin/env python

'''
EM: Program to check the stability of APOGEE instrument. 
It uses ArcLamp THAR data  and fits a gaussian function
to one spectral line x-center=43 and output fitting parameters
relative to reference data apRaw-09000004.fits fitting. 
 
EM 09/01/2013
Usage:  ./apogeeThar.py <-m1 "start mjd"> <-m2 enf mjd > 
the output might be redirected to file   

Example:

# ./apogeeThar.py -m1 56264 -m2 56541 > apogeeThar.outfile
# Master exposure path = /data/apogee/utr_cdr/56462/apRaw-09000004.fits
# Spectral line fitting:
#---------------------------------------------------------------------------
#  file   A/B   D    D-Do    I    I/Io,%    X    X-Xo     W    W-Wo   mjd
#---------------------------------------------------------------------------
# master   A  12.99         37083  100    42.99          2.34         56462
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
 08380009  A  12.99  0.00   36992   99    43.56   0.57    2.30 -0.04   56400
 08380011  B  13.50  0.51   37198  100    44.06   1.07    2.29 -0.05   56400
 08380029  A  12.99  0.00   37619  101    43.55   0.56    2.24 -0.09   56400
 08380031  B  13.50  0.51   37615  101    44.05   1.06    2.24 -0.10   56400
#---------------------------------------------------------------------------
 08390004  A  12.99  0.00   37523  101    43.55   0.57    2.29 -0.05   56401
 08390006  B  13.50  0.51   37576  101    44.06   1.07    2.29 -0.05   56401
 08390058  A  12.99  0.00   37785  101    43.57   0.59    2.27 -0.07   56401
 08390060  B  13.50  0.51   37892  102    44.08   1.09    2.27 -0.07   56401
#---------------------------------------------------------------------------

History: 
09/11/2013: EM added to apogeeql svn bin repository

'''

import argparse
import pyfits, numpy, scipy
from pylab import *
import scipy.optimize
import sys, os.path, glob

pathData="/data/apogee/utr_cdr/"
masterFile="09000004"
dthM= 12.994

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
#  print header 
  for i,ff in enumerate(files): 
      hdr,data=readFile(ff)  #   read data file
      if not ifThar(hdr):    # check if file thar arc? 
         continue
      spe= getSpe(data,1200, 1500) # select swap ad average to one spectrum

      # fit gaussian function
      pix=scipy.linspace(0, spe.shape[0], num=spe.shape[0])
      # define a gaussian fitting function where, 
      # p0[0] = amplitude, p0[1] = center, p0[2] = fwhm
      fitfunc = lambda p0, x: p0[0]*scipy.exp(-(x-p0[1])**2/(2.0*p0[2]**2))
      errfunc = lambda p, x, y: fitfunc(p,x)-y
      #  p0 = scipy.c_[37083, 42.986, 2.339]  # start fitting params
      p1, success = scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(pix,spe))

      # print the result of fitting  
      if success==1:
         dth=float(hdr['DITHPIX'])
         print p1[1] 
         offset=p1[1] - p0[0][1]
         ff1=ff[33:41]  # /data/apogee/utr_cdr/56462/apRaw-09000024
         mm=ff[21:26]
         intrel=(p1[0]/p0[0][0])*100
         widthrel=(p1[2] - p0[0][2])
         print " %8s  %s  %5.2f  %4.2f %7i  %3i    %5.2f  %5.2f    %4.2f %5.2f   %5s" % \
             (ff1, sdth(dth), dth,  dth-dthM, p1[0], intrel,  p1[1], \
             offset, p1[2], widthrel, mm)
      else: 
           print "Fitting was not successful, success code =",success 
  return

  
if __name__ == "__main__":

  desc = 'apogee arc Thar check'
  parser = argparse.ArgumentParser(description=desc)
#  parser.add_argument('-f', '--datafile', help='enter filename')
  parser.add_argument('-m1', '--mjd1', help='enter mjd1 - start, or just one night', type=int)
  parser.add_argument('-m2', '--mjd2',  help='enter mjd2 - end', type=int)
  args = parser.parse_args()    
  mjd1=args.mjd1
  mjd2=args.mjd2
  if mjd2==None:  mjd2=mjd1
  mjds=range(mjd1, mjd2+1)

  
# print master fitting parameners  
  print "# ./apogeeThar.py -m1 %s -m2 %s > apogeeThar.txt" % (mjd1, mjd2)
  print "# Master exposure path = %s" %  "/data/apogee/utr_cdr/56462/apRaw-09000004.fits" 
  print "# Spectral line fitting:"  
  kl=75 
  print "#%s" % ("-"*kl)
  prc="%s" % ("%")
  header= "#  file   A/B   D    D-Do    I    I/Io,%    X    X-Xo     W    W-Wo   mjd"
  print header 
  print "#%s" % ("-"*kl)
  dthM= 12.994
  
  p0 = scipy.c_[37083, 42.986, 2.339]  # start fitting params
  print "# master   %s  %5.2f       %7i  100    %5.2f          %4.2f         %5s" % \
     (sdth(dthM), dthM,   p0[0][0],  p0[0][1], p0[0][2], 56462)
  print "#%s" % ("-"*kl)

  for m,mjd in  enumerate(mjds):     
      checkOneMjd(mjd)
      print "#%s" % ("-"*kl)
  

