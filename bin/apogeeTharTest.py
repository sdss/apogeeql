#!/usr/bin/env python

'''apogeeTharTest.py  test #2 line fitting, plot optional (EM)

apogeeTharTestFitting.py
EM,  09/23/2013
this file is for testing of fitting. 

./apogeeTharTestFitting.py -f 09960005
    for fitting output
./apogeeTharTestFitting.py -f 09960005 -p
    to plot  spectrum and fitting
     
Fitting for line#2 with initial settings:
int=44941,   xc=939.402,  and wg=1.287 (gaussian width ?)
     p0 = scipy.c_[44941, 939.402, 1.287] #   Line 2 , reference
reference    "/data/apogee/quickred/56531/ap1D-a-09690003.fits.fz" 
'''
import argparse
import pyfits, numpy, scipy
import scipy.optimize
import sys, os.path, glob
import time

import matplotlib.pyplot as plt
from scipy.interpolate import spline

if __name__ == "__main__":
     desc = 'apogeeThar fitting for one image, line #2, plot optional'
     parser = argparse.ArgumentParser(description=desc)
     parser.add_argument('-f', '--fexp', help='exposure number, default=09930003',\
             default='09930003')
     parser.add_argument('-p', '--plot', help='select to plot',\
             default=False, action='store_true')
             
     args = parser.parse_args()    
     pp=args.plot
     mask="/data/apogee/quickred/*/ap1D-a-%s.fits.fz" % (args.fexp)
     file = glob.glob(mask)
     if len(file)==0: 
          sys.exit("Erros: no file found %s" % file)
     print "file=", file[0]

     # read fits file   
     hdulist=pyfits.open(file[0],'readonly')
     hdr = hdulist[0].header
     data1=hdulist[1].data
     hdulist.close()

     # check is file  SrcLamp and Thar? 
     q1=hdr.get('IMAGETYP')=="ArcLamp"
     q2=hdr.get('LAMPTHAR')==1
     if not(q1 and q2):
          sys.exit("Error: the file is not Thar arc")

     x1=930; x2=950  # define pix range for line #2
     spe=data1[150,x1:x2]  #read spectrum at y=150 and in line range 
     x=numpy.arange(data1.shape[1])[x1:x2]  #  x-axis array in pix

     # fit gaussian function
     p0 = scipy.c_[44941, 939.402, 1.287] #   Line 2 , reference
     fitfunc = lambda p0, x: p0[0]*scipy.exp(-(x-p0[1])**2/(2.0*p0[2]**2))
     errfunc = lambda p, x, y: fitfunc(p,x)-y
     p1, success= scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(x,spe))
     fitting = fitfunc(p1, x)

     print "Ref: int=%7.1f,  x=%7.3f,  wg=%5.3f"  % (p0[0][0],p0[0][1],p0[0][2] )
     print "Fit: int=%7.1f,  x=%7.3f,  wg=%5.3f"  % (p1[0],p1[1], p1[2])
     print "success of fitting(0-4 ok) =",success
     print "offset = =%5.2f" %  (p1[1]-p0[0][1])
     
     if not args.plot:
         sys.exit()  # no plotting requested 
# smooth
     xnew = numpy.linspace(x[0],x[-1],50)
     spe_smooth = spline(x,spe,xnew)
     fitting_smooth = spline(x,fitting,xnew)

     fig = plt.figure(figsize=(8,5))
     plt.subplot(1, 1, 1)   # horiz, vertical
     plt.title('%s' %(file[0])) 
     plt.xlabel('pixels')
     plt.ylabel('data')
     plt.plot(xnew, spe_smooth, marker='o', color='grey', markersize=3.5)
     plt.plot(xnew, fitting_smooth, color='red', )
     plt.xlim([x1,x2])
     plt.show()

