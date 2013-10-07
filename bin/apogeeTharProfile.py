#!/usr/bin/env python

'''apogeeTharTest.py  test #2 line fitting, plot optional (EM)

EM,  09/23/2013 -  program for the testing of fitting compare reference. 
The reference  are "/data/apogee/quickred/56531/ap1D-a-09690003.fits.fz"  (A)
                                               "ap1D-a-09690005.fits.fz"  (B)

Usage: 
./apogeeTharProfile.py   -    to run for reference data 

./apogeeTharProfile.py -f 09960005
      print fitting results 

./apogeeTharProfile.py -f 09960005 -p
    print fitting results and  them 


Use average A and B params as reference and initial values for fitting
p0_A - A profile 


 '''
import argparse
import pyfits, numpy, scipy
import scipy.optimize
import sys, os.path, glob
import time

import matplotlib.pyplot as plt
from scipy.interpolate import spline

p0a = scipy.c_[53864, 939.646, 1.2745]
p0b = scipy.c_[46184.2, 924.366, 1.071]
p0c = scipy.c_[31715, 1776.62, 0.803]


if __name__ == "__main__":
     desc = 'apogeeThar fitting for one image, line #2, plot optional'
     nexpDef='09690003'
     parser = argparse.ArgumentParser(description=desc)
     parser.add_argument('-n', '--nexp', help='exposure number, default=%s, on mjd=56531' % nexpDef,\
             default=nexpDef)
     parser.add_argument('-l', '--line', help='chip and line number (a,b,c), default a',\
             default='a') 
     parser.add_argument('-f', '--fiber', help='fiber, default 150', type=int, default=150) 
     parser.add_argument('-p', '--plot', help='select to plot',\
             default=False, action='store_true')
             
     args = parser.parse_args()    
     pp=args.plot
     if  args.line not in ['a','b','c']:
         sys.exit("not right line")
     fiber=args.fiber 
     if fiber > 300: 
         sys.exit("fiber should be 1:300, you entered %s" % fiber)

     mask="/data/apogee/quickred/*/ap1D-%s-%s.fits.fz" % (args.line, args.nexp)      
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

     pp="p0%s" % args.line
     p0=eval(pp); 
   #  print "Ref={int:%7.1f,  x:%7.3f,  wg:%5.3f}"  % (p0[0][0],p0[0][1],p0[0][2] )
 
     zone=15;  x1=int(p0[0][1])-zone;   x2=int(p0[0][1])+zone
     print "fiber=%s ( 1 -- 299) " % (fiber)
     spe=data1[fiber,x1:x2]  #read spectrum at y=150 and in line range 
     x=numpy.arange(data1.shape[1])[x1:x2]  #  x-axis array in pix
      
     intI=p0[0][0];  cx=p0[0][1];  wd=p0[0][2] 
     ll=numpy.where(spe == max(spe) )
     print ll[0][0], x1
     print ll[0][0]+x1


     p0[0][1]= ll[0][0]+x1    
             
     # fit gaussian function
     fitfunc = lambda p0, x: p0[0]*scipy.exp(-(x-p0[1])**2/(2.0*p0[2]**2))
     errfunc = lambda p, x, y: fitfunc(p,x)-y
     p1, success= scipy.optimize.leastsq(errfunc, p0.copy()[0],args=(x,spe))
     fitting = fitfunc(p1, x)

     print "success (0-4 ok) =",success
     print "Fit={int:%7.1f,  x:%7.3f,  wg:%5.3f}"  % (p1[0],p1[1], p1[2])
#     print "Ref={int:%7.1f,  x:%7.3f,  wg:%5.3f}"  % (p0[0][0],p0[0][1],p0[0][2] )
     print "Ref={int:%7.1f,  x:%7.3f,  wg:%5.3f}"  % (intI,cx,wd)     
     print "Dif={int:%7.1f,  x:%7.3f,  wg:%5.3f}" % \
              (p1[0]/intI, p1[1]-cx,  p1[2]-wd)
#     print "offset = =%5.2f" %  (p1[1]-p0[0][1])
     print ""
     
     if not args.plot:
         sys.exit()  # no plotting requested         
         
# smooth
     xnew = numpy.linspace(x[0],x[-1],100)
     spe_smooth = spline(x,spe,xnew)
     fitting_smooth = spline(x,fitting,xnew)

     refFit = fitfunc(p0[0], x)
     refFit_smooth = spline(x,refFit,xnew)

     fig = plt.figure(figsize=(8,5))
     plt.subplot(1, 1, 1)   # horiz, vertical
     plt.title('%s' %(file[0])) 
     plt.xlabel('pixels')
     rr=70000;  r1=-0.1*rr;  r2=1.1*rr
     plt.ylim((r1,r2))   
     plt.xlim((x1,x2))   
 #    plt.ylim((-max(spe_smooth)*0.1,max(spe_smooth)*1.1))   
     plt.ylabel('data')
     plt.plot(xnew, refFit_smooth, color='green', )     
     plt.plot(xnew, spe_smooth, color='black')
     plt.plot(x, spe, 'o', color='black', markersize=3.5)
     plt.plot(xnew, fitting_smooth, color='red', )
     plt.plot([p0[0][1],p0[0][1]], [r1,r2], color='black', )
     plt.xlim([x1,x2])
     plt.grid(True, which='both')
     plt.show()
