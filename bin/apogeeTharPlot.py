#!/usr/bin/env python

'''apogeeThar.py  plot outfile from  apogeeThar.py (EM)

This program is supplemented for apogeeThar.py. 
It uses apogeeThar.py output saved as  apogeeThar.outfile, 
read this file  as a table and plot 
x-centers of gaussian function from this table. 

EM 09/06/2013  

History: 
09/11/2013: EM: added to apogeeql svn repository
09/17/2013: forced matplotlib plot  mjd labels without offset
   
'''

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter, FormatStrFormatter
from pylab import * 
import argparse
import os.path, sys

if __name__ == "__main__":

    desc = 'apogeeThar.py result plotting'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-o', '--outfile', help='outfile to plot')
    args = parser.parse_args()    
    outfile=args.outfile
    if outfile == None: 
        sys.exit("no outfile entered")

    if  not os.path.isfile(outfile):
        sys.exit("no file found")
  
    # read  file  and split by lines
    with open(outfile) as f:
        data = f.read()
    data = data.split('\n')
    title=data[0][2:]

    print data[6]

    masterLine=data[6].strip().split(' ')
    centerLine=masterLine[18]
  
    # drop comments  
    data1=[]
    for line in data: 
       ll=line.lstrip()
       if len(ll)>0 and ll[0] !="#":  data1.append(line)
    data=data1

    # read line offset for A only   
    dataA=[]
    for line in data: 
       ll=line.lstrip()
       if ll.split()[1] =="A":
            dataA.append(line)
    xA = [int(row.split()[10]) for row in dataA]
    dA= [float(row.split()[7]) for row in dataA]
    mjdA=np.array(xA)
    dithA=np.array(dA)

    # read line offset for B only
    dataB=[]
    for line in data: 
       ll=line.lstrip()
       if ll.split()[1] =="B":
            dataB.append(line)
    xB = [int(row.split()[10]) for row in dataB]
    dB= [float(row.split()[7]) for row in dataB]
    mjdB=np.array(xB)
    dithB=np.array(dB)

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    plt.ylim((-2,2))
    #ax1.set_title("Line center - master center  pix")    
    ax1.set_title(title)    
    ax1.set_xlabel('mjd')
    ax1.set_ylabel("Line center - %s pix" % centerLine)
    #Aplot=ax1.plot(xA,dithA, 'o', color='red', markersize=3, label='A 12.994')
    Aplot=ax1.plot(mjdA,dithA, 'o', color='red', markersize=3.5, label='A')
    Bplot=ax1.plot(mjdB,dithB, 'o', color='blue', markersize=3.5, label='B')
    legend = ax1.legend(loc='lower right')
    ax1.grid(True, which='both')
    plt.ticklabel_format(useOffset=False)

    plt.show()
