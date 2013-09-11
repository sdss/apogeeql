#!/usr/bin/env python

'''
This program is supplemented for apogeeThar.py. 
It uses apogeeThar.py output saved as  apogeeThar.outfile, 
read this file  as a table and plot 
x-centers of gaussian function from this table. 

EM 09/06/2013  

History: 
09/11/2013: EM: added to apogeeql svn repository
'''

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter, FormatStrFormatter
from pylab import * 

# read data text file  and split by lines
file="apogeeThar.outfile"
with open(file) as f:
    data = f.read()
data = data.split('\n')

# remove # comments  
data1=[]
for line in data: 
   ll=line.lstrip()
   if len(ll)>0 and ll[0] !="#":  data1.append(line)
data=data1

# dither A  
dataA=[]
for line in data: 
   ll=line.lstrip()
   if ll.split()[1] =="A":
        dataA.append(line)
xA = [int(row.split()[10]) for row in dataA]
dA= [float(row.split()[7]) for row in dataA]
mjdA=np.array(xA)
dithA=np.array(dA)

# dither B
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
plt.ylim((-1,2))
ax1.set_title("Line center - 43 pix")    
ax1.set_xlabel('mjd')
ax1.set_ylabel("Line center - 43 pix")
Aplot=ax1.plot(mjdA,dithA, 'o', color='red', markersize=3, label='A 12.994')
Bplot=ax1.plot(mjdB,dithB, 'o', color='blue', markersize=3, label='B 13.499')
legend = ax1.legend(loc='lower right')

ax1.grid(True, which='both')
plt.show()
