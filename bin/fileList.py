#!/usr/bin/env python

'''fileList.py  print the list of files with the 1st line of docstring (EM)

todo:  use walk to make the list of files with variable directory tree 
'''

import os,sys
import glob

myName=os.path.basename(__file__)
files=sorted(glob.glob("*.py"))

for i, ff in enumerate(files): 
    fileName=os.path.basename(ff)        
    mm=os.path.splitext(fileName)[0]
    if ff == myName: 
       inf=__doc__
       ll=inf.split('\n')[0]
    else:         
       inf=__import__(mm).__doc__      
       ll=inf.split('\n')[0]
    print " - %s: %s" % (mm.ljust(15),ll)
print ""

