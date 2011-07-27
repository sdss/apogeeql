#!/usr/bin/env python

'''
This script will transform a plPlugMapM file to a plPlugMapA file for
APOGEE, adding the 2-Mass JHK magnitudes and 2-MAS target name to the
table.

Usage: Specify the plateId, fscan_mjd, fscan_id

    plugmapm2a.py -p 4385 -m 55752 -s 1


Script prerequisites:
---------------------
setup platedb
setup hooloovookit

'''
import os
import sys
import time
import types
import string
from optparse import OptionParser

try:
    from platedb.APODatabaseConnection import db # access to engine, metadata, Session
except ImportError:
    print 'Error on import - did you "setup platedb" and "setup hooloovookit" before running this script??\n'
    sys.exit(1)

try:
    print "Importing PlateDB"
    from platedb.ModelClasses import *
    print "Importing CatalogDB"
    from catalogdb.ModelClasses import *
except ImportError:
    print 'Could not find the platedb product - did you "setup platedb" before running this script??\n'
    try:
        db  
    except:
        db.engine.dispose() # avoid "unexpected EOF on client connection" on db server
    sys.exit(1)

import sqlalchemy
from sqlalchemy import not_, and_, or_ 
import yanny

import pyfits
import traceback


def makeApogeePlugMap(mysession, plugmap, newfilename):
   """Return the plPlugMapM given a plateId and pointingName"""

   # replace the \r with \n to get yanny to parse the text properly
   import re
   data = re.sub("\r", "\n", plugmap.file)

   # append to the standard plPlugMap to add 2mass_style and J, H, Ks mags
   par = yanny.yanny()
   par._contents = data
   par._parse()
   p0=par
   # update the definition of PLUGMAPOBJ
   pos=0
   for t in p0.tables():
      if t=='PLUGMAPOBJ':
         p0['symbols']['struct'][pos] = (p0['symbols']['struct'][pos]).replace('secTarget;','secTarget;\n char tmass_style[30];')
         break
      else:
         pos+=1
   
   p0['symbols']['PLUGMAPOBJ'].append('tmass_style')
   p0['PLUGMAPOBJ']['tmass_style']=[]
   for i in range(p0.size('PLUGMAPOBJ')):
      p0['PLUGMAPOBJ']['tmass_style'].append('-')

   # get the needed information from the plate_hole 
   ph = mysession.query(Fiber).join(PlateHole).join(CatalogObject).\
         filter(Fiber.pl_plugmap_m_pk==plugmap.pk).order_by(Fiber.fiber_id).\
         values('fiber_id','j','h','ks','tmass_style_id','apogee_target1','apogee_target2')
                     
   # we'll use the target1 and target2 to define the type of target
   # these are 32 bits each with each bit indicating a type
   skymask = 16
   hotmask = 512
   extmask = 1024
   starmask = skymask | hotmask

   # import pdb;  pdb.set_trace()

   # loop through the list and update the PLUGMAPOBJ
   for fid, j_mag, h_mag, k_mag, tmass_style, t1, t2 in ph:
      count = p0['PLUGMAPOBJ']['fiberId'].count(fid)
      if count >= 1:
          # we have more than one entry for this fiberId -> get the APOGEE 
          ind = -1
          for i in range(count):
              pos = p0['PLUGMAPOBJ']['fiberId'][ind+1:].index(fid)
              ind = pos+ind+1
              if p0['PLUGMAPOBJ']['spectrographId'][ind] == 2:
                  break

          # print "fid=%d    t1=%d   t2=%d" % (fid,t1,t2)
          # only modify the fibers for APOGEE (2)
          if p0['PLUGMAPOBJ']['spectrographId'][ind] == 2:
              p0['PLUGMAPOBJ']['mag'][ind][0] = j_mag
              p0['PLUGMAPOBJ']['mag'][ind][1] = h_mag
              p0['PLUGMAPOBJ']['mag'][ind][2] = k_mag
              p0['PLUGMAPOBJ']['tmass_style'][ind] = tmass_style
              if (t2 & skymask) > 0:
                 p0['PLUGMAPOBJ']['objType'][ind] = 'SKY'
              elif (t2 & hotmask) > 0:
                 p0['PLUGMAPOBJ']['objType'][ind] = 'HOT_STD'
              elif (t1 & extmask) > 0:
                 p0['PLUGMAPOBJ']['objType'][ind] = 'EXTOBJ'
              elif (t2 & starmask) == 0 and (t1 & extmask) ==0:
                 p0['PLUGMAPOBJ']['objType'][ind] = 'STAR'
              else:
                 print "fid=%d    t1=%d   t2=%d" % (fid,t1,t2)
            
   # delete file if it already exists
   if os.path.isfile(newfilename):
      os.remove(newfilename)

   p0.write(newfilename)

   return 


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-



# Process command line arguments
# ------------------------------
plateId = None
usage_text = '%s plateId ' % sys.argv[0]
description_text = "Append the APOG/EE specifc information to a plPlugMapM file and writes a new " + \
             "plPlugMapA file."

parser = OptionParser(usage=usage_text, description=description_text)

parser.add_option("-o", "--overwrite",
                  action="store_true", # for boolean options
                  dest="overwrite", # this will be the variable name
                  default=False,
                  help="overwrite any existing plPlugMapA file")

parser.add_option("-p", "--plateid",
                  dest="plateId",    # replaces "action", no default
                  help="plateId to process")

global options
(options, args) = parser.parse_args()

if (len(sys.argv) > 1):
    plateId = sys.argv[1]
else:
    # get the info from the command line arguments
    if options.plateId:
        plateId = int(options.plateId)

# make sure we have all the parameters to run
if (plateId == None):
    print
    print "Please specify the plateId to process."
    print
    print "Enter '%s --help' for more details." % sys.argv[0]
    print
    # db.engine.dispose() # avoid "unexpected EOF on client connection" on db server
    sys.exit()

mysession = db.Session()

# look for all the matching entries
pm = mysession.query(PlPlugMapM).join(Plugging).join(Plate).filter(Plate.plate_id==plateId).\
        order_by(PlPlugMapM.fscan_mjd.desc()).order_by(PlPlugMapM.fscan_id.desc())

if pm.count() == 0:
    print 'No entries for plate ',plateId
    sys.exit()
else:
    for i in range(pm.count()):
        print '%d   pm.filename=%s     pm.fscan_mjd=%d    pm.fscan_id=%d' % \
            (i+1,pm[i].filename,pm[i].fscan_mjd,pm[i].fscan_id)

id=''
id = raw_input("Select file to modify [1]: ")
if id == '':
    id=0
else:
    id = int(id)-1

if id+1 > pm.count():
    print 'wrong pm selected'
    sys.exit()

pm=pm[id]

fname = pm.filename
p = fname.find('MapM')
fname  = os.path.join('/data-ql/plugmaps/',fname[0:p+3]+'A'+fname[p+4:])
makeApogeePlugMap(mysession, pm, fname)

print 'wrote ',fname

