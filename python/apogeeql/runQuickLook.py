#!/usr/bin/env python
"""
This script will run the apogee quicklook process manually
If an output file is specified, all quicklook data, with the exception of large arrays,
will be written to that file in the form of CSV tables.

Usage: Specify the MJD, and optionally plate id or exposure number.  Use the -d (dbinsert) flag to specify a 
DB insert to QuicklookDB.
NOTE - any new Exposures and Observations will be commitetd to platedb, irrespective of the dbinsert flag.

	runQuicklook mjd -p plateId -e exposureId -o output_file -d

Script prerequisites:
----------------------
setup sdss_python_module
setup apgquicklook
"""


from optparse import OptionParser
import sys, os, tempfile, glob, shutil, subprocess
import logging
import pyfits
import traceback
import sqlalchemy

try:
    from sdss.internal.database.connections.APODatabaseUserLocalConnection import db # access to engine, metadata, Session
#	from sdss.internal.database.connections.APODatabaseDevAdminLocalConnection import db as db_dev #dev db for testing
except ImportError:
    print 'Error on import - did you "setup sdss_python_module" before running this script??\n'
    sys.exit(1)

try:
    from sdss.internal.database.apo.platedb.ModelClasses import *
    from sdss.apogee.addExposure import *
    from sdss.apogee.makeApogeePlugMap import *
except ImportError:
    print 'Could not create ModelClasses - did you "setup sdss_python_module" before running this script??\n'
    try:
        db
    except:
        db.engine.dispose() # avoid "unexpected EOF on client connection" on db server
    sys.exit(1)
'''
try:
	import plugmapm2a
except ImportError:
    print 'Error on import - did you "setup apogeeql" before running this script??\n'
    sys.exit(1)
'''
#initial setup
mjd = None
plate_id = None
exposure_id = None
output_file = None
prevPlateId = None
prevCartId = None
prevPointing = None
prevScanId = None
prevScanMJD = None
surveyLabel='APOGEE-2'
startOfSurvey = 55562

# Handle inputs
usage_text = '%s <mjd to process>' % sys.argv[0]
description_text = "This script will run the apogee quicklook process for the given MJD/plate/exposure."

parser = OptionParser(usage=usage_text, description=description_text)
    
parser.add_option("-p", "--plateid",
                      dest="plate_id",    # replaces "action", no default
                      help="plateId to process") 
parser.add_option("-e", "--exposureid",
                      dest="exposure_id",    # replaces "action", no default
                      help="exposureId to process") 

parser.add_option("-o", "--outputfile",
						dest="output_file",
						help="write quicklook output to this file")

parser.add_option("-d", "--dbinsert",
						dest="dbinsert",
						action="store_true",
						default=False,
						help="write to database if true")

global options
(options, args) = parser.parse_args()


# get the info from the command line arguments
if (len(sys.argv) > 1):
	mjd = sys.argv[1]
if options.plate_id:
    plate_id = int(options.plate_id)
if options.exposure_id:
    exposure_id = int(options.exposure_id)
if options.output_file:
	output_file = options.output_file
dbinsert = options.dbinsert


if (mjd == None):
    print
    print "Please specify the mjd to process."
    print
    print "Enter '%s --help' for more details." % sys.argv[0]
    print
    sys.exit()

try:
	data_dir = os.environ["APQLDATA_DIR"]
except:
	raise RuntimeError("Failed: APQLDATA_DIR is not defined")

try:
	#archive_dir = os.environ["APQLARCHIVE_DIR"]
	archive_dir = '/home/apogee/manual_quicklook/bin/archive/'
except:
	raise RuntimeError("Failed: APQLARCHIVE_DIR is not defined")

#plugmap_dir = '/data-ql/plugmaps/'
plugmap_dir = '/home/apogee/manual_quicklook/bin/tmp_plugmapAfiles/'
ics_dir = '/data-ics/'
raw_dir = os.path.join(data_dir,mjd)
mysession = db.Session()

#Verify that all the UTR files were copied from the ICS (won't do anything if there are no longer files
#in the ICS directory)
dayOfSurvey = str(int(mjd) - startOfSurvey)
indir = os.path.join(ics_dir,dayOfSurvey)
print indir
if exposure_id is not None:
	lst = glob.glob(os.path.join(indir,'apRaw-'+str(exposure_id)+'*.fits'))
else:
	lst = glob.glob(os.path.join(indir,'apRaw*.fits'))
lst.sort()
count=0
for infile in lst:
	# check that the file exists in the outdir
	outfile = os.path.join(raw_dir,os.path.basename(infile))
	if not os.path.exists(outfile):
		count+=1
		#just copy the file (no appending of fits keywords)
		shutil.copy(infile,outfile)
if count > 0:
	print '%d missing UTR files were copied from ICS directory' % (count)


#open a temporary file that will contain a list fo exposures, etc, to send to apql_wrapper_manual.pro
f_ql = tempfile.NamedTemporaryFile(delete=False)
listfile = f_ql.name

# get list of exposures for the mjd
if exposure_id is not None:
	rawfiles = glob.glob(os.path.join(raw_dir,'apRaw-'+str(exposure_id)+'*.fits'))
	exposures = sorted(set([os.path.basename(fn).split('-')[1] for fn in rawfiles]))
	if len(exposures) == 0:
		raise RuntimeError("Exposure %s not found for MJD %s" % (exposure_id, mjd))
else:
	rawfiles = glob.glob(os.path.join(raw_dir,'apRaw*.fits'))
	exposures = sorted(set([os.path.basename(fn).split('-')[1] for fn in rawfiles]))
	if len(exposures) == 0:
		raise RuntimeError("No exposures found for MJD %s" % (mjd))

count_exp = 0
# for each exposure
for exp in exposures:

	#get raw file and read cartidge and pointing from header
	exp_files = sorted(glob.glob(os.path.join(raw_dir,'apRaw-'+str(exp)+'*.fits')))
	hdulist=pyfits.open(exp_files[0])
	plateId = hdulist[0].header['PLATEID']

	#if user has specified a plate and this file is for a different plate, then skip
	if plate_id is not None and plateId != plate_id:
		continue
	
	survey=mysession.query(Survey).join(PlateToSurvey).join(Plate).filter(Plate.plate_id==plateId)
	if survey.count() > 0:
		if survey[0].label.upper().find("APOGEE") == -1 and survey[0].label.upper().find("MANGA") == -1:
			 # not an apogee or marvels plate - just skip
			 continue

	print 'Processing exposure ',exp

	cartId = hdulist[0].header['CARTID']
	pointing = hdulist[0].header['POINTING']
	
	# starttime is MJD in seconds
	startTime = int(mjd)*24.0*3600.0
	time_string = hdulist[0].header['DATE-OBS']
	p = time_string.find(':')
	if p > 0:
		hours = float(time_string[p-2:p])
		minutes = float(time_string[p+1:p+3])
		seconds = float(time_string[p+4:])
		startTime = startTime + seconds + (minutes + hours*60.0) * 60.0
	
	expTime = hdulist[0].header.get('EXPTIME')
	expType = hdulist[0].header.get('IMAGETYP')

	count_exp += 1
	# get plugmap
	if plateId != prevPlateId or cartId != prevCartId or pointing != prevPointing: 
		
		pm = mysession.query(PlPlugMapM).join(Plugging,Plate,Cartridge).\
			filter(Plate.plate_id==plateId).\
			filter(Cartridge.number==cartId).\
			filter(PlPlugMapM.pointing_name==pointing).\
			order_by(PlPlugMapM.fscan_mjd.desc()).order_by(PlPlugMapM.fscan_id.desc())

		if pm.count() == 0:
			raise RuntimeError("No plugmap found for plate %d cartidge %d plugging %s" % (plateId, cartId, pointing))
		else:
			pm=pm[0]		

		# create apogee plugmap file
		fname = pm.filename
		p = fname.find('MapM')
		fname  = os.path.join(plugmap_dir,fname[0:p+3]+'A'+fname[p+4:])
		print 'Creating APOGEE plugmap file', fname
		makeApogeePlugMap(mysession,pm,fname)
		
		prevPlateId = plateId
		prevCartId = cartId
		
		prevPointing = pointing
		prevScanId = pm.fscan_id
		prevScanMJD = pm.fscan_mjd
		
		# write a copy to the archive directory
		# define the current mjd archive directory to store the plPlugMapA file
		arch_dir = os.path.join(archive_dir, mjd)
		if not os.path.isdir(arch_dir):
			os.mkdir(arch_dir, 0o0775)

		res=os.path.split(fname)
		archivefile = os.path.join(arch_dir,res[1])
		print 'Archiving APOGEE plugmap file to ',archivefile
		shutil.copyfile(fname,archivefile)
	

	# create exposure entry in DB
	survey = mysession.query(Survey).filter(Survey.label==surveyLabel)
	survey = survey[0]
	#check if exposure already exists in DB, otherwise create it
	exp_obj = mysession.query(Exposure).filter(Exposure.survey_pk==survey.pk).filter(Exposure.exposure_no==int(exp))
	if exp_obj.count() == 1:
		exp_pk=exp_obj[0].pk
	elif exp_obj.count() == 0: 	
		exp_pk = addExposure(mysession, prevScanId, prevScanMJD, prevPlateId, mjd, int(exp), surveyLabel, startTime, expTime, expType, 'Manual apogeeQL')
	else:
		raise RuntimeError("ERROR: Multiple exposures already exist for exposure number %d, survey %s" \
						 % (exposureNo, survey.label))


	# run apql_wrapper_manual IDL code
	plugfile = fname
#	plugfile='tmp_plugmapAfiles/plPlugMapA-8264-57111-01.par'
	#write mjd, exp, plate_id and plug file to tmp file to send to apql_wrapper_manual 
	f_ql.write('%s, %s, %s, %s\n' %(mjd, exp, plateId, plugfile))
f_ql.close()


if output_file is not None:
	ql_cmd = 'idl -e "apql_wrapper_manual,\'%s\',no_dbinsert=%i, outfile=\'%s\'"' % (listfile, not dbinsert, output_file)
else:
	ql_cmd = 'idl -e "apql_wrapper_manual,\'%s\',no_dbinsert=%i"' % (listfile, not dbinsert)
print ql_cmd

ql_process = subprocess.Popen(ql_cmd, stderr=subprocess.PIPE, shell=True)
output=ql_process.communicate()[0] 
if output is not None:
	print output

#delete tmp list file
os.remove(listfile)


#close db connection
db.engine.dispose()


if count_exp == 0:
	raise RuntimeError("No APOGEE or MANGA exposures found for plate %s on MJD %s" % (plate_id, mjd))

