#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import traceback
import os, shutil, glob
import time
import datetime

from astropy.time import Time
from astropy.io import fits as pyfits

from apogee_mountain.quicklookThread import do_quickred, do_quicklook
from apogee_mountain.bundleThread import do_bundle
from sdssdb.peewee.sdss5db.opsdb import Exposure
from clu import LegacyActor
import clu.command
import actorcore.utility.fits as actorFits

from apogeeql import __version__, log, config
from apogeeql.tools import wrapBlocking, Timer
from apogeeql.Commands import parser as apogeeql_parser


def writeExposure(params):
    """Write exposure to DB
    """

    exp = Exposure(**params)
    exp.save()
    return exp.pk


class Apogeeql(LegacyActor):

    parser = apogeeql_parser

    def __init__(self, **kwargs):
        observatory = os.getenv("OBSERVATORY")

        if observatory == "APO":
            tcc = "tcc"
        else:
            tcc = "lcotcc"
        monitoredActors = ["mcp", "guider", "cherno", tcc, "apogee", "apogeecal", "hal", "jaeger"]

        super().__init__(name="apogeeql",
                         models=monitoredActors,
                         log=log,
                         host=config["tron"]["tronHost"],
                         port=config["tron"]["port"],
                         version=__version__,
                         **kwargs)

        #
        # register the keywords that we want to pay attention to
        #
        # if observatory == "APO":
        #     self.models["tcc"]["inst"].register_callback(self.TCCInstCB)
        # else:
        #     self.models["lcotcc"]["inst"].register_callback(self.TCCInstCB)
        # self.models["apogee"]["exposureState"].register_callback(self.ExposureStateCB)
        # self.models["apogee"]["exposureWroteFile"].register_callback(self.exposureWroteFileCB)
        # self.models["apogee"]["exposureWroteSummary"].register_callback(self.exposureWroteSummaryCB)
        # self.models["apogee"]["ditherPosition"].register_callback(self.ditherPositionCB)
        # self.models["jaeger"]["configuration_loaded"].register_callback(self.configurationLoadedCB)

        # only the ics_datadir is defined in the cfg file - expect the datadir to be an
        # environment variable set by the apgquicklook setup
        # (don't want to keep it in 2 different places)
        try:
            self.datadir = os.environ["APQLDATA_DIR"]
        except:
            log.error("Failed: APQLDATA_DIR is not defined")
            traceback.print_exc()

        # environment variable set by the apgquicklook setup (don't want to keep it in 2 different places)
        try:
            self.archive_dir = os.environ["APQLARCHIVE_DIR"]
        except:
            log.error("Failed: APQLARCHIVE_DIR is not defined")
            traceback.print_exc()

        self.qlconfig = config["apogeeql"]

        # MJD value of start of survey (Jan 1 2011) for filenames (55562)
        self.startOfSurvey = self.qlconfig.get('startOfSurvey')
        self.snrAxisRange = [self.qlconfig.get('snrAxisMin'), self.qlconfig.get('snrAxisMax')]
        self.rootURL = self.qlconfig.get('rootURL')
        self.plugmap_dir = self.qlconfig.get('plugmap_dir')
        self.ics_datadir = self.qlconfig.get('ics_datadir')
        self.cdr_dir = self.qlconfig.get('cdr_dir')
        self.summary_dir = self.qlconfig.get('summary_dir')
        self.delFile = self.qlconfig.get('delFile')
        self.criticalDiskSpace = self.qlconfig.get('criticalDiskSpace')
        self.seriousDiskSpace = self.qlconfig.get('seriousDiskSpace')
        self.warningDiskSpace = self.qlconfig.get('warningDiskSpace')
        self.updateInterval = int(self.qlconfig.get('updateInterval'))
        self.diskAlarmInterval = int(self.qlconfig.get('diskAlarmInterval'))

        self.prevCartridge=-1
        self.prevPlate = None
        self.prevScanMJD = -1
        self.prevPointing='A'
        self.config_id = None
        self.design_id = None
        self.field_id = None
        self.summary_file = ''
        self.inst = ''
        self.startExp = False
        self.endExp = False
        self.expState=''
        self.expType=''
        self.numReadsCommanded=0
        self.actor=''
        self.exp_pk = 0
        self.frameid = ''
        self.ditherPos = 0.0
        self.namedDitherPos = ''
        self.rawdir = '/data/apogee/raw/'

        self.statusTimer = Timer()
        self.diskTimer = Timer()

    async def start(self):
        await super().start()

        if observatory == "APO":
            self.models["tcc"]["inst"].register_callback(self.TCCInstCB)
        else:
            self.models["lcotcc"]["inst"].register_callback(self.TCCInstCB)
        self.models["apogee"]["exposureState"].register_callback(self.ExposureStateCB)
        self.models["apogee"]["exposureWroteFile"].register_callback(self.exposureWroteFileCB)
        self.models["apogee"]["exposureWroteSummary"].register_callback(self.exposureWroteSummaryCB)
        self.models["apogee"]["ditherPosition"].register_callback(self.ditherPositionCB)
        self.models["jaeger"]["configuration_loaded"].register_callback(self.configurationLoadedCB)

        await self.periodicStatus()
        await self.periodicDisksStatus()

    async def TCCInstCB(self, model_property):
        '''callback routine for tcc.inst'''
        keyVar = model_property.value
        self.inst = keyVar[0]

    async def configurationLoadedCB(self, model_property):
        '''callback routine for jaeger.configuration_loaded'''
        # https://github.com/sdss/actorkeys/blob/58598b120b70693774d042eedcbd3a07461cd2fc/python/actorkeys/jaeger.py#L59
        # jaeger.configuration_loaded
        # configuration_id, design_id, field_id, ra_boresight, dec_boresight, position_angle, alt_boresight, az_boresight,
        #    summary_file
        # Key('configuration_loaded',
        #   Int('configuration_id', help='Configuration ID'),
        #   Int('design_id', help='Design ID'),
        #   Int('field_id', help='Field ID'),
        #   Float('ra_boresight', help='RA of the boresight pointing'),
        #   Float('dec_boresight', help='Dec of the boresight pointing'),
        #   Float('position_angle', help='Position angle of the pointing'),
        #   Float('alt_boresight', help='Altitude of the boresight pointing'),
        #   Float('az_boresight', help='Azimuth of the boresight pointing'),
        #   String('summary_file', help='Summary file path'))

        keyVar = model_property.value
        print("configurationLoadedCB=",keyVar)

        # if pointing is None than just skip this
        if keyVar[1] == None:
            return

        config_id = int(keyVar[0])
        design_id = int(keyVar[1])
        field_id = str(keyVar[2])
        summary_file = str(keyVar[8])

        # Make absolute path for configuration summary file
        # We can't use sdss_access because it requires python 3
        #if os.path.dirname(summary_file)=='':
        #   configgrp = '{:0>4d}XX'.format(int(configid) // 100)
        #   config_dir = os.environ['SDSSCORE_DIR']+'apo/summary_files/'+configgrp+'/'
        #   summary_file = config_dir+summary_file
        # summary_file has the absolute path!

        if config_id != self.prevPlate:
            # pass the info to IDL QL
            # Apogeeql.actor.ql_in_queue.put(('configInfo', config_id, summary_file))

            # print 'plugMapFilename=%s' % (fname)
            self.config_id = config_id
            self.design_id = design_id
            self.field_id = field_id
            self.summary_file = summary_file
            self.prevPointing = config_id
            self.prevPlate = config_id
            self.prevCartridge = 'FPS'

    async def ExposureStateCB(self, model_property):
        '''callback routine for apogeeICC.exposureState '''
        # exposureState expState expType nReads expName
        #
        # exposureState EXPOSING SCIENCE 50 apRaw-0054003      -> mark start of exposure
        # utrReadState apRaw-0054003 READING 1 50              -> mark first UTR read status
        # utrReadState apRaw-0054003 SAVING 1 50
        # utrReadState apRaw-0054003 DONE 1 50
        # exposureWroteFile apRaw-0054003-001.fits             -> mark first UTR read
        # exposureWroteFile apRaw-0054003-002.fits             -> mark first UTR read
        # exposureWroteSummary apRaw-0054003.fits              -> mark first CDS read
        # ...
        # exposureWroteFile apRaw-0054003-050.fits             -> mark last requested UTR read
        # exposureWroteSummary apRaw-0054003.fits              -> mark first UTR read
        # exposureState DONE SCIENCE 50 apRaw-0054003          -> mark end of exposure
        # test existance of passed variable
        # print 'ExposureStateCB keyVar=',keyVar
        
        keyVar = model_property.value

        # if not keyVar.isGenuine:
        #     return

        self.expState = keyVar[0]
        if self.expState.upper() == 'EXPOSING':
            # check the communication to IDL quicklook
            # Apogeeql.startQuickLook(Apogeeql.actor)
            self.startExp = True
            self.startExp = True
            self.endExp = False
            self.expType = keyVar[1].upper()
            self.numReadsCommanded = int(keyVar[2])
            # we may have to do something special for QL at the start of a new exposure
        elif self.expState.upper() in ['DONE', 'STOPPED', 'FAILED']:
            # ignore if we weren't actually exposing
            if self.startExp == True:
                self.startExp = False
                self.endExp = True
                self.expType = keyVar[1].upper()
                filebase = keyVar[3]
                # make sure we have all the UTR files before bundling
                await wrapBlocking(self.completeUTR, filebase)
                self.numReadsCommanded = 0
                res = keyVar[3].split('-')
                self.frameid = res[1][:8]
                mjd5 = int(self.frameid[:4]) + int(self.startOfSurvey)

                args = ('UTRDONE', self.actor, self.frameid, mjd5, self.exp_pk)
                await wrapBlocking(do_quickred, args, self.summary_file,
                                                self.rawdir, self.namedDitherPos)
                await wrapBlocking(do_bundle, "BUNDLE", self.frameid, mjd5, self.exp_pk)
                # Apogeeql.actor.ql_in_queue.put(('UTRDONE',Apogeeql.actor,Apogeeql.frameid, mjd5, Apogeeql.exp_pk),block=True)
                # Apogeeql.actor.bndl_in_queue.put(('BUNDLE',Apogeeql.frameid, mjd5, Apogeeql.exp_pk),block=True)

        elif self.expState.upper() != 'STOPPING':
            # when a stop was requested, a couple of images will still be coming in
            # only change the startExp in other cases
            self.startExp = False

    async def exposureWroteFileCB(self, model_property):
        '''callback routine for apogeeICC.exposureState '''
        # exposureWroteFile apRaw-0054003-001.fits   -> mark first UTR read
        # exposureWroteFile apRaw-0054003-002.fits   -> mark first UTR read
        # exposureWroteSummary apRaw-0054002.fits    -> mark first CDS read
        # ...
        # exposureWroteFile apRaw-0054003-050.fits   -> mark last requested UTR read
        # exposureWroteSummary apRaw-0054003.fits    -> mark last CDS read
        # exposureState DONE SCIENCE 50 apRaw-0054003          -> mark end of exposure
        # test existance of passed variable

        # if we're not actually exposing than skip (we get these messages from the hub when
        # starting the apogeeql if apogee ics is already started)
        # print "exposureWroteFileCB=",keyVar
        
        keyVar = model_property.value

        # if not keyVar.isGenuine:
        #     return

        filename=keyVar[0]
        if filename == None:
            return

        if len(filename) == 0:
            print("exposureWroteFileCB  -> Null filename received")
            return

        # create a new FITS file by appending the telescope fits keywords
        newfilename, starttime, exptime = await wrapBlocking(self.appendFitsKeywords,
                                                             filename)

        #Don't create a new exposure if the exposure is not an APOGEE or MANGA object
        #if self.prevPlate == -1:
        #   print "exposureWroteFileCB  -> Not an APOGEE/MANGA Object"
        #   return
        # COMMENTING THIS OUT. DLN 10/26/21

        # Exposure flavor
        # pk |   label
        # ----+------------
        #  1 | Science
        #  2 | Arc
        #  3 | Flat
        #  4 | Bias
        #  5 | Object
        #  6 | Dark
        #  7 | Sky
        #  8 | Calib
        #  9 | LocalFlat
        # 10 | SuperDark
        # 11 | SuperFlat
        # 12 | DomeFlat
        # 13 | QuartzFlat
        # 14 | ArcLamp
        expflavordict = {'Science':1, 'Arc':2, 'Flat':3, 'Bias':4, 'Object':5, 'Dark':6,
                              'Sky':7, 'Calib':8, 'LocalFlat':9, 'SuperDark':10, 'SuperFlat':11,
                              'DomeFlat':12, 'QuartzFlat':13, 'ArcLamp':14}
        # this converts the APOGEE STUI exptype to the exposure_flavor "label" value
        exptype2flavor = {'OBJECT':'Science', 'DARK':'Dark', 'INTERNALFLAT':'InternalFlat',
                                'QUARTZFLAT':'QuartzFlat', 'DOMEFLAT':'DomeFlat',
                                'ARCLAMP':'ArcLamp', 'BLACKBODY':'Calib'}

        # get the mjd from the filename
        res=filename.split('-')
        try:
            mjd = int(res[1][:4]) + int(self.startOfSurvey)
            readnum = int(res[2].split('.')[0])
            expnum = int(res[1])
        except:
            raise RuntimeError( "The filename doesn't match expected format (%s)" % (filename))

        if readnum == 1 or self.exp_pk == 0:

            #Create new exposure object
            #currently hard-coded for survey=APOGEE-2

             #if self.prevPlate > 15000:
             #    surveyLabel = 'MWM'
             #else:
             #    surveyLabel = 'APOGEE-2'
             # surveyLabel = 'MWM'

             try:
                # exposure_flavor "label" value for this exposure
                expflavorlabel = exptype2flavor.get(self.expType)
                if expflavorlabel is None:
                    expflavorlabel = 'Object'
                # get exposure_flavor_pk for this exposure
                expflavorpk = expflavordict.get(expflavorlabel)
                if expflavorpk is None:
                     expflavorpk = 5  # Object by default
                print('exptype = ',self.expType)
                print('expflavorpk = ',expflavorpk)
                # survey_pk=2 is for MWM
                params = {"configuration_id":self.config_id, "exposure_no":expnum,
                          "exposure_time":exptime, "exposure_flavor_pk":expflavorpk,
                          "start_time":datetime.datetime.now(),"survey_pk":2}
                new_pk = await wrapBlocking(writeExposure, params)
                self.exp_pk = new_pk

             except RuntimeError as e:
                 log.error('Failed in call addExposure for exposureNo %d' %expnum)
                 log.error('Exception: %s'%e)
                 raise RuntimeError('Failed in call addExposure for exposureNo %d' %expnum +'\n'+str(e))

        args = ('UTR', self, newfilename, self.exp_pk, readnum, self.numReadsCommanded)
        await wrapBlocking(do_quicklook, args, self.summary_file)

        # Apogeeql.actor.ql_in_queue.put(('UTR', self.actor, newfilename, self.exp_pk, readnum, self.numReadsCommanded),block=True)

    async def exposureWroteSummaryCB(self, model_property):

        '''callback routine for apogeeICC.exposureWroteSummary '''
        # exposureWroteFile apRaw-0054003-001.fits   -> mark first UTR read
        # exposureWroteFile apRaw-0054003-002.fits   -> mark first UTR read
        # exposureWroteSummary apRaw-0054003.fits    -> mark first CDS read
        # ...
        # exposureWroteFile apRaw-0054003-050.fits   -> mark last requested UTR read
        # exposureWroteSummary apRaw-0054003.fits    -> mark first CDS read
        # exposureState DONE SCIENCE 50 apRaw-0054003          -> mark end of exposure
        # test existance of passed variable
        # print "exposureWroteSummaryCB=",keyVar

        keyVar = model_property.value

        # if not keyVar.isGenuine:
        #     return

        filename = keyVar[0]
        res = (filename.split('-'))[1].split('.')
        dayOfSurvey = res[0][:4]
        mjd = int(dayOfSurvey) + int(self.startOfSurvey)
        indir = self.summary_dir
        outdir = self.cdr_dir

        try:
            indir  = os.path.join(indir, dayOfSurvey)
            infile = os.path.join(indir, filename)
            outdir = os.path.join(outdir, str(mjd))
            outfile = os.path.join(outdir, filename)
            if not os.path.isdir(outdir):
                await wrapBlocking(os.mkdir, outdir)
                # print 'Directory created at: ' + dest
            # t0=time.time()
            await wrapBlocking(shutil.copy2, infile, outfile)
            # print "shutil.copy2 took %f seconds" % (time.time()-t0)
        except:
            #raise RuntimeError( "Failed to copy the summary file (%s)" % (filename))
            raise RuntimeError( "Failed to copy the summary file (%s -> %s)" % (infile,outfile))

    async def ditherPositionCB(self, model_property):

        '''callback routine for apogeeICC.ditherPosition '''
        # ditherPosition=13.9977,A
        # test existance of passed variable
        # print "ditherPositionCB=",keyVar
        
        keyVar = model_property.value

        # if not keyVar.isGenuine:
        #     return

        # save the current dither pixel and named position
        self.ditherPos = float(keyVar[0])
        self.namedDitherPos = keyVar[1]
        # Apogeeql.actor.ql_in_queue.put(('ditherPosition',Apogeeql.ditherPos, Apogeeql.namedDitherPos),block=True)

    async def periodicStatus(self):
        '''Run some command periodically'''
        # self.callCommand('update')
        # reactor.callLater(int(self.config.get(self.name, 'updateInterval')), self.periodicStatus)

        await clu.command('status').parse()

        await self.statusTimer.start(self.updateInterval, self.periodicStatus)

    async def periodicDisksStatus(self):
        '''Check on the disk free space '''
        # self.callCommand('checkdisks')
        # reactor.callLater(int(self.config.get(self.name, 'diskAlarmInterval')), self.periodicDisksStatus)

        await clu.command('checkdisks').parse()

        self.diskTimer.start(self.diskAlarmInterval, periodicDisksStatus)

    def appendFitsKeywords(self, filename):
        '''make a copy of the input FITS file with added keywords'''

        # we need to form the paths where the file can be found and written
        # expecting something like: apRaw-DDDDXXXX-RRR.fits
        # where:
        # DDDD is a 4 digit day number starting with 0000 for Jan 1, 2011 which is MJD=55562
        # XXXX is a 4 digit exposure number of the day, starting with 0001
        # RRR is a 3 digit read number starting with 001 for the first read
        #
        # We are using the SDSS version of MJD which is MJD+0.3 days, which
        # rolls over at 9:48 am MST.
        #
        # The directories from the ICS are named from the day-of-survey 4 digits
        # The directories for the quicklook and archive are by the 5 digits MJD
        #
        res = filename.split('-')
        indir = self.ics_datadir
        outdir = self.datadir

        try:
            indir  = os.path.join(self.ics_datadir,res[1][:4])
            mjd = int(res[1][:4])+int(self.startOfSurvey)
            outdir = os.path.join(self.datadir,str(mjd))
        except:
            raise RuntimeError( "The filename doesn't match expected format (%s)" % (filename))

        # create directory if it doesn't exist
        if not os.path.isdir(outdir):
            os.mkdir(outdir)
        outFile = os.path.join(outdir, filename)
        filename = os.path.join(indir, filename)

        # Since the Java nom.tam.fits library used by the ICS has an incompatible
        # checksum calculation with pyfits, we're rolling our own here

        # first extract the value of the checksum from the fits header (pyfits.getval removes
        # any checksum or datasum keywords)
        f=open(filename,'rb')
        checksum = None
        # only read the first 72 lines (which should be the whole header plus padding)
        for p in range(72):
             line = f.read(80)
             if line[0:8] == 'END     ':
                  break
             if line[0:8] == 'CHECKSUM':
                  checksum = line[11:27]
                  cs_comment = line[33:80]

        f.close()
        # don't touch the data, which is supposed to be uint16s.
        hdulist = pyfits.open(filename, do_not_scale_image_data=True, uint16=True)
        if checksum != None:
             # validate the value of the checksum found (corresponding to DATASUM in pyfits)
             # calulate the datasum
             ds = hdulist[0]._calculate_datasum('standard')

             # add a new CHECKSUM line to the header (pyfits.open removes it) with same comment
             hdulist[0].header.update("CHECKSUM",'0'*16, cs_comment)

             # calulate a new checksum
             cs=hdulist[0]._calculate_checksum(ds,'standard')
             if cs != checksum:
                  log.error("CHECKSUM Failed for file %s" % (filename))
             else:
                  # log.info("CHECKSUM checked ok")
                  f = open(os.path.join(outdir, self.delFile),'a')
                  f.write(filename+'\n')
                  f.close()

        # force these to be ints:
        # As of August 2013, the ICS writes them both as floats, but the
        # FITS standard wants them to be ints.
        bscale = int(hdulist[0].header.get('BSCALE',1))
        bzero = int(hdulist[0].header.get('BZERO',32768))
        del hdulist[0].header['BSCALE']
        del hdulist[0].header['BZERO']
        hdulist[0].header.update('BSCALE',bscale,after='GCOUNT')
        hdulist[0].header.update('BZERO',bzero,after='BSCALE')

        hdulist[0].header.update('TELESCOP' , 'SDSS 2-5m')
        hdulist[0].header.update('FILENAME' ,outFile)
        hdulist[0].header.update('EXPTYPE' ,self.expType)

        # get the calibration box status
        lampqrtz, lampune, lampthar, lampshtr, lampcntl = self.getCalibBoxStatus()
        hdulist[0].header.update('LAMPQRTZ',lampqrtz, 'CalBox Quartz Lamp Status')
        hdulist[0].header.update('LAMPUNE',lampune, 'CalBox UNe Lamp Status')
        hdulist[0].header.update('LAMPTHAR',lampthar, 'CalBox ThArNe Lamp Status')
        hdulist[0].header.update('LAMPSHTR',lampshtr, 'CalBox Shutter Lamp Status')
        hdulist[0].header.update('LAMPCNTL',lampcntl, 'CalBox Controller Status')

        # add gang connector state
        gangstate,gstate = self.getGangState()
        hdulist[0].header.update('GANGSTAT',gstate, 'APOGEE Gang Connector State')

        # add shutter information
        # shutterLimitSwitch=False,True   shutter is closed
        # shutterLimitSwitch=True,False   shutter is open
        # Any other combination means there's something wrong with the shutter.
        shutterstate = self.getShutterState()
        hdulist[0].header.update('SHUTTER',shutterstate, 'APOGEE Shutter State')

        # Add FPI information
        #hdulist[0].header.update('LAMPFPI',lampfpi, 'FPI Lamp shutter status')

        """
        # guider i refractionCorrection=1.00000
        refraction = Apogeeql.actor.models['guider'].keyVarDict['refractionCorrection'][0]
        refraction = numpy.nan_to_num(refraction)
        hdulist[0].header.update('REFRACOR',refraction, 'guider refractionCorrection')
        """

        # guider i seeing=2.09945
        #seeing = Apogeeql.actor.models['guider'].keyVarDict['seeing'][0]
        seeing = self.models['cherno'].keyVarDict['astrometry_fit'][4]
        #print('cherno astrometry_fit: ',Apogeeql.actor.models['cherno'].keyVarDict['astrometry_fit'])
        #print('seeing: ',seeing)
        try:
             seeing=float(seeing)
             if seeing == float('NaN'):
                  seeing=0.0
        except:
             seeing=0.0
        hdulist[0].header.update('SEEING',seeing, 'RMS seeing from guide fibers')

        # starttime is MJD in seconds
        time_string = hdulist[0].header['DATE-OBS']
        starttime = Time(time_string,format='isot',scale='utc').mjd * 86400.0

        exptime = hdulist[0].header['exptime']

        cards=[]
        cards.extend(actorFits.mcpCards(self.models, cmd=self.bcast))

        observatory = os.getenv("OBSERVATORY")
        if observatory == "APO" :
             cards.extend(actorFits.tccCards(self.models, cmd=self.bcast))
        else :
             cards.extend(actorFits.lcoTCCCards(self.models, cmd=self.bcast))

        cards.extend(actorFits.plateCards(self.models, cmd=self.bcast))

        # Get the guider (cherno) offsets.
        default_offset = self.models['cherno'].keyVarDict['default_offset']
        offset = self.models['cherno'].keyVarDict['offset']
        for idx, name in enumerate(['RA', 'DEC', 'PA']):
            default_ax = default_offset[idx]
            offset_ax = offset[idx]
            if (default_ax is None or offset_ax is None or
                    float(default_ax) == -999.0 or float(offset_ax) == -999.0):
                full_offset = -999.0
            else:
                full_offset = float(default_ax) + float(offset_ax)
            cards.append(('OFF'+name, full_offset, 'Guider offset in '+name))

        for name, val, comment in cards:
             try:
                  hdulist[0].header.update(name, val, comment)
             except:
                  log.warn('text="failed to add card: %s=%s (%s)"' % (name, val, comment))


        # New SDSS-V FPS keywords
        # CARTID (set to FPS-N), DESIGNID, CONFID, and FIELDID.
        hdulist[0].header.update('CARTID','FPS', 'Using FPS')
        hdulist[0].header.update('CONFIGID',self.config_id, 'FPS configID')
        hdulist[0].header.update('DESIGNID',self.design_id, 'DesignID')
        hdulist[0].header.update('FIELDID',self.field_id, 'FieldID')
        hdulist[0].header.update('CONFIGFL',self.summary_file, 'config summary file')

        # repair (if possible) any problems with the header (mainly OBSCMNT too long)
        hdulist.verify('fix')
        hdulist.writeto(outFile, clobber=True, output_verify='warn', checksum=True)
        os.chmod(outFile,0o444) # all read only
        return outFile, starttime, exptime

    def getShutterState(self):
        """ Get APOGEE shutter state."""

        # add shutter information
        # shutterLimitSwitch=False,True   shutter is closed
        # shutterLimitSwitch=True,False   shutter is open
        # Any other combination means there's something wrong with the shutter.

        # get the shutter status from the actor
        shutterinfo = self.models['apogee'].keyVarDict['shutterLimitSwitch']
        if tuple(shutterinfo) == (False,True):
          shutterstate = 'Closed'
        elif tuple(shutterinfo) == (True,False):
          shutterstate = 'Open'
        else:
          shutterstate = 'Unknown'

        print('shutterinfo = ',shutterinfo)
        print('shutterstate = ',shutterstate)

        return shutterstate

    def getGangState(self):
        """ Get APOGEE gang connector state."""

        # get the lamp status from the actor
        gangstate = self.models['mcp'].keyVarDict['apogeeGang'][0]
        gstate = 'Podium'
        if str(gangstate)=='17' or str(gangstate)=='18':
          gstate = 'FPS'

        # Key('apogeeGang',
        # Enum('0', '1', '17', '4', '12', '20', '28',
        #      labelHelp=('Unknown', 'Disconnected', 'At Cart', 'Podium?',
        #                 'Podium: dense', 'Podium + FPI', 'Podium dense + FPI'))),

        print('gangstate = ',gangstate)
        print('gstate = ',gstate)

        return gangstate, gstate

    def getCalibBoxStatus(self):
         """Insert a new row in the platedb.exposure table """

         # get the lamp status from the actor
         lampqrtz = self.models['apogeecal'].keyVarDict['calSourceStatus'][0]
         lampune  = self.models['apogeecal'].keyVarDict['calSourceStatus'][1]
         lampthar = self.models['apogeecal'].keyVarDict['calSourceStatus'][2]
         lampshtr = self.models['apogeecal'].keyVarDict['calShutter'][0]
         lampcntl = self.models['apogeecal'].keyVarDict['calBoxController'][0]

         return lampqrtz, lampune, lampthar, lampshtr, lampcntl

    def completeUTR(self, filebase=None):
        """Verifies that all of the UTR files where copied from the ICS"""

        # expecting something like: apRaw-DDDDXXXX
        # make sure a filebase was passed
        if not filebase:
          return

        res=filebase.split('-')
        try:
            indir  = os.path.join(self.ics_datadir,res[1][:4])
            mjd = int(res[1][:4])+int(self.startOfSurvey)
            outdir = os.path.join(self.datadir,str(mjd))
        except:
            raise RuntimeError( "The filename doesn't match expected format (%s)" % (filename))

        lst = glob.glob(os.path.join(indir,filebase+'*.fits'))
        lst.sort()
        count=0
        for infile in lst:
            # check that the file exists in the outdir
            outfile = os.path.join(outdir,os.path.basename(infile))
            if not os.path.exists(outfile):
                # need to annotate and copy the file
                count+=1
                # should we try this or just make a copy without the annotation?
                newfilename, starttime, exptime = self.appendFitsKeywords(os.path.basename(infile))
                if not os.path.exists(outfile):
                    # copy the file if appendFitsKeywords did not work
                    shutil.copy(infile, outdir)

        if count > 0:
            self.write(message_code="w", message={'text': f"{filebase} had {count} missing UTR"})
            log.info(f'APOGEEQL -> had {count} missing UTR')
        return

#-------------------------------------------------------------
if __name__ == '__main__':
    pass
