#!/usr/bin/env python

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

# the APOTestDatabaseconnection will make version v1_0_6 of platedb
# work (with the new hooloovookit stuff) - requires version v1_0_6 of later
# from platedb.APOTestDatabaseConnection import db
from platedb.APODatabaseConnection import db
from platedb.ModelClasses import *
from catalogdb.ModelClasses import *

import opscore.actor.model
import opscore.actor.keyvar

import actorcore.Actor
import actorcore.CmdrConnection as actorCmdrConnection
import actorcore.utility.fits as actorFits

import pyfits
import actorkeys
import traceback

#
# Import sdss3logging before logging if you want to use it
#
import logging
import os, signal, subprocess, tempfile, shutil
import time
import types
import yanny
# import numpy

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class QuickLookLineServer(LineReceiver):
    def connectionMade(self):
        # the IDL end-of-line string is linefeed only (no carriage-return) 
        # we need to change the delimiter to be able to detect a full line
        # and have lineReceived method be called (otherwise it just sits there)
        self.delimiter = '\n'
        self.peer = self.transport.getPeer()
        self.factory.qlActor.qlSources.append(self)
        logging.info("Connection from %s %s" % (self.peer.host, self.peer.port))
        print "Connection from ", self.peer.host, self.peer.port
        # ping the quicklook
        if self.factory.qlActor.ql_pid > 0:
           for s in self.factory.qlActor.qlSources:
              s.sendLine('PING')
              s.sendLine('STARTING')

    def lineReceived(self, line):
        if line.upper()=='PONG':
            # normal response from aliveness test
            self.factory.qlActor.watchDogStatus = True
        elif line.upper()=='QUIT':
            # request to disconnect
            self.transport.loseConnection()
        elif line.upper()=='STARTED':
            # we got the initial response from the apql_wrapper
            # send the PointingInfo stuff
            if self.factory.qlActor.prevPlate != -1:
               for s in self.factory.qlActor.qlSources:
                  s.sendLine('plugMapInfo=%s,%s,%s,%s' % (self.factory.qlActor.prevPlate, \
                       self.factory.qlActor.prevScanMJD, self.factory.qlActor.prevScanId, \
                       self.factory.qlActor.plugFname))
        elif line == "callback":
            logging.info("preparing callback")
            reactor.callLater(5.0,self.sendcomment)
        else:
            # assume the messages are properly formatted to pass along
            # print 'Received from apql_wrapper.pro: ',line
            self.factory.qlActor.bcast.finish(line)

    def connectionLost(self, reason):
        logging.info("Disconnected from %s %s"  % (self.peer.port, reason.value))

    def sendcomment(self):
        logging.info("in the callback routine")


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class QLFactory(ClientFactory):
    protocol = QuickLookLineServer
    def __init__(self, qlActor): 
        self.qlActor=qlActor

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class QuickRedLineServer(LineReceiver):
    def connectionMade(self):
        # the IDL end-of-line string is linefeed only (no carriage-return) 
        # we need to change the delimiter to be able to detect a full line
        # and have lineReceived method be called (otherwise it just sits there)
        self.delimiter = '\n'
        self.peer = self.transport.getPeer()
        self.factory.qrActor.qrSources.append(self)
        logging.info("apqr_wrapper Connection from %s %s" % (self.peer.host, self.peer.port))
        print "apqr_wrapper -> Connection from ", self.peer.host, self.peer.port
        # ping the quicklook
        if self.factory.qrActor.ql_pid > 0:
           for s in self.factory.qrActor.qrSources:
              s.sendLine('PING')
              s.sendLine('STARTING')

    def lineReceived(self, line):
        if line.upper()=='PONG':
            # normal response from aliveness test
            self.factory.qrActor.watchDogStatus = True
        if line=='quit':
            # request to disconnect
            self.transport.loseConnection()
        elif line.upper()=='STARTED':
            # we got the initial response from the apql_wrapper
            # send the PointingInfo stuff
            if self.factory.qrActor.prevPlate != -1:
               for s in self.factory.qrActor.qrSources:
                  s.sendLine('plugMapInfo=%s,%s,%s,%s' % (self.factory.qrActor.prevPlate, \
                       self.factory.qrActor.prevScanMJD, self.factory.qrActor.prevScanId, \
                       self.factory.qrActor.plugFname))
        elif line == "callback":
            logging.info("preparing callback")
            reactor.callLater(5.0,self.sendcomment)
        else:
            # assume the messages are properly formatted to pass along
            # print 'Received from apqr_wrapper.pro: ',line
            self.factory.qrActor.bcast.finish(line)

    def connectionLost(self, reason):
        logging.info("Disconnected from %s %s"  % (self.peer.port, reason.value))

    def sendcomment(self):
        logging.info("in the callback routine")


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class QRFactory(ClientFactory):
    protocol = QuickRedLineServer
    def __init__(self, qrActor): 
        self.qrActor=qrActor

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Apogeeql(actorcore.Actor.Actor):

   models = {}
   ql_pid = 0
   qlSources=[]
   qr_pid = 0
   qrSources=[]
   prevCartridge=-1
   prevPlate=-1
   prevScanId = -1
   prevScanMJD = -1
   prevPointing='A'
   pluggingPk = 0
   apogeeSurveyPk=0
   plugFname=''
   inst = ''
   startExp = False
   endExp = False
   expState=''
   expType=''
   numReadsCommanded=0
   actor=''
   startOfSurvey=55562
   obs_pk = 0
   exp_pk = 0
   cdr_dir = ''
   summary_dir = ''
   frameid = ''

   # setup the variables for the watchdog to the IDL code
   watchDogStatus=True  # if True, the idl code is stil responding
   watchDogTimer=2.0    # should return a reply within this time if still alive

   def __init__(self, name, productName=None, configFile=None, debugLevel=30):
      actorcore.Actor.Actor.__init__(self, name, productName=productName, configFile=configFile)

      Apogeeql.actor=self
      self.headURL = '$HeadURL$'

      self.logger.setLevel(debugLevel)
      self.logger.propagate = True
      self.logger.info('Starting the Apogeeql Actor ...')

      # only the ics_datadir is defined in the cfg file - expect the datadir to be an
      # environment variable set by the apgquicklook setup 
      # (don't want to keep it in 2 different places)
      try:
         self.datadir = os.environ["APQLDATA_DIR"]
      except:
         self.logger.error("Failed: APQLDATA_DIR is not defined")
         traceback.print_exc()

      # MJD value of start of survey (Jan 1 2011) for filenames (55562)
      self.startOfSurvey = self.config.get('apogeeql','startOfSurvey')
      self.snrAxisRange = [self.config.get('apogeeql','snrAxisMin'), self.config.get('apogeeql','snrAxisMax')]
      self.rootURL = self.config.get('apogeeql','rootURL')
      self.plugmap_dir = self.config.get('apogeeql','plugmap_dir')
      self.ics_datadir = self.config.get('apogeeql','ics_datadir')
      self.cdr_dir = self.config.get('apogeeql','cdr_dir')
      self.summary_dir = self.config.get('apogeeql','summary_dir')
      self.qlPort = self.config.getint('apogeeql', 'qlPort') 
      self.qlHost = self.config.get('apogeeql', 'qlHost') 
      self.qrPort = self.config.getint('apogeeql', 'qrPort') 
      self.qrHost = self.config.get('apogeeql', 'qrHost') 

      #
      # Explicitly load other actor models. We usually need these for FITS headers.
      #
      self.models = {}
      # for actor in ["mcp", "guider", "platedb", "tcc", "apo", "apogeetest"]:
      for actor in ["mcp", "guider", "platedb", "tcc", "apogee", "apogeecal"]:
         self.models[actor] = opscore.actor.model.Model(actor)

      #
      # register the keywords that we want to pay attention to
      #
      self.models["tcc"].keyVarDict["inst"].addCallback(self.TCCInstCB, callNow=False)
      self.models["platedb"].keyVarDict["pointingInfo"].addCallback(self.PointingInfoCB, callNow=True)
      self.models["apogee"].keyVarDict["exposureState"].addCallback(self.ExposureStateCB, callNow=False)
      self.models["apogee"].keyVarDict["exposureWroteFile"].addCallback(self.exposureWroteFileCB, callNow=False)
      self.models["apogee"].keyVarDict["exposureWroteSummary"].addCallback(self.exposureWroteSummaryCB, callNow=False)

      #
      # Connect to the platedb
      #
      self.mysession = db.Session()


   @staticmethod
   def TCCInstCB(keyVar):
      '''callback routine for tcc.inst'''

      # print 'TCCInstCB keyVar=',keyVar
      Apogeeql.inst = keyVar[0]

   @staticmethod
   def PointingInfoCB(keyVar):
      '''callback routine for platedb.pointingInfo'''
      # plate_id, cartridge_id, pointing_id, boresight_ra, boresight_dec, hour_angle, temperature, wavelength
      # print "PointingInfoCB=",keyVar

      # if pointing is None than just skip this
      if keyVar[1] == None:
         return

      plate = int(keyVar[0])
      cartridge = int(keyVar[1])
      pointing = str(keyVar[2])

      """
      MODIFIED TO TESTING QUICKLOOK ON TEST DATABASE WITH SIMULATED DATA AND FAKE ICS

      cartridge = 3
      plate = 4918
      pointing = 'A'

      cartridge = 1
      plate = 4929
      pointing = 'A'
      """
 
      # print Apogeeql.actor.models['platedb'].keyVarDict['activePlugging']

      # find the plateIddb.survey.pk corresponding to APOGEE
      survey=Apogeeql.actor.mysession.query(Survey).filter(Survey.label=='APOGEE')
      if survey.count() > 0:
         Apogeeql.actor.apogeeSurveyPk = survey[0].pk

      if plate != Apogeeql.prevPlate or cartridge != Apogeeql.prevCartridge or pointing != Apogeeql.prevPointing:
         # we need to ignore all plates that are not for APOGEE or MARVELS
         survey=Apogeeql.actor.mysession.query(Survey).join(PlateToSurvey).join(Plate).filter(Plate.plate_id==plate)
         if survey.count() > 0:
              if survey[0].label.upper().find("APOGEE") == -1 and survey[0].label.upper().find("MARVELS") == -1:
                  # not an apogee or marvels plate - just skip
                  return

         # we need to extract and pass a new plugmap to IDL QuickLook
         pm = Apogeeql.actor.getPlPlugMapM(Apogeeql.actor.mysession, cartridge, plate, pointing)

         # routine returns a yanny par file
         # replace plPlugMapM-xxxx by plPlugMapA-xxxx
         fname = pm.filename
         p = fname.find('MapM')
         fname  = os.path.join(Apogeeql.actor.plugmap_dir,fname[0:p+3]+'A'+fname[p+4:])

         # print 'fname=',fname
         Apogeeql.actor.makeApogeePlugMap(pm, fname)

         # pass the info to IDL QL
         for s in Apogeeql.actor.qlSources:
             s.sendLine('plugMapInfo=%s,%s,%s,%s' % (plate, pm.fscan_mjd, pm.fscan_id, fname))
         for s in Apogeeql.actor.qrSources:
             s.sendLine('plugMapInfo=%s,%s,%s,%s' % (plate, pm.fscan_mjd, pm.fscan_id, fname))

         # print 'plugMapFilename=%s' % (fname)
         Apogeeql.prevPointing = pointing
         Apogeeql.prevPlate = plate
         Apogeeql.prevCartridge = cartridge
         Apogeeql.prevScanId = pm.fscan_id
         Apogeeql.prevScanMJD = pm.fscan_mjd
         # the plugging_pk is needed to find the right observation_pk
         Apogeeql.pluggingPk = pm.plugging_pk
         Apogeeql.plugFname = fname


   @staticmethod
   def ExposureStateCB(keyVar):
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
      if not keyVar.isGenuine:
         return

      Apogeeql.expState=keyVar[0]
      if Apogeeql.expState.upper() == 'EXPOSING':
         # check the communication to IDL quicklook
         # Apogeeql.startQuickLook(Apogeeql.actor)
         Apogeeql.startExp = True
         Apogeeql.startExp = True
         Apogeeql.endExp = False
         Apogeeql.expType = keyVar[1].upper()
         Apogeeql.numReadsCommanded = int(keyVar[2])
         # we may have to do something special for QL at the start of a new exposure
      elif Apogeeql.expState.upper() in ['DONE', 'STOPPED', 'FAILED']:
         # ignore if we weren't actually exposing
         if Apogeeql.startExp == True:
            Apogeeql.startExp = False
            Apogeeql.endExp = True
            Apogeeql.expType = keyVar[1].upper()
            Apogeeql.numReadsCommanded = 0
            res = keyVar[3].split('-')
            Apogeeql.frameid = res[1][:8]
            mjd5 = int(Apogeeql.frameid[:4]) + Apogeeql.startOfSurvey
            # do something for the quickreduction at the end of an exposure
            for s in Apogeeql.qlSources:
               s.sendLine('UTR=DONE')
            for s in Apogeeql.qrSources:
               s.sendLine('UTR=DONE,%s,%d,%s' % (Apogeeql.frameid, mjd5, Apogeeql.exp_pk))
      else:
         Apogeeql.startExp = False


   @staticmethod
   def exposureWroteFileCB(keyVar):
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
      if not keyVar.isGenuine:
         return

      filename=keyVar[0]
      if filename == None:
         return

      if len(filename) == 0:
         print "exposureWroteFileCB  -> Null filename received"
         return

      # create a new FITS file by appending the telescope fits keywords
      newfilename, starttime, exptime = Apogeeql.appendFitsKeywords(Apogeeql.actor,filename)

      # get the mjd from the filename
      res=filename.split('-')
      try:
         mjd = int(res[1][:4]) + Apogeeql.startOfSurvey
         readnum=int(res[2].split('.')[0])
         expnum=int(res[1])
      except:
         raise RuntimeError, ( "The filename doesn't match expected format (%s)" % (filename)) 

      if readnum == 1:
         # get the corresponding platedb.observation.pk (creating a new one if necessary)
         survey = Apogeeql.actor.mysession.query(Survey).join(PlateToSurvey).join(Plate).filter(Plate.plate_id==Apogeeql.prevPlate)

         Apogeeql.obs_pk = Apogeeql.actor.getObservationPk(mjd)

         # insert a new entry in the platedb.exposure table (one per UTR exposure)
         Apogeeql.exp_pk = Apogeeql.actor.getExposurePk(Apogeeql.obs_pk, expnum, starttime, exptime)

         # print 'Apogeeql.obs_pk=',Apogeeql.obs_pk
         # print 'Apogeeql.exp_pk=',Apogeeql.exp_pk

      for s in Apogeeql.qlSources:
         s.sendLine('UTR=%s,%d,%d,%d' % (newfilename, Apogeeql.exp_pk, readnum, Apogeeql.numReadsCommanded))

   @staticmethod
   def exposureWroteSummaryCB(keyVar):

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
      if not keyVar.isGenuine:
         return

      filename=keyVar[0]
      res=(filename.split('-'))[1].split('.')
      dayOfSurvey = res[0][:4]
      mjd = int(dayOfSurvey) + int(Apogeeql.startOfSurvey)
      indir=Apogeeql.actor.summary_dir
      outdir = Apogeeql.actor.cdr_dir

      try:
         indir  = os.path.join(indir,dayOfSurvey)
         infile= os.path.join(indir,filename)
         outdir = os.path.join(outdir,str(mjd))
         outfile= os.path.join(outdir,filename)
         if not os.path.isdir(outdir):
            os.mkdir(outdir)
            # print 'Directory created at: ' + dest
         # t0=time.time()
         shutil.copy2(infile,outfile)
         # print "shutil.copy2 took %f seconds" % (time.time()-t0)
      except:
         raise RuntimeError, ( "Failed to copy the summary file (%s)" % (filename)) 


   def connectQuickLook(self):
      '''open a socket through twisted to send/receive information to/from apogee_IDL'''
      # get the port from the configuratio file 
      reactor.listenTCP(self.qlPort, QLFactory(self))

   def startQuickLook(self):
      '''Open a twisted reactor to communicate with IDL socket'''
      #
      # check if an apogeeql_IDL process is already running before starting a new one
      if self.ql_pid > 0:
         self.stopQuickLook()

      # spawn the apogeeql IDL process and don't wait for its completion
      try:
         # get the string corresponding to the command to start the IDL quicklook process
         qlCommand = self.config.get('apogeeql','qlCommandName')
         qlCommand = qlCommand.strip('"')
         # this adds the arguments to the IDL command line
         qlCommand += " -args %s %s" % (self.qlHost, self.qlPort)
         # Popen does NOWAIT by default
         ql_process = subprocess.Popen(qlCommand.split(), stderr=subprocess.STDOUT)
         self.ql_pid = ql_process.pid
      except:
         self.logger.error("Failed to start the apogeeql_IDL process")
         traceback.print_exc()


   def stopQuickLook(self):
      '''If a quickLook IDL process already exists - just kill it (for now)'''
      if self.ql_pid == 0:
         p1=subprocess.Popen(['/bin/ps'],stdout=subprocess.PIPE)
         # apql_wrapper is the name of the program ran when starting IDL (in apogeeql.cfg)
         processName = (self.config.get('apogeeql','qlCommandName')).split()
         if '-e' in processName:
            # look at the name of the program called when IDL is started
            pos = processName[processName.index('-e')+1]
         else:
            pos = 0
         args = ['grep',processName[pos]]
         p2=subprocess.Popen(args,stdin=p1.stdout,stdout=subprocess.PIPE)
         output=p2.communicate()[0]
         p2.kill()
         p1.kill()
         if len(output) > 0:
            # process exists -> kill it
            self.ql_pid = output.split()[0]

      os.kill(self.ql_pid,signal.SIGKILL)
      killedpid, stat = os.waitpid(self.ql_pid, os.WNOHANG)
      if killedpid == 0:
         # failed in killing old IDL process
         # print error message here
         self.logger.warn("Unable to kill existing apogeeql_IDL process %s" % self.ql_pid)
      else:
         self.ql_pid = 0

   def connectQuickReduce(self):
      '''open a socket through twisted to send/receive information to/from apqr_wrapper IDL'''
      # get the port from the configuratio file 
      reactor.listenTCP(self.qrPort, QRFactory(self))

   def startQuickReduce(self):
      '''Open a twisted reactor to communicate with IDL socket'''
      #
      # check if an apogeeql_IDL process is already running before starting a new one
      if self.qr_pid > 0:
         self.stopQuickReduce()

      # spawn the apgreduce IDL process and don't wait for its completion
      try:
         # get the string corresponding to the command to start the IDL quicklook process
         qrCommand = self.config.get('apogeeql','qrCommandName')
         qrCommand = qrCommand.strip('"')
         # this adds the arguments to the IDL command line
         qrCommand += " -args %s %s" % (self.qrHost, self.qrPort)
         # Popen does NOWAIT by default
         qr_process = subprocess.Popen(qrCommand.split(), stderr=subprocess.STDOUT)
         self.qr_pid = qr_process.pid
      except:
         self.logger.error("Failed to start the apogeeqr_IDL process")
         traceback.print_exc()


   def stopQuickReduce(self):
      '''If a quickReduce IDL process already exists - just kill it (for now)'''
      if self.qr_pid == 0:
         p1=subprocess.Popen(['/bin/ps'],stdout=subprocess.PIPE)
         # apqr_wrapper is the name of the program ran when starting IDL (in apogeeqr.cfg)
         processName = (self.config.get('apogeeqr','qlCommandName')).split()
         if '-e' in processName:
            # look at the name of the program called when IDL is started
            pos = processName[processName.index('-e')+1]
         else:
            pos = 0
         args = ['grep',processName[pos]]
         p2=subprocess.Popen(args,stdin=p1.stdout,stdout=subprocess.PIPE)
         output=p2.communicate()[0]
         p2.kill()
         p1.kill()
         if len(output) > 0:
            # process exists -> kill it
            self.qr_pid = output.split()[0]

      os.kill(self.ql_pid,signal.SIGKILL)
      killedpid, stat = os.waitpid(self.qr_pid, os.WNOHANG)
      if killedpid == 0:
         # failed in killing old IDL process
         # print error message here
         self.logger.warn("Unable to kill existing apogeeql_IDL process %s" % self.qr_pid)
      else:
         self.qr_pid = 0

   def periodicStatus(self):
      '''Run some command periodically'''
      #
      # Obtain and send the data
      #
      self.callCommand('update')
      reactor.callLater(int(self.config.get(self.name, 'updateInterval')), self.periodicStatus)

   def sendAliveTest(self):
      '''Run some command periodically'''
      #
      # Obtain and send the data
      #
      self.watchDogStatus = False
      s.sendLine('PING')
      reactor.callLater(int(self.watchDogTimer), self.isApqlAlive)

   def isApqlAlive(self):
      '''Run some command periodically'''
      #
      # Obtain and send the data
      #
      if not self.watchDogStatus:
         # APQL is not reponding - > restart it
         self.logger.warn("APOGEEQL -> IDL_QL process not responding ... restarting ...")
         self.startQuickLook()

   def connectionMade(self):
      '''Runs this after connection is made to the hub'''
      #
      # Schedule an update.
      #
      reactor.callLater(3, self.periodicStatus)

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
      res=filename.split('-')
      indir=self.ics_datadir
      outdir = self.datadir

      try:
         indir  = os.path.join(self.ics_datadir,res[1][:4])
         mjd = int(res[1][:4])+int(self.startOfSurvey)
         outdir = os.path.join(self.datadir,str(mjd))
      except:
         raise RuntimeError, ( "The filename doesn't match expected format (%s)" % (filename)) 

      # create directory if it doesn't exist
      if not os.path.isdir(outdir):
         os.mkdir(outdir)
      outFile = os.path.join(outdir, filename)
      filename = os.path.join(indir, filename)

      hdulist = pyfits.open(filename, uint16=True)
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

      """
      # guider i refractionCorrection=1.00000
      refraction = Apogeeql.actor.models['guider'].keyVarDict['refractionCorrection'][0]
      refraction = numpy.nan_to_num(refraction)
      hdulist[0].header.update('REFRACOR',refraction, 'guider refractionCorrection')
      """

      # guider i seeing=2.09945
      seeing = Apogeeql.actor.models['guider'].keyVarDict['seeing'][0]
      hdulist[0].header.update('SEEING',seeing, 'RMS seeing from guide fibers')
      try:
          seeing=float(seeing)
          if seeing == float('NaN'):
              seeing=0.0
      except ValueError:
          seeing=0.0
      hdulist[0].header.update('SEEING',seeing, 'RMS seeing from guide fibers')

      # starttime is MJD in seconds
      starttime = mjd*24.0*3600.0
      exptime = hdulist[0].header['exptime']

      cards=[]
      cards.extend(actorFits.mcpCards(self.models, cmd=self.bcast))
      cards.extend(actorFits.tccCards(self.models, cmd=self.bcast))
      cards.extend(actorFits.plateCards(self.models, cmd=self.bcast))

      for name, val, comment in cards:
          try:
              hdulist[0].header.update(name, val, comment)
          except:
              self.logger.warn('text="failed to add card: %s=%s (%s)"' % (name, val, comment))

      hdulist.writeto(outFile, clobber=True)
      return outFile, starttime, exptime


   def getPlPlugMapM(self, session, cartridgeId, plateId, pointingName):
       """Return the plPlugMapM given a plateId and pointingName"""

       try:
           pm = session.query(PlPlugMapM).join(Plugging,Plate,Cartridge,ActivePlugging).\
                   filter(Plate.plate_id==plateId).\
                   filter(Cartridge.number==cartridgeId).\
                   filter(PlPlugMapM.pointing_name==pointingName).one()
       except sqlalchemy.orm.exc.NoResultFound:
           raise RuntimeError, ("NO plugmap from for plate %d" % (plateId))
       except sqlalchemy.orm.exc.MultipleResultsFound:
           raise RuntimeError, ("More than one plugmap from for plate %d" % (plateId))

       return pm

   def makeApogeePlugMap(self, plugmap, newfilename):
       """Return the plPlugMapM given a plateId and pointingName"""

       from sqlalchemy import and_

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
       ph = self.mysession.query(Fiber).join(PlateHole).join(CatalogObject).\
             filter(Fiber.pl_plugmap_m_pk==plugmap.pk).order_by(Fiber.fiber_id).\
             values('fiber_id','j','h','ks','tmass_style_id','apogee_target1','apogee_target2')
                         
       # we'll use the target1 and target2 to define the type of target
       # these are 32 bits each with each bit indicating a type
       skymask = 16
       hotmask = 512
       extmask = 1024
       starmask = skymask | hotmask

       # loop through the list and update the PLUGMAPOBJ
       for fid, j_mag, h_mag, k_mag, tmass_style, t1, t2 in ph:
          ind = p0['PLUGMAPOBJ']['fiberId'].index(fid)
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
        
       # delete file if it already exists
       if os.path.isfile(newfilename):
          os.remove(newfilename)

       p0.write(newfilename)

       return 


   def getObservationPk(self, mjd):
       """Insert a new row in the platedb.observation table if needed"""

       # make sure the currently loaded plate is from APOGEE/MARVELS
       survey = self.mysession.query(Survey).join(PlateToSurvey).join(Plate).filter(Plate.plate_id==self.prevPlate)

       if survey.count() >= 1:
          if (survey[0].label).upper().find('APOGEE') >= 0 or (survey[0].label).upper().find('MARVELS') >= 0:
             # a MARVELS / APOGEE plate
             # get the plate_pointing_pk from the database
             platePointing=self.mysession.query(PlatePointing).filter(PlatePointing.pointing_name==self.prevPointing).\
                   join(Plate).filter(Plate.plate_id==self.prevPlate)
             if platePointing.count() != 1:
                 # found more than one entry for the plate_pointing
                 raise RuntimeError, (\
                       "Found more than one platedb.plate_pointing for plate %d and pointing %s" % \
                       (plate, pointing))
             # see if a matching entry in the observation table exists
             platePointing = platePointing[0]
             observation=self.mysession.query(Observation).filter(Observation.plate_pointing_pk==platePointing.pk).\
                   filter(Observation.plugging_pk==self.pluggingPk).filter(Observation.mjd==mjd)
             if observation.count() == 0:
                # no observation entry found -> create one (start a new observation)
                self.mysession.begin()
                new_obs=Observation()
                new_obs.mjd=mjd
                new_obs.plate_pointing_pk = platePointing.pk
                new_obs.plugging_pk = self.pluggingPk
                new_obs.comment = "apogeeQL"
                self.mysession.add(new_obs)
                self.mysession.commit()
                return new_obs.pk
             elif observation.count() == 1:
                # return the primary of the entry in the platedb.observation table
                return observation[0].pk
             else: 
                # we found more than one entry
                 raise RuntimeError, ("Found more than one entry in the observation table")

       # create a new entry in the observation tabel without an associated plugging
       self.mysession.begin()
       new_obs=Observation()
       new_obs.mjd=mjd
       new_obs.comment = "apogeeQL: no associated plate"
       self.mysession.add(new_obs)
       self.mysession.commit()
       return new_obs.pk

   def getExposurePk(self, obs_pk, readnum, starttime, exptime):
       """Insert a new row in the platedb.exposure table """

       self.mysession.begin()
       new_exp = Exposure()
       new_exp.observation_pk = obs_pk
       new_exp.exposure_no = readnum
       new_exp.survey_pk = self.apogeeSurveyPk
       new_exp.start_time = starttime
       new_exp.exposure_time = exptime
       new_exp.comment = "apogeeQL"
       # get the corresponding exposure_flavor_pk from the expType
       expType = self.mysession.query(ExposureFlavor).filter(ExposureFlavor.label.match(self.expType))
       if expType.count() >= 1:
          new_exp.exposure_flavor_pk = expType[0].pk
       self.mysession.add(new_exp)
       self.mysession.commit()
       return new_exp.pk

   def getCalibBoxStatus(self):
       """Insert a new row in the platedb.exposure table """

       # get the lamp status from the actor
       lampqrtz = Apogeeql.actor.models['apogeecal'].keyVarDict['calSourceStatus'][0]
       lampune  = Apogeeql.actor.models['apogeecal'].keyVarDict['calSourceStatus'][1]
       lampthar = Apogeeql.actor.models['apogeecal'].keyVarDict['calSourceStatus'][2]
       lampshtr = Apogeeql.actor.models['apogeecal'].keyVarDict['calShutter'][0]
       lampcntl = Apogeeql.actor.models['apogeecal'].keyVarDict['calBoxController'][0]

       return lampqrtz, lampune, lampthar, lampshtr, lampcntl


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def main():
   apogeeql = Apogeeql('apogeeql', 'apogeeql')
   apogeeql.connectQuickLook()
   apogeeql.startQuickLook()
   apogeeql.connectQuickReduce()
   apogeeql.startQuickReduce()
   apogeeql.run()

#-------------------------------------------------------------
if __name__ == '__main__':
   try:
       main()
   except Exception,e:
       traceback.print_exc()


