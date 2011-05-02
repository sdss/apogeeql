#!/usr/bin/env python

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

from platedb.db_connection import Session
from platedb.ModelClasses import *
import platedb.plPlugMapM as plPlugMapM

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
import os, signal, subprocess, tempfile
import types
import yanny

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class QuickLookLineServer(LineReceiver):
    def connectionMade(self):
        # the IDL end-of-line string is linefeed only (no carriage-return) 
        # we need to change the delimiter to be able to detect a full line
        # and have lineReceived method be called (otherwise it just sits there)
        self.delimiter = '\n'
        self.peer = self.transport.getPeer()
        self.factory.qlActor.qlSources.append(self)
        print "Connection from ", self.peer.host, self.peer.port
        # pass the default parameters to the IDL quicklook
        if self.factory.qlActor.ql_pid > 0:
           for s in self.factory.qlActor.qlSources:
              s.sendLine('DATADIR=%s' % (self.factory.qlActor.datadir))


    def lineReceived(self, line):
        if line=='quit':
            # request to disconnect
            self.transport.loseConnection()
        elif line == "callback":
            logging.info("preparing callback")
            reactor.callLater(5.0,self.sendcomment)
        else:
            # assume the messages are properly formatted to pass along
            print 'Received from apql_wrapper.pro: ',line
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
   inst = ''
   startExp = False
   endExp = False
   expState=''
   actor=''
   Session = ''

   def __init__(self, name, productName=None, configFile=None, debugLevel=30):
      actorcore.Actor.Actor.__init__(self, name, productName=productName, configFile=configFile)

      Apogeeql.actor=self
      self.headURL = '$HeadURL$'

      self.logger.setLevel(debugLevel)
      self.logger.propagate = True
      # only the ics_datadir is defined in the cfg file - expect the datadir to be an
      # environment variable set by the apgquicklook setup 
      # (don't want to keep it in 2 different places)
      try:
         self.datadir = os.environ["APQLDATA_DIR"]
      except:
         self.logger.error("Failed: APQLDATA_DIR is not defined")
         traceback.print_exc()

      # MJD value of start of survey (Jan 1 2011) for filenames
      self.startOfSurvey = 55562  
      self.snrAxisRange = [self.config.get('apogeeql','snrAxisMin'), self.config.get('apogeeql','snrAxisMax')]
      self.rootURL = self.config.get('apogeeql','rootURL')

      #
      # Explicitly load other actor models. We usually need these for FITS headers.
      #
      self.models = {}
      for actor in ["mcp", "guider", "platedb", "tcc", "apo", "apogeetest"]:
         self.models[actor] = opscore.actor.model.Model(actor)

      #
      # register the keywords that we want to pay attention to
      #
      self.models["tcc"].keyVarDict["inst"].addCallback(self.TCCInstCB, callNow=False)
      self.models["platedb"].keyVarDict["pointingInfo"].addCallback(self.PointingInfoCB, callNow=False)
      self.models["apogeetest"].keyVarDict["exposureState"].addCallback(self.ExposureStateCB, callNow=False)
      self.models["apogeetest"].keyVarDict["UTRfilename"].addCallback(self.UTRfilenameCB, callNow=False)

      """
      print dir(self.models['platedb'])
      print dir(self.models['platedb'].keyVarDict)
      print self.models['platedb'].keyVarDict.items()
      """

      #
      # Connect to the platedb
      #
      self.Session = Session
      self.mysession = self.Session()

      #
      # Finally start the reactor
      #
      # self.run()

   @staticmethod
   def TCCInstCB(keyVar):
      '''callback routine for tcc.inst'''
      print "TCCInstCB=",keyVar
      Apogeeql.inst = keyVar[0]

   @staticmethod
   def PointingInfoCB(keyVar):
      '''callback routine for platedb.pointingInfo'''
      # plate_id, cartridge_id, pointing_id, boresight_ra, boresight_dec, hour_angle, temperature, wavelength
      # print "PointingInfoCB=",keyVar
      plate = keyVar[0]
      cartridge = keyVar[1]
      pointing = keyVar[2]
      # check that APOGEE (or MARVELS) is the current instrument - otherwise ignore new platedb info
      # if Apogeeql.inst not in ['APOGEE','MARVELS']:
      #   return

      #print Apogeeql.actor.models['platedb'].keyVarDict['activePlugging']
      if plate == None:
         return

      if plate != Apogeeql.prevPlate or cartridge != Apogeeql.prevCartridge or pointing != Apogeeql.prevPointing:
         # we need to extract and pass a new plugmap to IDL QuickLook
         pm = Apogeeql.actor.getPlPlugMapM(Apogeeql.actor.mysession, cartridge, plate, pointing)

         # routine returns a yanny par file
         # apg_yanny = Apogeeql.actor.makeApogeePlugMap(pm.file)

         # open a temporary file to save the blob from the database
         # f=tempfile.NamedTemporaryFile(delete=False,dir='/tmp',prefix=os.path.splitext(pm.filename)[0]+'.')
         # the same file is used over and over again 
         fname = '/tmp/apoge_latest_plate.par'
         f=open(fname,'w+')
         f.write(pm.file)
         f.close()

         # pass the info to IDL QL
         for s in Apogeeql.qlSources:
            s.sendLine('plugMapInfo=%s,%s,%s,%s' % (plate, pm.fscan_mjd, pm.fscan_id, fname))

         # print 'plugMapFilename=%s' % (fname)
         Apogeeql.prevPointing = pointing
         Apogeeql.prevPlate = plate
         Apogeeql.prevCartridge = cartridge
         Apogeeql.prevScanId = pm.fscan_id
         Apogeeql.prevScanMJD = pm.fscan_mjd

         # we need to query the database and get all the previous apogee exposures with this plate
         # to populate the table on the right of the STUI quicklook window

   @staticmethod
   def ExposureStateCB(keyVar):
      '''callback routine for apogeeICC.exposureState '''
      # exposureState state time time_left UTR_counter
      # exposureState INTEGRATING 600.0 600.0 0    -> mark start of exposure
      # exposureState UTR 0.0 590.0 1    -> mark first UTR read
      # ...
      # exposureState UTR 0.0 0.0 12   -> mark 12th UTR read
      # exposureState DONE | ABORTED 0.0 0.0 0   -> mark end of UTR exposure
      Apogeeql.expState=keyVar[0]
      if keyVar[0] == 'INTEGRATING':
         Apogeeql.startExp = True
         Apogeeql.endExp = False
         # we may have to do something special for QL at the start of a new exposure
      elif keyVar[0] in ['DONE', 'ABORTED']:
         Apogeeql.startExp = False
         Apogeeql.endExp = True
         # do something for the quickreduction at the end of an exposure
         for s in Apogeeql.qlSources:
            s.sendLine('UTR=DONE')
      else:
         Apogeeql.startExp = False

   @staticmethod
   def UTRfilenameCB(keyVar):
      '''callback routine for apogeeICC.exposureState '''
      # UTRfilename="apRaw-12345678-060.fits -> mark 60th UTR read
      # exposureState DONE | ABORTED 0.0 0.0 0   -> mark end of UTR exposure
      if Apogeeql.expState != "UTR":
         return

      filename=keyVar[0]
      # create a new FITS file by appending the telescope fits keywords
      newfilename = Apogeeql.appendFitsKeywords(Apogeeql.actor,filename)
      print "newfilename=",newfilename
      for s in Apogeeql.qlSources:
         s.sendLine('UTR=%s' % (newfilename))


   def connectQuickLook(self):
      '''open a socket through htwisted to send/receive information to/from apogee_IDL'''
      # get the port from the configuratio file 
      self.qlPort = self.config.getint('apogeeql', 'qlPort') 
      self.qlHost = self.config.get('apogeeql', 'qlHost') 
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

   def periodicStatus(self):
      '''Run some command periodically'''
      #
      # Obtain and send the data
      #
      self.callCommand('update')
      reactor.callLater(int(self.config.get(self.name, 'updateInterval')), self.periodicStatus)

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
      # the directories are organized by day, as /$root_dir/DDDD/
      #
      res=filename.split('-')
      indir=self.ics_datadir
      outdir = self.datadir
      if len(res) == 3:
         indir  = os.path.join(self.ics_datadir,res[1][:4])
         mjd = int(res[1][:4]) + self.startOfSurvey
         outdir = os.path.join(self.datadir,str(mjd))
      filename = os.path.join(indir, filename)
      outFile = os.path.join(outdir, outFile)
      hdulist = pyfits.open(filename, uint16=True)
      hdulist[0].header.update('TELESCOP' , 'SDSS 2-5m')
      hdulist[0].header.update('FILENAME' ,outFile)

      cards=[]
      cards.extend(actorFits.mcpCards(self.models, cmd=self.bcast))
      cards.extend(actorFits.tccCards(self.models, cmd=self.bcast))
      cards.extend(actorFits.plateCards(self.models, cmd=self.bcast))

      for name, val, comment in cards:
          try:
              hdulist[0].header.update(name, val, comment)
          except:
              self.logger.warn('text="failed to add card: %s=%s (%s)"' % (name, val, comment))

      hdulist.writeto(outFile)
      return outFile


   def getPlPlugMapM(self, session, cartridgeId, plateId, pointingName):
       """Return the plPlugMapM given a plateId and pointingName"""

       from sqlalchemy import and_

       plugging = session.query(Plugging).join(ActivePlugging).join(Cartridge).\
                    order_by(Cartridge.number).all()

       plugging = [p for p in plugging if p.cartridge.number == cartridgeId]

       if len(plugging) == 0:
            raise RuntimeError, ( "No plugging found with cartridgeId = %d" % (cartridgeId)) 
       elif len(plugging) != 1:
            raise RuntimeError, ( "More than one found with cartridgeId = %d" % (cartridgeId)) 
       else:
          plugging=plugging[0]

       """
       print 'plugging=',plugging.plate.plate_id
       print 'plugging.fscan_id=',plugging.fscan_id
       print 'plugging.fscan_mjd=',plugging.fscan_mjd
       """

       pm = session.query(PlPlugMapM).join(Plugging).\
            filter(and_(PlPlugMapM.fscan_id == plugging.fscan_id,
                        PlPlugMapM.fscan_mjd == plugging.fscan_mjd,
                        PlPlugMapM.plugging == plugging))

       if pm.count() != 1:
           if pointingName:
               pm = [p for p in pm if p.pointing_name == pointingName]

           if len(pm) != 1:
               # Look for the correct pointing
               raise RuntimeError, (
                   "Found more than one plugging/plPlugMapM pairing with fscan_mjd, fscan_id = %s, %d; %s" % (
                   plugging.fscan_mjd, plugging.fscan_id, [p.pointing_name for p in pm]))

       return pm[0]

   def makeApogeePlugMap(self, plplugmap_file):
       """Return the plPlugMapM given a plateId and pointingName"""

       from sqlalchemy import and_

       # get the needed information from the plate_hole 
       ph = self.session.query(PlateHole).join(Fiber).order_by(Fiber.fiber_id).\
             filter(and_(Fiber.pl_plugmap_m_pk == pm[0].pk,
                         Fiber.plate_hole_pk == PlateHole.pk))
                         

       # get the needed information from the fiber, plate_hole and catalog_object tables

       

       # append to the standard plPlugMap to add 2mass_style and J, H, Ks mags
       par = yanny.yanny()
       par._contents = pm[0].file
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

       # create the new array to add to the file
       tmass_style=[]
       size=len(p0['PLUGMAPOBJ']['mag'][0])
       # mag = [size][]
       for i in range(p0.size('PLUGMAPOBJ')):
          tmass_style.append('something')

       p0['PLUBMAPOBJ']['tmass_style']=tmass_style


       return p0

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def main():
   apogeeql = Apogeeql('apogeeql', 'apogeeql')
   apogeeql.connectQuickLook()
   apogeeql.startQuickLook()
   apogeeql.run()

#-------------------------------------------------------------
if __name__ == '__main__':
   try:
       main()
   except Exception,e:
       traceback.print_exc()


