#!/usr/bin/env python

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

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
import os, signal, subprocess
import types

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
              s.sendLine('IMAGEDIR=%s' % (self.factory.qlActor.imagedir))


    def lineReceived(self, line):
        if line=='quit':
            # request to disconnect
            self.transport.loseConnection()
        elif line == "callback":
            logging.info("preparing callback")
            reactor.callLater(5.0,self.sendcomment)
        else:
            # normal message processing from the apogeeql_IDL
            # self.sendLine(line)
            print 'Received from quicklook_main.pro: ',line

    def connectionLost(self, reason):
        logging.info("Disconnected from %s %s"  % (self.peer.port, reason.value))

    def sendcomment(self):
        logging.info("in the callback routine")

class QuickReduceLineServer(LineReceiver):
    def connectionMade(self):
        self.peer = self.transport.getPeer()
        self.factory.qrActor.qrSources.append(self)
        # print "Connected from", self.peer
        self.transport.write("print,2+2\r\n") 

    def lineReceived(self, line):
        if line==self.end:
            # request to disconnect
            self.transport.loseConnection()
        else:
            # normal message processing from the apogeeql_IDL
            self.sendLine(line)

    def connectionLost(self, reason):
        logging.info("Disconnected from %s %s"  % (self.peer.port, reason.value))

class QLFactory(ClientFactory):
    protocol = QuickLookLineServer
    def __init__(self, qlActor): 
        self.qlActor=qlActor

class QRFactory(ClientFactory):
    protocol = QuickReduceLineServer
    def __init__(self, qrActor): 
        self.qrActor=qrActor


class Apogeeql(actorcore.Actor.Actor):

   models = {}
   ql_pid = 0
   qlSources=[]
   qr_pid = 0
   qrSources=[]
   prevCartridge=-1
   prevPlate=-1
   prevPointing='A'
   inst = ''
   startExp = False
   endExp = False
   expState=''
   actor=''

   def __init__(self, name, productName=None, configFile=None, debugLevel=30):
      actorcore.Actor.Actor.__init__(self, name, productName=productName, configFile=configFile)

      Apogeeql.actor=self
      self.headURL = '$HeadURL$'

      self.logger.setLevel(debugLevel)
      self.logger.propagate = True
      self.imagedir = self.config.get('apogeeql', 'imagedir') 

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
      print "PointingInfoCB=",keyVar
      plate = keyVar[0]
      cartridge = keyVar[1]
      pointing = keyVar[2]
      # check that APOGEE (or MARVELS) is the current instrument - otherwise ignore new platedb info
      if Apogeeql.inst not in ['APOGEE','MARVELS']:
         return

      if Apogeeql.plate != plate or Apogeeql.cartridge != cartridge or Apogeeql.pointing != pointing:
         # we need to extract and pass a new plugmap to IDL QuickLook
         plugInfo = Apogeeql.getPlugMap(cartridge, plate, pointing)
         # pass the info to IDL QL
         for s in Apogeeql.qlSources:
            s.sendLine('plugfile=%s plateid=%s platemjd=%f' % (plugInfo['plugfile'], plugInfo['plateid'], plugInfo['mjd']))
            for id, type, mag in plugInfo['fiberdata']:
               s.sendLine('fiberid=%d objtype=%s mag=%f' % (id, type, mag))

         Apogeeql.pointing = pointing
         Apogeeql.plate = plate
         Apogeeql.cartridge = cartridge

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

   def connectQuickReduce(self):
      '''open a socket through htwisted to send/receive information to/from apogee_IDL'''
      # get the port from the configuratio file 
      self.qrPort = self.config.getint('apogeeql', 'qrPort') 
      self.qrHost = self.config.get('apogeeql', 'qrHost') 
      reactor.listenTCP(self.qrPort, QLFactory(self))

   def startQuickLook(self):
      '''Open a twisted reactor to communicate with IDL socket'''
      #
      # check if an apogeeql_IDL process is already running before starting a new one
      if self.ql_pid > 0:
         self.stopQuickLook()

      # spawn the apogeeql_IDL process and don't wait for its completion
      try:
         # get the string corresponding to the command to start the IDL quicklook process
         qlCommand = self.config.get('apogeeql','qlCommandName')
         qlCommand = qlCommand.strip('"')
         # this adds the arguments to the IDL command line
         qlCommand += " -args %s %s" % (self.qlHost, self.qlPort)
         # print 'qlCommand=',qlCommand.split()
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
         # quicklook_main is the name of the program ran when starting IDL (in apogeeql.cfg)
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
         self.logger.warn("Unable to kill existing apogeeql_IDL process %s" % self.ql_id)
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
      outFile=filename.replace('apRaw','ap')
      if outFile == filename:
         p = filename.find('-')
         if p >= 0:
            outFile = 'ap'+filename[p:]
         else:
            outFile = 'ap'+filename

      outFile = os.path.join(self.imagedir, outFile)
      filename = os.path.join(self.imagedir, filename)
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


def main():
   apogeeql = Apogeeql('apogeeql', 'apogeeql')
   apogeeql.connectQuickLook()
   #apogeeql.connectQuickReduce()
   apogeeql.startQuickLook()
   apogeeql.run()

#-------------------------------------------------------------
if __name__ == '__main__':
   try:
       main()
   except Exception,e:
       traceback.print_exc()


