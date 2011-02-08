#!/usr/bin/env python

from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

import opscore.actor.model
import opscore.actor.keyvar

import actorcore.Actor
import actorcore.CmdrConnection as actorCmdrConnection

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
   def __init__(self, name, productName=None, configFile=None, debugLevel=30):
      actorcore.Actor.Actor.__init__(self, name, productName=productName, configFile=configFile)

      self.headURL = '$HeadURL$'

      self.logger.setLevel(debugLevel)
      self.logger.propagate = True
      self.ql_pid = 0
      self.qlSources=[]
      self.qr_pid = 0
      self.qrSources=[]

      #
      # Explicitly load other actor models. We usually need these for FITS headers.
      #
      self.models = {}
      for actor in ["mcp", "guider", "platedb", "tcc", "apo"]:
         self.models[actor] = opscore.actor.model.Model(actor)
      #
      # Finally start the reactor
      #
      # self.run()

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


def main():
   apogeeql = Apogeeql('apogeeql', 'apogeeqlActor')
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


