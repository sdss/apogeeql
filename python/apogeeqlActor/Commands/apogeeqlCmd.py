#!/usr/bin/env python

import pdb
import logging
import pprint
import re, sys, time
from time import sleep

import opscore.protocols.keys as keys
import opscore.protocols.types as types

from opscore.utility.qstr import qstr
import actorcore.help as help

class apogeeqlCmd(object):

   def __init__(self, actor):
      self.actor = actor

      # Define some typed command arguemetnts
      self.keys = keys.KeysDictionary("apogeeql_apogeeql", (1, 1),
                              keys.Key("actor", types.String(), help="Another actor to command"),
                              keys.Key("cmd", types.String(), help="A command string"),
                              keys.Key("count", types.Int(), help="A count of things to do"))
      #
      # Declare commands
      #
      self.vocab = [
         ('status', '', self.status),
         ('update', '', self.update),
         ('stopidl', '', self.stopIDL),
         ('startidl', '', self.startIDL),
         ('ql', '<cmd>', self.quicklook),
         ('doSomething', '<count>', self.doSomething),
         ('passAlong', 'actor <cmd>', self.passAlong),
      ]

   def ping(self, cmd):
      '''Query the actor for liveness/happiness.'''

      cmd.finish("text='Present and (probably) well'")

   def status(self, cmd):
      '''Report status and version; obtain and send current data'''

      self.actor.sendVersionKey(cmd)
      self.doStatus(cmd, flushCache=True)

   def update(self, cmd):
      '''Report status and version; obtain and send current data'''

      self.doStatus(cmd=cmd)

   def doStatus(self, cmd=None, flushCache=False, doFinish=True):
      '''Report full status'''

      if not cmd:
         cmd = self.actor.bcast

      keyStrings = ['text="nothing to say, really"']
      keyMsg = '; '.join(keyStrings)

      cmd.inform(keyMsg)
      cmd.diag('text="still nothing to say"')
      cmd.finish()

   def startIDL(self, cmd):
      '''Start a new IDL quicklook process'''

      if not cmd:
         cmd = self.actor.bcast

      # the startQuickLook method will kill an existing IDL process before starting a new one
      keyStrings = ['text="starting new IDL process"']
      keyMsg = '; '.join(keyStrings)
      cmd.inform(keyMsg)
      self.actor.startQuickLook()
      cmd.finish()

   def stopIDL(self, cmd):
      '''Stop the currently running IDL quicklook process if it exists'''

      if not cmd:
         cmd = self.actor.bcast

      if self.actor.ql_pid  != 0:
         keyStrings = ['text="stopping process %s"' % self.actor.ql_pid]
         keyMsg = '; '.join(keyStrings)
         cmd.inform(keyMsg)
         self.actor.stopQuickLook()
      else:
         keyStrings = ['text="no process to kill"']
         keyMsg = '; '.join(keyStrings)
         cmd.inform(keyMsg)
      cmd.finish()

   def doSomething(self, cmd):
      """ Do something pointless. """

      cnt = cmd.cmd.keywords["count"].values[0]
      for i in range(cnt):
         cmd.inform('cnt=%d' % (i))
      cmd.finish()

   def quicklook(self, cmd):
      """ command is addressed to the IDL quicklook process"""

      cmdString = cmd.cmd.keywords["cmd"].values[0]
      # pass the command string to the socket to quicklook_main.pro
      # print cmdString
      for s in self.actor.qlSources:
         s.sendLine(cmdString)
      cmd.finish()

   def passAlong(self, cmd):
      """ Pass a command along to another actor. """

      actor = cmd.cmd.keywords["actor"].values[0]
      cmdString = cmd.cmd.keywords["cmd"].values[0]

      cmdVar = self.actor.cmdr.call(actor=actor, cmdStr=cmdString, timeLim=30.0)
      if cmdVar.didFail:
         cmd.fail('text=%s' % (qstr('Failed to pass %s along to %s' % (cmdStr, actor))))
      else:
         cmd.finish()

