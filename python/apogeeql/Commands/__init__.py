#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import click

from clu.parsers.click import CluGroup, help_, ping, version
from clu.command import Command

from apogeeql.tools import wrapBlocking

def _checkDisk(cmd,space,diskName):
    '''Send appropriate keyword message about current space remaining on diskName.'''
    if space > int(cmd.actor.warningDiskSpace):
        cmd.write(message_code="i", message={f'{diskName}DiskAlarm': f'Ok,{int(space)}'})
    elif space > int(cmd.actor.seriousDiskSpace):
        cmd.write(message_code="w", message={f'{diskName}DiskAlarm': f'Warning,{int(space)}'})
    elif space > int(cmd.actor.criticalDiskSpace):
        cmd.write(message_code="e", message={f'{diskName}DiskAlarm': f'Serious,{int(space)}'})
    else:
        cmd.write(message_code="e", message={f'{diskName}DiskAlarm': f'Critical,{int(space)}'})


@click.group(cls=CluGroup)
def parser(*args):
    pass


parser.add_command(ping)
parser.add_command(version)
parser.add_command(help_)

@parser.command()
async def status(command):
    '''Report status and version; obtain and send current data'''
    await Command("version", parent=command).parse()

    actor = command.actor

    command.write(message_code="i", message={"rootURL": actor.rootURL})
    command.write(message_code="i", 
                  message={"snrAxisRange": f"{actor.snrAxisRange[0]},{actor.snrAxisRange[1]}"})

    return command.finish()

@parser.command()
async def update(command):
    '''Report status'''
    await Command("status", parent=command).parse()
    return command.finish()

@parser.command()
async def checkDisks(command):
    '''Report disk space remaining on ICS, QL and Arch.'''

    s = await wrapBlocking(os.statvfs, command.actor.ics_datadir)
    icsSpace = s.f_bsize * s.f_bavail / 1024 / 1024 / 1024  # free space in GB

    s = await wrapBlocking(os.statvfs, command.actor.datadir)
    qlSpace = s.f_bsize * s.f_bavail / 1024 / 1024 / 1024  # free space in GB

    s = await wrapBlocking(os.statvfs, command.actor.archive_dir)
    archSpace = s.f_bsize * s.f_bavail / 1024 / 1024 / 1024  # free space in GB

    command.write(message_code="i", message={'freeDiskSpace': [int(icsSpace), int(qlSpace), int(archSpace)]})

    _checkDisk(command, icsSpace, 'ics')
    _checkDisk(command, qlSpace, 'ql')
    _checkDisk(command, archSpace, 'arch')

    command.finish()

@parser.command()
async def stopidl(command):
    '''Deprecated, Stop the currently running IDL quicklook process'''
    command.write(message_code="e", message={"text": "deprecated"})
    return command.fail()

@parser.command()
async def startidl(command):
    '''Deprecated, Start a new IDL quicklook process'''
    command.write(message_code="e", message={"text": "deprecated"})
    return command.fail()

@parser.command()
async def ql(command):
    '''Deprecated, Send a command to the IDL quicklook process'''
    command.write(message_code="e", message={"text": "deprecated"})
    return command.fail()

@parser.command()
async def doSomething(command):
    '''Deprecated, For testing'''
    command.write(message_code="e", message={"text": "deprecated"})
    return command.fail()

@parser.command()
async def passAlong(command):
    '''Deprecated, Pass a command along to another actor'''
    command.write(message_code="e", message={"text": "deprecated"})
    return command.fail()
