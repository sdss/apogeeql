#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='apogeeql',
      version='2.0.0',
      description='APOGEE quicklook actor',
      author='Stephane Beland, David Nidever',
      author_email='dnidever@montana.edu',
      url='https://github.com/sdss/apogeeql',
      packages=find_packages(exclude=["tests"]),
      scripts=['bin/apogeeql','bin/apogeeql','bin/runQuickLook.py'],
      install_requires=['numpy','astropy(>=4.0)','scipy','apogee_mountain'])
