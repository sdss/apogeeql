.. _apogeeql:

apogeeql
===============================

The apogeeql actor orchestrates APOGEE data taking at both APO and LCO.
It interacts with the APOGEE instrument software, quicklook, quickred and
the hub to handle the images that are taken.  Once a read is finished the
apogeeql actor "annotates" the FITS header with extra information, copies the
annotated frame to a certain location on disk, and lets the quicklook know
that a read is ready to be checked.  At the beginning of an exposure
the actor also reads in necessary information (plugmap, calibration
data, etc.), creates a temporary plugmap file (plPlugMapA).  

The actual reduction of the images is done by IDL routines referred
to as quicklook and quickred. quicklook processes individual reads,
while quickred processes individual exposures, i.e., after all reads
have been completed. The IDL routines extract spectra, calculate
S/N for each object, and calculate an overall plate S/N at a fiducial
magnitude, using the magnitudes of each object from a plugmap file.

The communication between the Python actor and IDL is accomplished
by linking several IDL processes to the actor when the actor is started.
These bridge processes run the IDL commands apql_wrapper and apqr_wrapper.
These set up some calibration files to be used for the processing.
A few test commands and responses are then sent, and then the
wrappers go into indefinite loops, waiting for commands from the
apogeeql actor.

As the Python actor gets notifications from the hub about exposure and
plate loading activity, it acts on these and, when necessary, passes
information and/or commands to the IDL processes to do the processing.

quicklook is much more complicated than quickred because it returns
information back to the actor to be sent to the hub. In practice, however,
quicklook information has not really been used by the observers in SDSS-IV.

Specifically, notifications are acted upon as follows

  When the actor receives a pointingInfo notification, it
  gets the plugmap information and creates a plPlugMapA file for use
  by the IDL routines. It communicates this information to the IDL
  processes by sending:
   plugMapInfo={plate},{scan_mjd},{scan_id},{filename}
  to quicklook and quickred wrappers, who save the filename to 
  be passed to the reduction routines.

  When the actor receives an exposureState notification with a status
  of DONE, STOPPED, or FAILED, it sends:
     UTR=DONE
  to quicklook, and
     UTR=DONE,{frameid},{mjd5},{exp_pk}
  to the quickred wrapper. The quickred wrapper in turns starts the
  reduction with:
      apquickred,frameid,plugfile=plugfile,rawdir=rawdir,bundledir=bundledir,quickreddir=quickreddir,bpmid=bpmid,psfid=psfid,exp_pk=exp_pk,mjd5=mjd5,/NOWAIT

  When the actor receives an exposureWroteFile notification, if it is
  the first read of an exposure, it creates a database entry and gets a
  primary key for the exposure. With every exposureWroteFile notification, it sends:
         s.sendLine('UTR=%s,%d,%d,%d' % (newfilename, Apogeeql.exp_pk, readnum, Apogeeql.numReadsCommanded))
  to quicklook.

  When the actor receives an exposureWroteSummary notification, it copies
the summary-ICS file to the apogee computer.

  When the actor receives a ditherPosition notification, it sends
         s.sendLine('ditherPosition=%f,%s' % (Apogeeql.ditherPos, Apogeeql.namedDitherPos))
  to quicklook





