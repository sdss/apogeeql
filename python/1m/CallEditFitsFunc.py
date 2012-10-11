# Edit and move fits
# A module to move and edit the raw FITS files as the become available from the ICS
import numpy as np
import pyfits as pyf
import os
import datetime
import re
def movefits(CurrentFileName,DayNumber,current_obs_ra,current_obs_dec,current_obs_az,current_obs_alt,current_epoch):
    
        NEWCARDS  = ['RA','DEC','EPOCH','AZ','ALT','TELESCOP']
        NEWValues = [current_obs_ra,current_obs_dec,current_epoch,current_obs_az,current_obs_alt,'NMSU 1m']
        timestamp = datetime.datetime.now()
        rawdirec  = '/data-ics/'+DayNumber # Directory for Raw FITS files
        pathraw   = os.path.abspath(rawdirec) # Path to raw files
        MJD5      = int(DayNumber)+55562 # MJD calculted with daynumber
        direc = '/data-ql/data/'+str(MJD5) # Directory where edited FITS will be saved
	if os.path.exists(direc)!=1:
		os.mkdir(direc)
        editdirec = '/data-ql/data/'+str(MJD5)+'/1m/' # Directory where edited FITS will be saved
	t = editdirec
	if os.path.exists(t)!=1:
		os.mkdir(t)
        pathedit  = os.path.abspath(editdirec) # Path for edited FITS
        time      = str(datetime.datetime.now())
        img = pyf.open(pathraw+'/'+CurrentFileName,do_not_scale_image_data=True)
        tbl_header = img[0].header
        strp = re.sub('.fits',"",CurrentFileName) # strip .fits of file name
        new  = strp + '.fits' # add edit.fits to file name
        for i in range(len(NEWCARDS)):
		tbl_header.update(NEWCARDS[i],NEWValues[i],'Taken from 1-meter')
	tbl_header.add_history('FITS file edited'+' '+time)
	img.writeto(pathedit+'/'+new)
        print 'Done editing',CurrentFileName
	
	return
