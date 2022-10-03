# apogeeql
The APOGEE QuickLook actor


## start, stop, restart

`systemctl --user start|stop|restart apogeeql`

## basic description

monitors exposures.

starts and controlls a bundling and quicklook thread

When exposures finish, sends commands to bundling thread to bundle exposure. Quicklook runs every 6th read, and at the end.
