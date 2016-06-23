# CSV file to Fedora Commons using PCDM lite

This is a python 3 script (which may also run under Python 2) designed to upload a tab separated CSV data file into Fedora 4 to populate a repository. Each row in the spreadsheet represents an Object or a Collection and Objects can reference associated files by path or URL. 


## Status

This is pre-alpha, under documented experimental code being used to test Fedora 4. It may work for you and we're happy to help out if you are interested in getting it running but it is not designed to be production quality. 

##  Install

We very strong recommended that you use a virtual environment for working on this code, as you need to install libaries which are not available via pip.

*  Install Fedora commons 4.

*  Install standard dependencies using pip:

  ```TODO```

*  Install pcdmlite as per the [instruction at its github site](https://github.com/ptsefton/pcdcmlite)

## Run

From the root directory of this repo try this command (note the Fedora endpoint (-u) might be different for you):

  ```python3 csv2f4.py -u http://localhost:8080  sample-data/first-fleet-maps.csv ```


If you have any questions raise an issue here.
