pyhydllp
==========
pyhydllp is a wrapper package that contains many Python functions for extracting data from Hydstra using the hydllp API.
Detailed documentation about hydllp and relevant parameters can be found here: `<http://kisters.com.au/doco/hydllp.htm>`_.
You must have a Hydstra installation and license to run the functions.

Documentation
-------------
The main documentation can be found `here <https://pyhydllp.readthedocs.io>`_.

Installation
-------------
Install via pip::

  pip install pyhydllp

Or conda::

  conda install -c mullenkamp pyhydllp

Requirements
------------
At a minimum to access the base functions, pyhydllp requires a 32bit python installation and the Pandas package.

To access the MSSQL functionality, the `pdsql <https://github.com/mullenkamp/pdsql>`_ package is required::

  conda install -c mullenkamp pdsql
