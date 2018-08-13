pyhydllp
==========
pyhydllp is a package that contains many Python functions for extracting data from Hydstra.

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

Usage
-----
Most functions can be accessed and initialized from the hyd class:

.. code-block:: python

  from pyhydllp import hyd

  ini_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd'
  dll_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\sys\run'
  username = ''
  password = ''
  hydllp_filename = 'hydllp.dll'
  hyaccess_filename = 'Hyaccess.ini'
  hyconfig_filename = 'HYCONFIG.INI'

  hyd1 = hyd(ini_path, dll_path, hydllp_filename=hydllp_filename,
             hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename,
             username=username, password=password)

Then all of the functions can be accessed via the newly initiated hyd1 object.
The following example won't work outside of ECan:

.. code-block:: python

  sites = [70105, 69607]
  varfrom = 100 # the 100 code is water level
  varto = 140 # the 140 code is flow
  from_mod_date = '2018-01-01'
  to_mod_date = '2018-03-26'

  ch1 = hyd1.ts_data_changes(varto=[varfrom], sites=sites, from_mod_date=from_mod_date,
                             to_mod_date=to_mod_date)
  print(ch1)

  tsdata = hyd1.get_ts_data(sites=sites, start=from_mod_date, end=to_mod_date, varfrom=varfrom, varto=varto)

  print(tsdata)
