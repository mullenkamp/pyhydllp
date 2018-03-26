# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 13:43:17 2018

@author: michaelek
"""
from pyhydllp import hyd


#################################################
### Parameters

ini_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd'
dll_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\sys\run'
username = ''
password = ''
hydllp_filename = 'hydllp.dll'
hyaccess_filename = 'Hyaccess.ini'
hyconfig_filename = 'HYCONFIG.INI'
server = 'SQL2012PROD03'
database = 'Hydstra'

sites = [70105, 69607]
varto = [100, 140]
from_mod_date = '2018-01-01'
to_mod_date = '2018-03-26'

################################################
### Tests

hyd1 = hyd(ini_path, dll_path, hydllp_filename=hydllp_filename, hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename, username=username, password=password)


def test_sites_var_periods():
    ch2 = hyd1.sites_var_periods(server=server, database=database, varto=varto, sites=sites)
    assert len(ch2) == 4


def test_get_ts_data_bulk():
    tsdata = hyd1.get_ts_data_bulk(server=server, database=database, varto=varto, sites=sites, from_mod_date=from_mod_date, to_mod_date=to_mod_date, interval='day', qual_codes=[30, 20, 10, 11, 21, 18], concat_data=True)
    assert (len(tsdata) == 1291) & (len(tsdata.columns) == 3)

