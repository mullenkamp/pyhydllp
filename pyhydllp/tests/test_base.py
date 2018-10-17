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
#ini_path = r'\\fs02\TestManagedShares\Data\Hydstra\hyd'
#dll_path = r'\\fs02\TestManagedShares\Data\Hydstra\hyd\sys\run'
#ini_path = r'\\fs02\DevManagedShares\Data\Hydstra\hyd'
#dll_path = r'\\fs02\DevManagedShares\Data\Hydstra\hyd\sys\run'
username = ''
password = ''
hydllp_filename = 'hydllp.dll'
hyaccess_filename = 'Hyaccess.ini'
hyconfig_filename = 'HYCONFIG.INI'

sites = [70105, 69607]
varto = 100
from_mod_date = '2018-01-01'
to_mod_date = '2018-07-26'

################################################
### Tests

hyd1 = hyd(ini_path, dll_path, hydllp_filename=hydllp_filename, hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename, username=username, password=password)


def test_ts_data_changes():
    ch1 = hyd1.ts_data_changes(varto=[varto], sites=sites, from_mod_date=from_mod_date, to_mod_date=to_mod_date)
    assert len(ch1) == 2


def test_get_ts_blockinfo():
    b1 = hyd1.get_ts_blockinfo(sites=sites, from_mod_date=from_mod_date, to_mod_date=to_mod_date)
    assert len(b1) >= 300


def test_get_variable_list():
    v1 = hyd1.get_variable_list(sites=sites)
    assert len(v1) >= 4


def test_get_ts_data():
    tsdata = hyd1.get_ts_data(sites=sites, varfrom=100, varto=140, start=from_mod_date, end=to_mod_date)
    assert len(tsdata) >= 400

