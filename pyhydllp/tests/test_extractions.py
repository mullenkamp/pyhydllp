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
varto1 = 100
varto2 = [100, 140]
from_mod_date = '2018-01-01'
to_mod_date = '2018-03-26'

################################################
### Tests

hyd1 = hyd(ini_path, dll_path, hydllp_filename=hydllp_filename, hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename, username=username, password=password)


def test_ts_data_changes():
    ch1 = hyd1.ts_data_changes(varto=[varto1], sites=sites, from_mod_date=from_mod_date, to_mod_date=to_mod_date)
    assert len(ch1) == 2


def test_sites_var_periods():
    ch2 = hyd1.sites_var_periods(server=server, database=database, varto=varto2, sites=sites)
    assert len(ch2) == 4

def test_get_ts_data_bulk():
    tsdata = hyd1.get_ts_data_bulk(server=server, database=database, varto=varto2, sites=sites, from_mod_date=from_mod_date, to_mod_date=to_mod_date, interval='day', qual_codes=[30, 20, 10, 11, 21, 18], concat_data=True)




    tparam = rd_csv_param[csv].copy()

    ## Read
    h1 = Hydro().rd_csv(os.path.join(py_dir, csv), **tparam)
    h1._base_stats_fun()
    assert (len(h1._base_stats) > 4)

    ## Write
    dformat = tparam['dformat']
    out_param = {}
    if dformat == 'long':
        out_param.update({'pivot': False})
    else:
        out_param.update({'pivot': True})
    h1.to_csv(os.path.join(py_dir, csv), **out_param)

    ## Read
    h1 = Hydro().rd_csv(os.path.join(py_dir, csv), **tparam)
    h1._base_stats_fun()
    assert (len(h1._base_stats) > 4)

































