# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 11:30:30 2018

@author: michaelek
"""
from pyhydllp.hydllp import Hydllp


class hyd(object):
    """
    Class to initiate the Hydstra connection and access the extraction functions.

    Parameters
    ----------
    ini_path : str or None
        Path to the Hyaccess.ini file.
    dll_path : str or None
        Path to the hydllp.dll file.
    hydllp_filename : str
        The hydllp file name.
    hyaccess_filename : str
        The hyaccess file name.
    hyconfig_filename : str
        The hyconfig file name.
    username : str
        The login username for Hydstra. Leave a blank str to have Hydstra use the local user machine username.
    password : str
        Same as username, but for password.

    Returns
    -------
    hyd object
    """
    ### Initialisation
    def __init__(self, ini_path, dll_path, hydllp_filename='hydllp.dll', hyaccess_filename='Hyaccess.ini', hyconfig_filename='HYCONFIG.INI', username='', password=''):

        hydllp = Hydllp(ini_path=ini_path, dll_path=dll_path, hydllp_filename=hydllp_filename, hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename, username=username, password=password)
        self.hydllp = hydllp

    ### Load functions
    from pyhydllp.base import get_variable_list, get_ts_blockinfo, get_ts_data, ts_data_changes
    try:
        from pyhydllp.combo import get_ts_data_bulk, sites_var_periods
    except ImportError:
        None



















