# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 11:30:30 2018

@author: michaelek
"""
from pyhydllp.hydllp import openHyDb, Hydllp

ini_path=r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd'
dll_path=r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\sys\run'
username=''
password=''
hydllp_filename='hydllp.dll'
hyaccess_filename='Hyaccess.ini'
hyconfig_filename='HYCONFIG.INI'


class hyd(object):
    """

    """
    ### Initialisation
    def __init__(self, ini_path, dll_path, hydllp_filename='hydllp.dll', hyaccess_filename='Hyaccess.ini', hyconfig_filename='HYCONFIG.INI', username='', password=''):
        """

        Parameters
        ----------
        ini_path : str or None
            Path to the Hyaccess.ini file.
        dll_path : str or None
            Path to the hydllp.dll file.
        username : str
            The login username for Hydstra. Leave a blank str to have Hydstra use the local user machine username.
        password : str
            Same as username, but for password.
        hydllp_filename : str
            The hydllp file name.
        hyaccess_filename : str
            The hyaccess file name.
        hyconfig_filename : str
            The hyconfig file name.
        """
        hydllp = Hydllp(ini_path=ini_path, dll_path=dll_path, hydllp_filename=hydllp_filename, hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename, username=username, password=password)
        self.hydllp = hydllp














