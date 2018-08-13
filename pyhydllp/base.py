# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 17:38:18 2018

@author: michaelek
"""
import numpy as np
import pandas as pd
from datetime import date
from pyhydllp import util, hydllp


def get_ts_blockinfo(self, sites, datasources=['A'], variables=['100', '10', '110', '140', '130', '143', '450'], start='1900-01-01', end='2100-01-01', from_mod_date='1900-01-01', to_mod_date='2100-01-01'):
    """
    Wrapper function to extract info about when data has changed between modification dates.

    Parameters
    ----------
    sites : list
        Site numbers.
    datasource : list of str
        Hydstra datasource code (usually ['A']).
    variables : list of int or float
        The hydstra conversion data variable (140.00 is flow).
    start : str
        The start time in the format of '2001-01-01'.
    end : str
        Same formatting as start.
    from_mod_date : str
        The starting date of the modification.
    to_mod_date : str
        The ending date of the modification.

    Returns
    -------
    DataFrame
        With site, data_source, varto, from_mod_date, and to_mod_date.
    """
    ### Process sites
    sites1 = util.select_sites(sites).tolist()

    ### Extract data
    with hydllp.openHyDb(self.hydllp) as h:
        df = h.get_ts_blockinfo(sites1, start=start, end=end, datasources=datasources, variables=variables, from_mod_date=from_mod_date, to_mod_date=to_mod_date)
    return df


def ts_data_changes(self, varto, sites, data_source='A', from_mod_date=None, to_mod_date=None):
    """
    Function to determine the time series data indexed by sites and variables that have changed between the from_mod_date and to_mod_date. For non-flow rating sites/variables!!!

    Parameters
    ----------
    varto : list of str
        The hydstra conversion data variable (140.00 is flow).
    sites: list of str
        List of sites to be returned.
    data_source : str
        Hydstra datasource code (usually 'A').
    from_mod_date: str
        The starting date when the data has been modified.
    to_mod_date: str
        The ending date when the data has been modified.

    Returns
    -------
    DataFrame
        With site, varfrom, varto, from_date, and to_date
    """
    today1 = pd.Timestamp(date.today())

    ### Get changes for all other parameters
    if isinstance(from_mod_date, str):
        from_mod_date1 = pd.Timestamp(from_mod_date)
        if isinstance(to_mod_date, str):
            to_mod_date1 = pd.Timestamp(to_mod_date)
        else:
            to_mod_date1 = today1
        blocklist = self.get_ts_blockinfo(sites, [data_source], variables=varto, from_mod_date=from_mod_date1, to_mod_date=to_mod_date1)
        if blocklist.empty:
            return blocklist
        else:
            block_grp = blocklist.groupby(['site', 'varto'])
            min_date1 = block_grp['from_mod_date'].min()
            max_date1 = block_grp['to_mod_date'].max()
            min_max_date1 = pd.concat([min_date1, max_date1], axis=1)
            min_max_date1.columns = ['from_date', 'to_date']
            min_max_date2 = min_max_date1.reset_index()
            min_max_date2['varfrom'] = min_max_date2['varto']
            min_max_date3 = min_max_date2[['site', 'varfrom', 'varto', 'from_date', 'to_date']]
            return min_max_date3


def get_variable_list(self, sites, data_source='A'):
    """
    Function to get the variables list for a list of sites.

    Parameters
    ----------
    sites : list
        A list of site names.
    data_source : str
        The data source.

    Returns
    -------
    DataFrame
    """
    ### Extract data
    with hydllp.openHyDb(self.hydllp) as h:
        df = h.get_variable_list(sites, data_source)
    return df


def get_ts_data(self, sites, start=0, end=0, datasource='A', data_type='mean', varfrom=100, varto=140, interval='day', multiplier=1, qual_codes=[30, 20, 10, 11, 21, 18], report_time=None, sites_chunk=20, print_sites=False, export_path=None):
    """
    Wrapper function over hydllp to read in data from Hydstra's database. Must be run in a 32bit python. If either start_time or end_time is not 0, then they both need a date.

    Parameters
    ----------
    sites : list, array, one column csv file, or dataframe
        Site numbers.
    start : str or int of 0
        The start time in the format of either '2001-01-01' or 0 (for all data).
    end : str or int of 0
        Same formatting as start.
    datasource : str
        Hydstra datasource code (usually 'A').
    data_type : str
        mean, maxmin, max, min, start, end, first, last, tot, point, partialtot, or cum.
    varfrom : int or float
        The hydstra source data variable (100.00 is water level).
    varto : int or float
        The hydstra conversion data variable (140.00 is flow).
    interval : str
        The frequency of the output data (year, month, day, hour, minute, second, period). If data_type is 'point', then interval cannot be 'period' (use anything else, it doesn't matter).
    multiplier : int
        interval frequency.
    qual_codes : list of int
        The quality codes in Hydstra for filtering the data.
    sites_chunk : int
        Number of sites to request to hydllp at one time. Do not change unless you understand what it does.

    Return
    ------
    DataFrame
        In long format with site and time as a MultiIndex.
    """

    ### Process sites into workable chunks
    sites1 = util.select_sites(sites)
    n_chunks = np.ceil(len(sites1) / float(sites_chunk))
    sites2 = np.array_split(sites1, n_chunks)

    ### Run instance of hydllp
    data = pd.DataFrame()
    with hydllp.openHyDb(self.hydllp) as h:
        for i in sites2:
            if print_sites:
                print(i)
            ### extract data
            df = h.get_ts_traces(site_list=list(i), start=start, end=end, datasource=datasource, data_type=data_type, varfrom=varfrom, varto=varto, interval=interval, multiplier=multiplier, qual_codes=qual_codes, report_time=report_time)
            data = pd.concat([data, df])

    if isinstance(export_path, str):
        util.save_df(data, export_path)

    return data
