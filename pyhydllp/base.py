# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 13:39:58 2018

@author: michaelek
"""
import os
import pandas as pd
import numpy as np
from datetime import date
from pdsql.mssql import del_mssql_table_rows, to_mssql, rd_sql
from pyhydllp.hydllp import openHyDb
from pyhydllp.util import select_sites, rd_dir, save_df


def rd_hydstra(varto, sites=None, data_source='A', from_date=None, to_date=None, from_mod_date=None, to_mod_date=None, interval='day', qual_codes=[30, 20, 10, 11, 21, 18], concat_data=False, export=None, code_convert=None, qual_code_convert=None):
    """
    Function to read in data from Hydstra's database using HYDLLP. This function extracts all sites with a specific variable code (varto).

    Parameters
    ----------
    varto : int or float
        The hydstra conversion data variable (140.00 is flow).
    sites: list of str
        List of sites to be returned. None includes all sites.
    data_source : str
        Hydstra datasource code (usually 'A').
    from_date: str
        The starting date for the returned data given other constraints.
    to_date: str
        The ending date for the returned data given other constraints.
    from_mod_date: str
        The starting date when the data has been modified.
    to_mod_date: str
        The ending date when the data has been modified.
    interval : str
        The frequency of the output data (year, month, day, hour, minute, second, period). If data_type is 'point', then interval cannot be 'period' (use anything else, it doesn't matter).
    qual_codes : list of int
        The hydstra quality codes for output.
    consat_data : bool
        Shoud the data be concat and returned?
    export: str
        Path string where the data should be saved, or None to not save the data.
    code_convert : dict
        A dict to convert the hydstra mtype codes to other codes.
    qual_code_convert : dict
        A dict to convert the hydstra quality codes to another set of codes.

    Return
    ------
    DataFrame
        In long format with site and time as a MultiIndex and data, qual_code, and hydstra_var_code as columns.
    """
    ### Parameters
    device_data_type = {100: 'mean', 140: 'mean', 143: 'mean', 450: 'mean', 110: 'mean', 130: 'mean', 10: 'tot'}

    ### Determine the period lengths for all sites and variables
    sites_var_period = hydstra_sites_var_periods(varto=varto, sites=sites, data_source=data_source)
#    sites_list = sites_var_period.site.unique().tolist()
    varto_list = sites_var_period.varto.unique().astype('int32').tolist()

    ### Restrict period ranges - optional
    if isinstance(from_date, str):
        from_date1 = pd.Timestamp(from_date)
        sites_var_period = sites_var_period[sites_var_period.to_date > from_date1]
        from_date_df = sites_var_period.from_date.apply(lambda x: x if x > from_date1 else from_date1)
        sites_var_period['from_date'] = from_date_df
    if isinstance(to_date, str):
        to_date1 = pd.Timestamp(to_date)
        sites_var_period = sites_var_period[sites_var_period.from_date > to_date1]
        to_date_df = sites_var_period.to_date.apply(lambda x: x if x > to_date1 else to_date1)
        sites_var_period['to_date'] = to_date_df

    ### Only pull out data according to the modifcation date ranges - optional
    if isinstance(from_mod_date, str):
        sites_block = sites_var_period[sites_var_period.varfrom == sites_var_period.varto]
        varto_block = sites_block.varto.unique().astype('int32').tolist()

        chg1 = hydstra_data_changes(varto_block, sites_block.site.unique(), from_mod_date=from_mod_date, to_mod_date=to_mod_date)
        if not chg1.empty:
            chg1 = chg1.drop('to_date', axis=1)
        if 140 in varto_list:
            sites_flow = sites_var_period[(sites_var_period.varfrom != sites_var_period.varto) & (sites_var_period.varto == 140)]
            chg2 = rating_changes(sites_flow.site.unique().tolist(), from_mod_date=from_mod_date, to_mod_date=to_mod_date)
            chg1 = pd.concat([chg1, chg2])
        if chg1.empty:
            print('No data has been changed since last export')
            return None

        chg1.rename(columns={'from_date': 'mod_date'}, inplace=True)
        chg3 = pd.merge(sites_var_period, chg1, on=['site', 'varfrom', 'varto'])
        chg4 = chg3[chg3.to_date > chg3.mod_date].copy()
        chg4['from_date'] = chg4['mod_date']
        sites_var_period = chg4.drop('mod_date', axis=1).copy()

    ### Convert datetime to date as str
    sites_var_period2 = sites_var_period.copy()
    sites_var_period2['from_date'] = sites_var_period2['from_date'].dt.date.astype(str)
    sites_var_period2['to_date'] = sites_var_period2['to_date'].dt.date.astype(str)

    site_str_len = sites_var_period2.site.str.len().max()

    if isinstance(export, str):
            if export.endswith('.h5'):
                store = pd.HDFStore(export, mode='a')

    data = pd.DataFrame()
    for tup in sites_var_period2.itertuples(index=False):
        print('Processing site: ' + str(tup.site))
        varto = tup.varto
        data_type = device_data_type[varto]

        df = rd_hydstra_db([tup.site], data_type=data_type, start=tup.from_date, end=tup.to_date, varfrom=tup.varfrom, varto=varto, interval=interval, qual_codes=qual_codes)
        if df.empty:
            continue
        df['HydstraCode'] = varto
        site1 = str(tup.site).replace('_', '/')

        ## Convert code 143 to code 140
        if varto == 143:
            df.loc[:, 'data'] = df.loc[:, 'data'] * 0.001
            df['HydstraCode'] = 140

        ## Convert GW well sites to their proper name
        if varto in [110]:
            df.index = df.index.set_levels([site1], 'site')

        ### Make sure the data types are correct
        df.rename(columns={'data': 'Value', 'qual_code': 'QualityCode'}, inplace=True)
        df.index.rename(['Site', 'Time'], inplace=True)
        df.loc[:, 'QualityCode'] = df['QualityCode'].astype('int32')
        df.loc[:, 'HydstraCode'] = df['HydstraCode'].astype('int32')
#        df.loc[:, 'ModDate'] = today1
        code_name = 'HydstraCode'

        ## Convert Hydstra mtype codes to other codes if desired
        if isinstance(code_convert, dict):
            df.replace({'HydstraCode': code_convert}, inplace=True)
            df.rename(columns={'HydstraCode': 'FeatureMtypeSourceID'}, inplace=True)
            code_name = 'FeatureMtypeSourceID'

        ## Convert Hydstra quality codes to NEMS codes
        if isinstance(qual_code_convert, dict):
            df.replace({'QualityCode': qual_code_convert}, inplace=True)

        ### Export options
        if isinstance(export, dict):
            df = df.reset_index()
            from_date1 = str(df.Time.min())
            to_date1 = str(df.Time.max())
            del_rows_dict = {'where_col': {'Site': [site1], code_name: [str(df[code_name][0])]}, 'from_date':from_date1, 'to_date': to_date1, 'date_col': 'Time'}
            del_rows_dict.update(export)
            del_mssql_table_rows(**del_rows_dict)
            to_mssql(df, **export)
        elif isinstance(export, str):
            if export.endswith('.h5'):
                try:
                    store.append(key='var_' + str(varto), value=df, min_itemsize={'site': site_str_len})
                except Exception as err:
                    store.close()
                    raise err
        if concat_data:
            data = pd.concat([data, df])
    if isinstance(export, str):
        store.close()
    if concat_data:
        return data


def rating_changes(sites=None, from_mod_date=None, to_mod_date=None):
    """
    Function to determine flow rating changes during a specified period.

    Parameters
    ----------
    sites: list of str
        List of sites to be returned. None includes all sites.
    from_mod_date: str
        The starting date when the data has been modified.
    to_mod_date: str
        The ending date when the data has been modified.

    Returns
    -------
    DataFrame
        With site, varfrom, varto, and from_date
    """
    ### Parameters
    server= 'SQL2012PROD03'
    database = 'Hydstra'

    table_per = 'RATEPER'
    table_hed = 'RATEHED'
    fields_per = ['STATION', 'VARFROM', 'VARTO', 'SDATE', 'STIME', 'REFTAB', 'PHASE']
    names_per = ['site', 'varfrom', 'varto', 'sdate', 'stime', 'reftab', 'phase']
    fields_hed = ['STATION', 'VARFROM', 'VARTO', 'TABLE', 'RELDATE']
    names_hed = ['site', 'varfrom', 'varto', 'reftab', 'date']

    ### Read data
    if sites is not None:
        if isinstance(sites, list):
            where_col = {'STATION': sites}
        else:
            where_col = None
    else:
            where_col = None

    rate_hed = rd_sql(server, database, table_hed, fields_hed, where_col, rename_cols=names_hed, from_date=from_mod_date, to_date=to_mod_date, date_col='RELDATE')
    rate_hed['site'] = rate_hed['site'].str.strip()

    where_per = {'STATION': rate_hed['site'].astype(str).unique().tolist()}

    rate_per = rd_sql(server, database, table_per, fields_per, where_per, rename_cols=names_per, where_op='OR')
    rate_per['site'] = rate_per['site'].str.strip()
    time1 = pd.to_timedelta(rate_per['stime'] // 100, unit='H') + pd.to_timedelta(rate_per['stime'] % 100, unit='m')
    rate_per['sdate'] = rate_per['sdate'] + time1
    rate_per = rate_per.sort_values(['site', 'sdate']).reset_index(drop=True).drop('stime', axis=1)

    rate_per1 = pd.merge(rate_per, rate_hed[['site', 'reftab']], on=['site', 'reftab'])
    rate_per2 = rate_per1.groupby('site')['sdate'].min().reset_index()
    rate_per2.columns = ['site', 'from_date']

    rate_per2['varfrom'] = 100
    rate_per2['varto'] = 140

    return rate_per2[['site', 'varfrom', 'varto', 'from_date']]


def hydstra_data_changes(varto, sites, data_source='A', from_mod_date=None, to_mod_date=None):
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
        blocklist = rd_blocklist(sites, [data_source], variables=varto, start_modified=from_mod_date1, end_modified=to_mod_date1)
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


def hydstra_sites_var(varto=None, data_source='A', server='SQL2012PROD03', database='Hydstra'):
    """
    Function to extract all of the sites associated with specific varto codes. Calls to the site data stored in an SQL server.

    Parameters
    ----------
    varto: list of int or int
        The Hydstra specific variable codes. None equates to all varto's.
    data_source: str
        The Hydstra data source ID. 'A' is archive.
    server : str
        The SQL server name.
    database : str
        The database name.

    Returns
    -------
    DataFrame
        With site, data_source, varfrom, and varto
    """
    ### Parameters
    period_tab = 'PERIOD'

    period_cols = ['STATION', 'VARFROM', 'VARIABLE']
    period_names = ['site', 'varfrom', 'varto']

    ## Removals
    rem_dict = {'165131': [140, 140], '69302': [140, 140], '71106': [140, 140], '366425': [140, 140]}

    ### Import

    if varto is None:
        period_where = {'DATASOURCE': data_source}
    elif isinstance(varto, int):
        period_where = {'DATASOURCE': data_source, 'VARIABLE': [varto]}
    elif isinstance(varto, list):
        period_where = {'DATASOURCE': data_source, 'VARIABLE': varto}
    else:
        raise TypeError('period_where must be None, int, or list')

    period1 = rd_sql(server, database, period_tab, period_cols, where_col=period_where, rename_cols=period_names)
    period1.loc[:, 'site'] = period1.site.str.strip()

    ### Determine the variables to extract
    period2 = period1[period1.varto.isin(period1.varto.round())].sort_values('site')
    period2 = period2[period2.varto != 101]
    for i in rem_dict:
        period2 = period2[~((period2.site == i) & (period2.varfrom == rem_dict[i][0]) & (period2.varto == rem_dict[i][1]))]

    ### Convert variables to int
    period3 = period2.copy()
    period3['varfrom'] = period3['varfrom'].astype('int32')
    period3['varto'] = period3['varto'].astype('int32')

    ### Return
    return period3


def hydstra_sites_var_periods(varto=None, sites=None, data_source='A', server='SQL2012PROD03', database='Hydstra'):
    """
    Function to determine the record periods for Hydstra sites/variables.

    Parameters
    ----------
    varto : int or float
        The hydstra conversion data variable (140.00 is flow).
    sites: list of str
        List of sites to be returned. None includes all sites.
    data_source : str
        Hydstra datasource code (usually 'A').

    Returns
    -------
    DataFrame
        With site, varfrom, varto, from_date, and to_date.
    """
    sites_var = hydstra_sites_var(varto=varto, data_source=data_source, server=server, database=database)
    if isinstance(sites, list):
        sites_var = sites_var[sites_var.site.isin(sites)]
    sites_list = sites_var.site.unique().tolist()
    varto_list = sites_var.varto.unique().astype('int32').tolist()

    ### Determine the period lengths for all sites and variables
    sites_period = get_site_variable_period(sites_list, data_source)
    sites_period['varfrom'] = sites_period['varto']
    if 140 in varto_list:
        flow_rate_sites = sites_var[(sites_var.varfrom == 100) & (sites_var.varto == 140)]
        wl_sites = sites_period[(sites_period.site.isin(flow_rate_sites.site)) & (sites_period.varto == 100)]
        flow_rate_sites_period = pd.merge(flow_rate_sites, wl_sites[['site', 'from_date', 'to_date']], on='site')

        flow_sites_period = sites_period[sites_period.varto.isin(varto_list)].copy().drop(['var_name', 'units'], axis=1)

        sites_var_period = pd.concat([flow_rate_sites_period, flow_sites_period])
        sites_var_period = sites_var_period[['site', 'varfrom', 'varto', 'from_date', 'to_date']]
    else:
        sites_var_period = sites_period[sites_period.varto.isin(varto_list)].copy().drop(['var_name', 'units'], axis=1)
        sites_var_period = sites_var_period[['site', 'varfrom', 'varto', 'from_date', 'to_date']]

    return sites_var_period.reset_index(drop=True)


def rd_blocklist(sites, datasources=['A'], variables=['100', '10', '110', '140', '130', '143', '450'], start='1900-01-01', end='2100-01-01', start_modified='1900-01-01', end_modified='2100-01-01'):
    """
    Wrapper function to extract info about when data has changed between modification dates.

    Parameters
    ----------
    sites : list, array, one column csv file, or dataframe
        Site numbers.
    datasource : list of str
        Hydstra datasource code (usually ['A']).
    variables : list of int or float
        The hydstra conversion data variable (140.00 is flow).
    start : str
        The start time in the format of '2001-01-01'.
    end : str
        Same formatting as start.
    start_modified: str
        The starting date of the modification.
    end_modified: str
        The ending date of the modification.

    Returns
    -------
    DataFrame
        With site, data_source, varto, from_mod_date, and to_mod_date.
    """
    ### Process sites
    sites1 = select_sites(sites).tolist()

    ### Open connection
    hyd = openHyDb()
    with hyd as h:
        df = h.get_ts_blockinfo(sites1, start=start, end=end, datasources=datasources, variables=variables, start_modified=start_modified, end_modified=end_modified)
    return df


def get_site_variable_period(sites, data_source='A'):
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
    ### Open connection
    hyd = openHyDb()
    with hyd as h:
        df = h.get_variable_list(sites, data_source)
    return df


def rd_hydstra_db(sites, start=0, end=0, datasource='A', data_type='mean', varfrom=100, varto=140, interval='day', multiplier=1, qual_codes=[30, 20, 10, 11, 21, 18], report_time=None, sites_chunk=20, print_sites=False, export_path=None):
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
    sites1 = select_sites(sites)
    n_chunks = np.ceil(len(sites1) / float(sites_chunk))
    sites2 = np.array_split(sites1, n_chunks)

    ### Run instance of hydllp
    data = pd.DataFrame()
    for i in sites2:
        if print_sites:
            print(i)
        ### Open connection
        hyd = openHyDb()
        with hyd as h:
            df = h.get_ts_traces(i, start=start, end=end, datasource=datasource, data_type=data_type, varfrom=varfrom, varto=varto, interval=interval, multiplier=multiplier, qual_codes=qual_codes, report_time=report_time)
        data = pd.concat([data, df])

    if isinstance(export_path, str):
        save_df(data, export_path)

    return data


def hydstra_site_mod_time(sites=None):
    """
    Function to extract modification times from Hydstra data archive files. Returns a DataFrame of sites by modification date. The modification date is in GMT.

    Parameters
    ----------
    sites : list, array, Series, or None
        If sites is not None, then return only the given sites.

    Returns
    -------
    DataFrame
    """

    site_files_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\dat\hyd'
    files1 = rd_dir(site_files_path, 'A')
    file_sites = [os.path.splitext(i)[0] for i in files1]

    if sites is not None:
        sites1 = select_sites(sites).astype(str)
        sites2 = [i.replace('/', '_') for i in sites1]
        file_sites1 = [i for i in file_sites if i in sites2]
    else:
        file_sites1 = file_sites

    mod_times = pd.to_datetime([round(os.path.getmtime(os.path.join(site_files_path, i + '.A'))) for i in file_sites1], unit='s')

    df = pd.DataFrame({'site': file_sites1, 'mod_time': mod_times})
    return df

