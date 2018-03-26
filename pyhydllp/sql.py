# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 20:49:35 2018

@author: michaelek
"""
import pandas as pd
import pdsql


def rating_changes(server, database, sites=None, from_mod_date=None, to_mod_date=None):
    """
    Function to determine flow rating changes during a specified period.

    Parameters
    ----------
    server : str
        The SQL server name.
    database : str
        The database name.
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

    rate_hed = pdsql.mssql.rd_sql(server, database, table_hed, fields_hed, where_col, rename_cols=names_hed, from_date=from_mod_date, to_date=to_mod_date, date_col='RELDATE')
    rate_hed['site'] = rate_hed['site'].str.strip()

    where_per = {'STATION': rate_hed['site'].astype(str).unique().tolist()}

    rate_per = pdsql.mssql.rd_sql(server, database, table_per, fields_per, where_per, rename_cols=names_per, where_op='OR')
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


def sql_sites_var(server, database, varto=None, data_source='A'):
    """
    Function to extract all of the sites associated with specific varto codes. Calls to the site data stored in an SQL server.

    Parameters
    ----------
    server : str
        The SQL server name.
    database : str
        The database name.
    varto: list of int or int
        The Hydstra specific variable codes. None equates to all varto's.
    data_source: str
        The Hydstra data source ID. 'A' is archive.

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

    period1 = pdsql.mssql.rd_sql(server, database, period_tab, period_cols, where_col=period_where, rename_cols=period_names)
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


