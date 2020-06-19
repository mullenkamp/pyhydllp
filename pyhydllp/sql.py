# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 20:49:35 2018

@author: michaelek
"""
import pandas as pd
import pdsql


def rating_changes(server, database, sites=None, from_mod_date=None, to_mod_date=None, username=None, password=None):
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
            where_in = {'STATION': sites}
        else:
            where_in = None
    else:
            where_in = None

    rate_hed = pdsql.mssql.rd_sql(server, database, table_hed, fields_hed, where_in, rename_cols=names_hed, from_date=from_mod_date, to_date=to_mod_date, date_col='RELDATE', username=username, password=password)
    if rate_hed.empty:
        return pd.DataFrame()
    else:
        rate_hed['site'] = rate_hed['site'].str.strip()

        where_per = {'STATION': rate_hed['site'].astype(str).unique().tolist()}

        rate_per = pdsql.mssql.rd_sql(server, database, table_per, fields_per, where_per, rename_cols=names_per, where_op='OR', username=username, password=password)
        rate_per['site'] = rate_per['site'].str.strip()
        time1 = pd.to_timedelta(rate_per['stime'].astype(int) // 100, unit='H') + pd.to_timedelta(rate_per['stime'].astype(int) % 100, unit='m')
        rate_per['sdate'] = rate_per['sdate'] + time1
        rate_per = rate_per.sort_values(['site', 'sdate']).reset_index(drop=True).drop('stime', axis=1)

        rate_per1 = pd.merge(rate_per, rate_hed[['site', 'reftab']], on=['site', 'reftab'])
        rate_per2 = rate_per1.groupby('site')['sdate'].min().reset_index()
        rate_per2.columns = ['site', 'from_date']

        rate_per2['varfrom'] = 100
        rate_per2['varto'] = 140

        return rate_per2[['site', 'varfrom', 'varto', 'from_date']]


def sql_sites_var(server, database, varto=None, data_source='A', username=None, password=None):
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
        period_where = {'DATASOURCE': [data_source]}
    elif isinstance(varto, int):
        period_where = {'DATASOURCE': [data_source], 'VARIABLE': [varto]}
    elif isinstance(varto, list):
        period_where = {'DATASOURCE': [data_source], 'VARIABLE': varto}
    else:
        raise TypeError('period_where must be None, int, or list')

    period1 = pdsql.mssql.rd_sql(server, database, period_tab, period_cols, where_in=period_where, rename_cols=period_names, username=username, password=password)
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


def gaugings(server, database, sites=None, mtypes=['wl', 'flow'], from_date=None, to_date=None, from_mod_date=None, to_mod_date=None, stacked=False, username=None, password=None):
    """
    Function to extract gaugings data from Hydstra SQL.

    Parameters
    ----------
    server : str
        The SQL server name.
    database : str
        The database name.
    sites : list
        List of sites to be extracted.
    mtypes : list
        List of generic measurement types to be extracted (see note).
    from_date : str or None
        The start date that should be extracted.
    to_date : str or None
        The end date that should be extracted.
    from_mod_date : str or None
        The start modification date that should be extracted. If not None, has priority over from_date.
    to_mod_date : str or None
        The end modification date that should be extracted. If not None, has priority over to_date.
    stacked : bool
        Should the mtypes and data be stacked or not.

    Returns
    -------
    DataFrame

    Notes
    -----
    The supported mtypes are:
        wl, flow, temp, width, area, velocity, maxdepth, and wettedper
    """
    ### Extract the mtype codes
    mtype_dict = {'wl': 'M_GH', 'flow': 'FLOW', 'area': 'AREA', 'velocity': 'VELOCITY', 'maxdepth': 'MAXDEPTH', 'wettedper': 'WETTEDPER', 'temp': 'TEMP', 'width': 'WIDTH', 'deviation': 'DEVIATION'}

    mtypes_code = [mtype_dict[i] for i in mtypes if i in mtype_dict]

    ### Extract the gaugings
    cols = ['STN', 'MEAS_DATE', 'END_TIME']
    cols.extend(mtypes_code)
    cols.extend(['QUALITY'])
    rename_cols = ['site', 'date', 'time']
    rename_cols.extend(mtypes)
    rename_cols.extend(['qual_code'])
    cols.append('DATEMOD')
    rename_cols.append('mod_date')
#    mtypes.append('mod_date')

    if isinstance(sites, list):
        where_in = {'STN': sites}
    else:
        where_in = None

    if isinstance(from_mod_date, str) | isinstance(to_mod_date, str):
        g1 = pdsql.mssql.rd_sql(server, database, 'GAUGINGS', cols, where_in, from_date=from_mod_date, to_date=to_mod_date, date_col='DATEMOD', rename_cols=rename_cols, username=username, password=password)
    else:
        g1 = pdsql.mssql.rd_sql(server, database, 'GAUGINGS', cols, where_in, from_date=from_date, to_date=to_date, date_col='MEAS_DATE', rename_cols=rename_cols, username=username, password=password)
    g1.site = g1.site.str.strip()
    g1 = g1[g1.site.notnull()].copy()
    g1.loc[~(g1.time >= 100), 'time'] = 1200
    dt1 = pd.to_datetime(g1.date.astype(str) + ' ' + g1.time.astype(int).astype(str), format='%Y-%m-%d %H%M')
    g1.time = dt1
    g2 = g1.drop('date', axis=1)

    if stacked:
#        mtypes.append('qual_code')
        g3 = g2.melt(id_vars=['site', 'time'], value_vars=mtypes, var_name='mtype')
        g3 = pd.merge(g3, g2[['site', 'time', 'qual_code', 'mod_date']], on=['site', 'time']).set_index(['site', 'time', 'mtype'])
    else:
        g3 = g2.set_index(['site', 'time'])
    return g3
