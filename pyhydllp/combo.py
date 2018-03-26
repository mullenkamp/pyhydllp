# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 13:39:58 2018

@author: michaelek
"""
import pandas as pd
import pdsql
from pyhydllp import sql, hydllp


def get_ts_data_bulk(self, server, database, varto, sites=None, data_source='A', from_date=None, to_date=None, from_mod_date=None, to_mod_date=None, interval='day', qual_codes=[30, 20, 10, 11, 21, 18], concat_data=False, export=None, code_convert=None, qual_code_convert=None):
    """
    Function to read in data from Hydstra's database using HYDLLP. This function extracts all sites with a specific variable code (varto).

    Parameters
    ----------
    server : str
        The SQL server name.
    database : str
        The database name.
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
    sites_var_period = self.sites_var_periods(server=server, database=database, varto=varto, sites=sites, data_source=data_source)
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

        chg1 = self.ts_data_changes(varto_block, sites_block.site.unique(), from_mod_date=from_mod_date, to_mod_date=to_mod_date)
        if not chg1.empty:
            chg1 = chg1.drop('to_date', axis=1)
        if 140 in varto_list:
            sites_flow = sites_var_period[(sites_var_period.varfrom != sites_var_period.varto) & (sites_var_period.varto == 140)]
            chg2 = sql.rating_changes(server=server, database=database, sites=sites_flow.site.unique().tolist(), from_mod_date=from_mod_date, to_mod_date=to_mod_date)
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
    with hydllp.openHyDb(self.hydllp) as h:
        for tup in sites_var_period2.itertuples(index=False):
            print('Processing site: ' + str(tup.site))
            varto = tup.varto
            data_type = device_data_type[varto]

            df = h.get_ts_traces(site_list=[tup.site], data_type=data_type, start=tup.from_date, end=tup.to_date, varfrom=tup.varfrom, varto=varto, interval=interval, qual_codes=qual_codes)
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
                pdsql.mssql.del_mssql_table_rows(**del_rows_dict)
                pdsql.mssql.to_mssql(df, **export)
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


def sites_var_periods(self, server, database, varto=None, sites=None, data_source='A'):
    """
    Function to determine the record periods for Hydstra sites/variables.

    Parameters
    ----------
    server : str
        The SQL server name.
    database : str
        The database name.
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
    sites_var = sql.sql_sites_var(varto=varto, data_source=data_source, server=server, database=database)
    if isinstance(sites, list):
        sites_var = sites_var[sites_var.site.isin([str(i) for i in sites])]
    sites_list = sites_var.site.unique().tolist()
    varto_list = sites_var.varto.unique().astype('int32').tolist()

    ### Determine the period lengths for all sites and variables
    sites_period = self.get_variable_list(sites_list, data_source)
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
