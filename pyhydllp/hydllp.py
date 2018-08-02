"""
Functions to read in Hydstra data. Requires a 32bit python environment due to the hydllp.dll file being 32bit.
"""

import ctypes
import os
import contextlib
import pandas as pd

# Define a context manager generator
# that creates and releases the connection to the hydstra server
@contextlib.contextmanager
def openHyDb(hydllp, username=None, password=None):
    """
    Hydllp content manager generator.

    Parameters
    ----------
    hydllp : Hydllp
        An initialised Hydllp object.
    username : str
        The login username for Hydstra. Leave a blank str to have Hydstra use the local user machine username.
    password : str
        Same as username, but for password.

    Returns
    -------
    Generator
    """
    try:
        hydllp.login(username, password)
        yield hydllp
    finally:
        hydllp.logout()


# Exception for hydstra related errors
class HydstraError(Exception):
    pass


class HydstraErrorUnknown(HydstraError):
    pass


class Hydllp(object):
    def __init__(self, ini_path, dll_path, hydllp_filename, hyaccess_filename, hyconfig_filename, username='', password=''):

        self._dll_path = dll_path
        self._ini_path = ini_path

        self._dll_filename = os.path.join(self._dll_path, hydllp_filename)
        self._hyaccess_filename = os.path.join(self._ini_path, hyaccess_filename)
        self._hyconfig_filename = os.path.join(self._ini_path, hyconfig_filename)

        self._username = username
        self._password = password

        # See Hydstra Help file
        # According to the HYDLLP doc, the hydll.dll needs to run "in situ" since
        # it needs to reference other files in that directory.
        os.chdir(self._dll_path)

        # According to the HYDLLP doc, the stdcall calling convention is used.
        self._dll = ctypes.WinDLL(self._dll_filename)

        # Hydstra server handle. Unique to each instance.
        self._handle = ctypes.c_int()

        self._logged_in = False

        # ********************************************************************************

    # Start - Define HYDLLP Wrappers
    # ********************************************************************************

    def _decode_error(self, error_code):
        """
        HYDLLP.dll "DecodeError" function.

        Parameters
        ----------
        error_code : int
            The error code returned by startup_ex
        """

        # Reference the DecodeError dll function
        decode_error_lib = self._dll['DecodeError']
        decode_error_lib.restype = ctypes.c_int

        # string c_type to store the error message
        error_str = ""
        c_error_str = ctypes.c_char_p(error_str.encode('ascii'))

        # Allocate memory for the return string
        return_str = ctypes.create_string_buffer(b' ', 1400)

        # Call "DecodeError"
        err = decode_error_lib(ctypes.c_int(error_code),
                               c_error_str,
                               ctypes.c_int(1023))
        return return_str.value

    def _start_up_ex(self, user, password, hyaccess, hyconfig):
        """
        HYDLLP.dll "StartUpEx" function

        Parameters
        ----------
        user : str
            Hydstra username

        password : str
            Hydstra password

        hyaccess : str
            Fullpath to HYACCESS.INI

        hyconfig : str
            Fullpath to HYCONFIG.INI
        """

        startUpEx_lib = self._dll['StartUpEx']
        startUpEx_lib.restype = ctypes.c_int

        # Call the dll function "StartUpEx"
        err = startUpEx_lib(ctypes.c_char_p(user),
                            ctypes.c_char_p(password),
                            ctypes.c_char_p(hyaccess),
                            ctypes.c_char_p(hyconfig),
                            ctypes.byref(self._handle))
        return err

    def _shutdown(self):
        """
        HYDLLP.dll "ShutDown" function

        Parameters
        ----------
        None
        """

        shutdown_lib = self._dll['ShutDown']
        shutdown_lib.restype = ctypes.c_int

        error_code = shutdown_lib(self._handle)

        # Values other than 0 means that an error occured
        if error_code != 0:
            error_msg = self._decode_error(error_code)
            raise HydstraError(error_msg)

    def _json_call(self, request_str, return_str_len):
        """
        HYDLLP.dll "JsonCall" function
        """

        jsonCall_lib = self._dll['JSonCall']
        jsonCall_lib.restype = ctypes.c_int

        # Allocate memory for the return string
        return_str = ctypes.create_string_buffer(b' ', return_str_len)

        # c_return_str = ctypes.c_char_p(return_str)

        # Call the dll function "JsonCall"
        err = jsonCall_lib(self._handle,
                           ctypes.c_char_p(request_str.encode('ascii')),
                           return_str,
                           return_str_len)

        result = return_str.value
        return result

        # ********************************************************************************

    # End - Define HYDLLP Wrappers
    # ********************************************************************************

    def login(self, username=None, password=None):
        """
        Logs into hydstra using StartUpEx

        Parameters:
        -----------
        username : str
            Hydstra username

        passwords : str
            Hydstra password

        """
        if not isinstance(username, str):
            username = self._username
        if not isinstance(password, str):
            password = self._password

        error_code = self._start_up_ex(username.encode('ascii'),
                                       password.encode('ascii'),
                                       self._hyaccess_filename.encode('ascii'),
                                       self._hyconfig_filename.encode('ascii'))

        # Values other than 0 means that an error occured
        if error_code != 0:
#            error_msg = self._decode_error(error_code)
            if error_code == 10:
                error_msg = '	StartUp failed. Possible reasons include: SDE7.DLL or SDECDX7.DLL not found, HYCONFIG.INI or HYACCESS.INI not found or incorrect.'
            elif error_code == 12:
                error_msg = '	Login failed. Make sure the user ID and password are correct.'
            raise HydstraError(error_msg)

        self._logged_in = True

    def logout(self):
        """
        Log out of hydstra

        Parameters:
        ----------
        None
        """
        if self._logged_in:
            self._shutdown()

    def query_by_dict(self, request_dict):
        """
        Sends and receives request to the hydstra server using hydllp.dll.
        """
        import json

        # initial buffer length
        # If it is too small, we can resize, see below
        buffer_len = 1400

        # convert request dict to a json string
        request_json = json.dumps(request_dict)

        # call json_call and convert result to python dictionary
        result_json = self._json_call(request_json, buffer_len)
        result_dict = json.loads(result_json)

        # If the initial buffer is too small, then re-call json_call
        # with the actual buffer length given by the error response
        if result_dict["error_num"] == 200:
            buffer_len = result_dict["buff_required"]
            result_json = self._json_call(request_json, buffer_len)
            result_dict = json.loads(result_json)

        # If error_num is not 0, then an error occured
        if result_dict["error_num"] != 0:
            error_msg = "Error num:{}, {}".format(result_dict['error_num'],
                                                  result_dict['error_msg'])
            raise HydstraError(error_msg)

        # Just in case the result doesn't have a 'return'
        elif 'return' not in result_dict:
            error_msg = "Error code = 0, however no 'return' was found"
            raise HydstraError(error_msg)

        return (result_dict)

    def get_site_list(self, site_list_exp):
        # Generate a request of all the sites
        site_list_req_dict = {"function": "get_site_list",
                              "version": 1,
                              "params": {"site_list": site_list_exp}}

        site_list_result = self.query_by_dict(site_list_req_dict)

        return (site_list_result["return"]["sites"])

    def get_variable_list(self, site_list, data_source):

        # Convert the site list to a comma delimited string of sites
        site_list_str = ",".join([str(site) for site in site_list])

        var_list_request = {"function": "get_variable_list",
                            "version": 1,
                            "params": {"site_list": site_list_str,
                                       "datasource": data_source}}

        var_list_result = self.query_by_dict(var_list_request)
        list1 = var_list_result["return"]["sites"]
        df1 = pd.DataFrame()
        for i in list1:
            site = i['site']
            df_temp = pd.DataFrame(i['variables'])
            df_temp['site'] = site
            df1 = pd.concat([df1, df_temp])

        ## Mangling
        df2 = df1.copy().drop('subdesc', axis=1)
        df2['period_end'] = pd.to_datetime(df2['period_end'], format='%Y%m%d%H%M%S')
        df2['period_start'] = pd.to_datetime(df2['period_start'], format='%Y%m%d%H%M%S')
        df2['site'] = df2['site'].str.strip().astype(str)
        df2['variable'] = df2['variable'].astype(float)
        df2 = df2[df2['variable'].isin(df2['variable'].astype('int32'))]
        df2['variable'] = df2['variable'].astype('int32')
        df3 = df2.drop_duplicates().copy()

        df3.rename(columns={'name': 'var_name', 'period_start': 'from_date', 'period_end': 'to_date', 'variable': 'varto'}, inplace=True)
        df3 = df3[['site', 'varto', 'var_name', 'units', 'from_date', 'to_date']].reset_index(drop=True)

        return df3

    def get_subvar_details(self, site_list, variable):

        # Convert the site list to a comma delimited string of sites
        site_list_str = ",".join([str(site) for site in site_list])

        var_list_request = {"function": "get_subvar_details",
                            "version": 1,
                            "params": {"site_list": site_list_str,
                                       "variable": variable}}

        var_list_result = self.query_by_dict(var_list_request)

        return (var_list_result["return"]["sites"])

    def get_sites_by_datasource(self, data_source):

        # Convert the site list to a comma delimited string of sites
#        data_source_str = ",".join([str(i) for i in data_source])

        var_list_request = {"function": "get_sites_by_datasource",
                            "version": 1,
                            "params": {"datasources": data_source}}

        var_list_result = self.query_by_dict(var_list_request)

        return (var_list_result["return"]["datasources"])

    def get_db_areas(self, area_classes_list):

        db_areas_request = {"function": "get_db_areas",
                            "version": 1,
                            "params": {"area_classes": area_classes_list}}

        db_area_result = self.query_by_dict(db_areas_request)

        return (db_area_result["return"]["sites"])

    def get_ts_blockinfo(self, site_list, datasources=['A'], variables=['100', '10', '110', '140', '130', '143', '450'], start='1900-01-01', end='2100-01-01', from_mod_date='1900-01-01', to_mod_date='2100-01-01', fill_gaps=0, auditinfo=0):
        """

        """

        # Convert the site list to a comma delimited string of sites
        if isinstance(site_list, list):
            sites = site_list
        else:
            raise TypeError('site_list must be a list')

        site_list_str = ','.join([str(site) for site in sites])

        ### Datetime conversion
        start = pd.Timestamp(start).strftime('%Y%m%d%H%M%S')
        end = pd.Timestamp(end).strftime('%Y%m%d%H%M%S')
        start_modified = pd.Timestamp(from_mod_date).strftime('%Y%m%d%H%M%S')
        end_modified = pd.Timestamp(to_mod_date).strftime('%Y%m%d%H%M%S')

        ### dict request
        ts_blockinfo_request = {"function": "get_ts_blockinfo",
                                "version": 2,
                                "params": {'site_list': site_list_str,
                                           'datasources': datasources,
                                           'variables': variables,
                                           'starttime': start,
                                           'endtime': end,
                                           'start_modified': start_modified,
                                           'end_modified': end_modified
                                           }}

        ts_blockinfo_result = self.query_by_dict(ts_blockinfo_request)
        blocks = ts_blockinfo_result['return']['blocks']
        df1 = pd.DataFrame(blocks)
        if df1.empty:
            return(df1)
        else:
            df1['endtime'] = pd.to_datetime(df1['endtime'], format='%Y%m%d%H%M%S')
            df1['starttime'] = pd.to_datetime(df1['starttime'], format='%Y%m%d%H%M%S')
            df1['variable'] = pd.to_numeric(df1['variable'], errors='coerce', downcast='integer')
            df2 = df1[['site', 'datasource', 'variable', 'starttime', 'endtime']].sort_values(['site', 'variable', 'starttime'])
            df2.rename(columns={'datasource': 'data_source', 'variable': 'varto', 'starttime': 'from_mod_date', 'endtime': 'to_mod_date'}, inplace=True)

            return df2

    def get_ts_traces(self, site_list, start=0, end=0, varfrom=100, varto=140, interval='day', multiplier=1, datasource='A', data_type='mean', qual_codes=[30, 20, 10, 11, 21, 18], report_time=None):
        """

        """

        # Convert the site list to a comma delimited string of sites
        if isinstance(site_list, list):
            sites = site_list
        else:
            raise TypeError('site_list must be a list')

        site_list_str = ','.join([str(site) for site in sites])

        ### Datetime conversion - with dates < 1900
        c1900 = pd.Timestamp('1900-01-01')
        if start != 0:
            start1 = pd.Timestamp(start)
            if start1 > c1900:
                start = start1.strftime('%Y%m%d%H%M%S')
            else:
                start = start1.isoformat(' ').replace('-', '').replace(' ', '').replace(':', '')
        if end != 0:
            end1 = pd.Timestamp(end)
            if end1 > c1900:
                end = end1.strftime('%Y%m%d%H%M%S')
            else:
                end = end1.isoformat(' ').replace('-', '').replace(' ', '').replace(':', '')

        ts_traces_request = {'function': 'get_ts_traces',
                             'version': 2,
                             'params': {'site_list': site_list_str,
                                        'start_time': start,
                                        'end_time': end,
                                        'varfrom': varfrom,
                                        'varto': varto,
                                        'interval': interval,
                                        'datasource': datasource,
                                        'data_type': data_type,
                                        'multiplier': multiplier,
                                        'report_time': report_time}}

        ts_traces_request = self.query_by_dict(ts_traces_request)
        j1 = ts_traces_request['return']['traces']

        ### Convert json to a dataframe
        sites = [str(f['site']) for f in j1]

        out1 = pd.DataFrame()
        for i in range(len(j1)):
            df1 = pd.DataFrame(j1[i]['trace'])
            if not df1.empty:
                df1.rename(columns={'v': 'data', 't': 'time', 'q': 'qual_code'}, inplace=True)
                df1['data'] = pd.to_numeric(df1['data'], errors='coerce')
                df1['time'] = pd.to_datetime(df1['time'], format='%Y%m%d%H%M%S')
                df1['qual_code'] = pd.to_numeric(df1['qual_code'], errors='coerce', downcast='integer')
                df1['site'] = sites[i]
                df2 = df1[df1.qual_code.isin(qual_codes)]
                out1 = pd.concat([out1, df2])

        out2 = out1.set_index(['site', 'time'])[['data', 'qual_code']]

        return out2

