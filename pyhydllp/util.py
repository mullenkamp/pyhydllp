# -*- coding: utf-8 -*-
"""
Utility functions for Hilltop functions.
"""
import os
import pandas as pd
import numpy as np
from re import search, IGNORECASE, findall


def select_sites(x):
    """
    Function to check for different object types and create an array of values.
    """

    if isinstance(x, np.ndarray):
        x1 = x.copy()
    elif isinstance(x, (list, tuple)):
        x1 = np.array(x).copy()
    elif isinstance(x, (pd.Series, pd.Index)):
        x1 = x.values.copy()
    elif isinstance(x, pd.DataFrame):
        x1 = x.iloc[:, 0].values.copy()
    elif isinstance(x, str):
        x1 = pd.read_csv(x).iloc[:, 0].values.copy()
    elif x is None:
        x1 = x
    else:
        raise TypeError("I'm sure you can find some valid type to pass")

    return x1


def rd_dir(data_dir, ext, file_num_names=False, ignore_case=True):
    """
    Function to read a directory of files and create a list of files associated with a spcific file extension. Can also create a list of file numbers from within the file list (e.g. if each file is a station number.)
    """

    if ignore_case:
        files = np.array([filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename, IGNORECASE)])
    else:
        files = np.array([filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename)])

    if file_num_names:
        site_names = np.array([int(findall("\d+", fi)[0]) for fi in files])
        return files, site_names
    else:
        return files


def save_df(df, path_str, index=True, header=True):
    """
    Function to save a dataframe based on the path_str extension. The path_str must  either end in csv or h5.

    df -- Pandas DataFrame.\n
    path_str -- File path (str).\n
    index -- Should the row index be saved? Only necessary for csv.
    """

    path1 = os.path.splitext(os.path_str)

    if path1[1] in '.h5':
        df.to_hdf(path_str, 'df', mode='w')
    if path1[1] in '.csv':
        df.to_csv(path_str, index=index, header=header)