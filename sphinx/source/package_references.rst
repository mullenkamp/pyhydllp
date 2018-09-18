Package References
===================

Base class
--------------

.. autoclass:: pyhydllp.hyd

Methods where SQL is not needed
--------------------------------

.. automethod:: pyhydllp.hyd.get_variable_list

.. automethod:: pyhydllp.hyd.get_ts_blockinfo

.. automethod:: pyhydllp.hyd.ts_data_changes

.. automethod:: pyhydllp.hyd.get_ts_data

Methods requiring both Base class and SQL (pdsql)
-------------------------------------------------

.. automethod:: pyhydllp.hyd.sites_var_periods

Functions only requiring SQL (pdsql)
------------------------------------

.. autofunction:: pyhydllp.sql.rating_changes

.. autofunction:: pyhydllp.sql.sql_sites_var

.. autofunction:: pyhydllp.sql.gaugings

API Pages
---------

.. currentmodule:: pyhydlllp
.. autosummary::
  :template: autosummary.rst
  :toctree: package_references/
