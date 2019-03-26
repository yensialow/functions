"""
.. module:: sparkagg
    :platform: Ubuntu 14.04.5 LTS Python 2.7.10 Pyspark 2.2.0.cloudera2
    :synopsis: Pyspark functions for aggregating, filtering by regex, arrays

.. moduleauthor:: Yen Low (put name down for initial traceability; we'll eventually reorganize into Pirates)
"""


from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import numpy as np

pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_columns', 100)


def group_pct(tot, x='*', pct=False):
    """
    Return proportions or percentages after a groupby operation

    Args:
        tot (int): denominator for computing proportions, e.g. df.counts()
        x (str): name of column to be counted. Defaults to '*' for row counts
        pct (boolean): flag for proportions (False, default) or percentage (True)
    Returns:
        float: proportion or percentages

    Example:
        >>> df.groupby('group').agg(group_pct(tot, '*').alias('pct')).sort('pct',ascending=False).show()
    """
    if pct:
        factor=100.0
    else:
        factor=1.0
    #Need factor to be float to ensure answer is also float
    #Do not change order of operations to output float answers
    return factor*count(x)/tot


def array_rlike(df, arrayColname, selDistinctCol, regexmask):
    """
    if array column matches regex, return pyspark dataframe with corresponding selDistinctCol columns (e.g. instanceId)

    Args:
        df: pyspark dataframe with the array of interest
        arrayColname (str): array name
        selDistinctCol (str): name of columns to return. Performs a 'select distinct' on these columns (e.g. instanceId)
        regexmask: regular expression to match on
    Returns:
        pyspark dataframe with only selDistinctCol columns

    Example:
        >>> array_rlike(df, 'medicalCodes.code', 'instanceId', '^E900').collect()
    """

    #ensure selDistinctCol is a list
    if not isinstance(selDistinctCol, (list,)):
        selDistinctCol = [selDistinctCol]

    arrayParentName=arrayColname.split(".")[0]
    selColumns = selDistinctCol + [arrayParentName]
    return df.select(selColumns).\
            withColumn('exploded',explode(df[arrayColname])).\
            where(col('exploded').rlike(regexmask)).\
            select(selDistinctCol).distinct()


def array_contains_any(array, ilist):
    """
    True/False if array column contains *any* of items in ilist

    Args:
        array: pyspark array or nested element
        ilist: list with at least one value (either all str or all numeric) that array must match on. If single element, must be in a list, e.g. ['match_pattern'] or [1]
    Returns:
        boolean: True/False pyspark indices
    Example:
        >>> df.where(array_contains_any(df.medicalCodes.code, ["E900.0","V67.59"]))
    """
    list_of_filtered_rows = [array_contains(array, x) for x in ilist] #pyspark column pointers
    #items of list are concatenated by | (i.e. OR) operator
    return reduce(lambda i, j: i | j, list_of_filtered_rows)


def array_contains_all(array, ilist):
    """
    True/False if array column contains *all* the items in ilist

    Args:
        array: pyspark array or nested element
        ilist: list of values (either all str or all numeric) that array must match all on. If single element, must be in a list, e.g. ['match_pattern'] or [1]
    Returns:
        boolean: True/False pyspark indices
    Example:
        >>> df.where(array_contains_all(df.medicalCodes.code, ["E900.0","V67.59"]))
    """
    list_of_filtered_rows = [array_contains(array, x) for x in ilist] #pyspark column pointers
    #items of list are concatenated by | (i.e. OR) operator
    return reduce(lambda i, j: i & j, list_of_filtered_rows)


def countDistinctWhen(colCondition, conditionvalue=1, elsevalue=None, colDistinct='userId'):
    """
    Equivalent of count(distinct case when colCondition==conditionvalue then colDistinct else elsevalue end)

    Args:
        colCondition (str): name of column which condition is applied, e.g. 'group'
        conditionvalue (float, int or str): Value that colCondition must be equal to, e.g. 1 for group==1
        elsevalue (float, int, str or None): Value to return when colCondition!=conditionvalue (default: None)
        colDistinct (str): name of column with value returned when condition is True (e.g. 'id')
    Returns:
        int: count of distinct values in colDistinct column when colCondition==conditionvalue
    Example:
        >>> df.agg(countDistinctWhen('engage_flag',colDistinct='userId')).show()
    """
    return countDistinct(when(col(colCondition)==conditionvalue, col(colDistinct)).otherwise(elsevalue))


def cnt2pct(df,colSelected=None,denominator=None,pct=True):
    """
    Converts pyspark dataframe of counts to percentages

    Args:
        df: input dataframe where each cell is a count
        colSelected: (list of) column names of dataframe where counts will be converted to percentages
        denominator (int or None): denominator over which counts will be divided
        pct (boolean): flag for proportions (False) or percentage (True). Defaults to True
    Returns:
        pyspark dataframe of proportion or percentages
    Example:
        >>> cnt2pct(df, ['count1','count2'])).show()
    """
    if pct is True:
        factor=100.0
    else:
        factor=1.0
    if colSelected is None:
        colSelected=df.columns
    if denominator is None:
        denominator=df.count()
    #TODO: or should denominator be sum of all counts?
    return df.select(colSelected).\
              select(*((col(c)*factor/denominator).alias(c) for c in colSelected))

