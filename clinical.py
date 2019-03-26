"""
.. module:: clinical
    :platform: Ubuntu 14.04.5 LTS Python 2.7.10 Pyspark 2.2.0.cloudera2
    :synopsis: Functions to facilitate working with ICD codes. Looks up ICD9 to ICD 10 table clincodes.icd9_10_desc

.. moduleauthor:: Yen Low (put name down for initial traceability; we'll eventually reorganize into Pirates)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
            .enableHiveSupport()\
            .getOrCreate()

# To load ICD csv to hive table (if needed)
# Table from 2018 CMS GEM
# ref ~/gem/load_icd9_10_desc.py
# https://github.com/AtlasCUMC/ICD10-ICD9-codes-conversion
#
# from pyspark.sql import HiveContext
#
# hdfs:///home/yensia.low/ICD9_10_desc.csv
# df = spark.read.csv('ICD9_10_desc.csv',sep="|",header=True)
# df.show(3,truncate=False)
# df.printSchema()
# df.write.saveAsTable("yen.ICD9_10_desc",mode="overwrite", format="orc")
#
# #Test that csv is loaded into table
# tmp=sqlContext.sql('select icd9 from clincodes.icd9_10_desc where icd9 between 721 and 724')
# tmp.show()

#df_icd=spark.table('clincodes.icd9_10_desc')  #where the ICD table is

def ICDbetween(start,end, regexmask=None, icd=10, verbose=True, nshow=20):
    """
    Extracts all the ICD (9 or 10) codes between start and end.
    Based on static ICD9 to 10 table from CMS GEM in 2018
    https://www.cms.gov/Medicare/Coding/ICD10/2018-ICD-10-CM-and-GEMs.html

    Args:
        start (float, int or str): starting ICD number. Ignored if regex is specified
        end (float, int or str): ending ICD number. Ignored if regex is specified
        regexmask: regular expression to match to
        icd (int): ICD system (9 or 10). Defaults to 10 otherwise
        verbose (boolean): prints ICD table if True (default)
        nshow (int): number of records to show. Defaults to 20
    Returns:
        unique list of ICD codes (str) between start and end

    Examples:
        >>> ICDbetween(3.0,3.09, icd=9, verbose=True, nshow=20)
        +-----+-----+--------------------+
        |icd9 |icd10|description         |
        +-----+-----+--------------------+
        |003.0|A02.0|Salmonella enteritis|
        +-----+-----+--------------------+

        >>> ICDbetween(start=None,end=None,regexmask='^E00[0-2]',icd=9,verbose=False, nshow=20)
        ['E000.0', 'E000.1', 'E000.2', 'E000.8', 'E000.9', 'E001.0', 'E001.1', 'E002.0', 'E002.1', 'E002.2', 'E002.3' 'E002.4', 'E002.5', 'E002.6', 'E002.7', 'E002.8', 'E002.9']
    """
    global df_icd

    #set ICD system
    if icd==9:
        colmn='icd9'
    elif icd==10:
        colmn = 'icd10'
    else:
        print "icd should be 9 or 10. Defaults to 10 otherwise"
        colmn = 'icd10'

    #get selected ICDs
    if regexmask is None:
        selected = df_icd.where(col(colmn).between(start, end))
    else:
        selected = df_icd.where(col(colmn).rlike(regexmask))

    if verbose:
        selected.show(nshow, truncate=False)

    codes=selected.select(colmn).sort(colmn).rdd.flatMap(lambda x: x).collect()

    return [x.encode("utf-8") for x in codes]


def ICDexpand(number, icd=10, verbose=True, nshow=20):
    """
    Get all child ICD codes under given parent code. Calls a modified version of ICDbetween()

    Args:
        number (float or int): parent code
        icd (int): ICD system (9 or 10). Defaults to 10 otherwise
        verbose (boolean): prints ICD table if True (default(
        nshow (int): number of records to show. Defaults to 20
    Returns:
        unique list of ICD codes (str) expanded from input number

    Example:
        >>> ICDexpand(3.0,icd=9)
        +-----+-----+--------------------+
        |icd9 |icd10|description         |
        +-----+-----+--------------------+
        |003.0|A02.0|Salmonella enteritis|
        +-----+-----+--------------------+
        ['003.0']
    """
    global df_icd

    try:
        number_int = int(str(number))
        dp=0
        codes = ICDbetween(number_int, number_int+1-0.01, icd=icd, verbose=verbose, nshow=nshow)
    except:
        dp=len(str(number).split('.')[1])

        if dp==1:
            codes = ICDbetween(number, number+10**(-dp)-0.01, icd=icd, verbose=verbose, nshow=nshow)
        elif dp==2:
            codes = list(str(number).encode("utf-8"))
        else:
            raise ValueError("ICD code must have between 0 and 2 decimal places")
    return codes
