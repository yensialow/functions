.. Created by sphinx-quickstart on Fri Mar 22 18:25:45 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 2


Utility functions
*****************
Frequently used functions for the Pirates team. Let's collect useful functions here. We can reorganize them into a module when a structure emerges


Documentating your code
=======================
Describe your function

* what it does
* what parameters it requires
* what it returns
* provide usage examples

Use `docstring <https://pythonhosted.org/an_example_pypi_project/sphinx.html#full-code-example>`_ formatting (using Google style here) where possible.
Sphinx allows you to use markdown or .rst to autogenerate documentation for your code.

For example, in your code

.. code-block:: python

   def sum(arg1, arg2):
       """
       Sums 2 integers

       Args:
           arg1 (int): first integer
           arg2 (int): second integer
       Returns:
           int: sum of the 2 integers
       Example:
           >>> sum(1,2)
           3
       """
       return arg1 + arg2

You can access documentation on the git repo markdown page or within your console when you type ``help(sum)``

.. image:: ../sum_doc.png
   :scale: 50 %

Modules (individual .py files)
===============================

clinical
----------
Python functions for working with ICD codes (e.g. extract codes between 2 numbers or matching some regex)

.. automodule:: clinical
   :members:
   :noindex:

sparkagg
----------
Pyspark functions for aggregating pyspark dataframes (e.g. counts or percentages grouped by some column)

.. automodule:: sparkagg
   :members:
   :noindex:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

