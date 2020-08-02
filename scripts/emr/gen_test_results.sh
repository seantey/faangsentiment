#!/bin/bash
# Run using spark-submit might be faster and more efficient than running with Jupyter
# There seems to be serious memory issues when running with Jupyter or as a Python Script
# From visual inspection of memory usage, Jupyter/Python crashes after using all available memory
# on my system at the time which is about 7GB.
# The spark-submit version ran fast and smoothly using only 2GB~3GB max.

spark-submit spark_process_sentiment.py '2020-07-19_Hour=15'
spark-submit spark_process_sentiment.py '2020-07-20_Hour=15'
spark-submit spark_process_sentiment.py '2020-07-21_Hour=15'
spark-submit spark_process_sentiment.py '2020-07-22_Hour=15'
spark-submit spark_process_sentiment.py '2020-07-23_Hour=15'
spark-submit spark_process_sentiment.py '2020-07-24_Hour=15'
spark-submit spark_process_sentiment.py '2020-07-25_Hour=15'
