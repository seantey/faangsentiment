#!/bin/bash
# Make sure the system has Java 8 (May or may not work with Java 9 or above)
# Need to wait until https://github.com/audienceproject/spark-dynamodb supports spark 3.0
# So we use PySpark 2.4.6 for now which uses Scala version 2.11
pip install pyspark==2.4.6
pip install pandas
pip install beautifulsoup4
pip install spark-nlp
pip install torch
pip install transformers
pip install pytz

# https://ipywidgets.readthedocs.io/en/stable/user_install.html
pip install ipywidgets
jupyter nbextension enable --py widgetsnbextension --sys-prefix
