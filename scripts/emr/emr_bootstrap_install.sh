#!/bin/bash

# Python Modules needed for PySpark Scripts
# TODO might be safer to use a requirements.txt file or specify versions
# so that it doesn't break as easily.
sudo pip3 install -U pandas \
 boto3                      \
 beautifulsoup4             \
 spark-nlp                  \
 torch                      \
 transformers               \
 pytz                       \
 ipywidgets

# HuggingFace transformer pipelines requires a cache folder
# When running as the Hadoop user (ssh), the folder can be created just fine
# However, when running as an application "Step" in AWS EMR, it seems
# to throw a permission denied error. It may be the case that the user
# for running the "Steps" is not Hadoop or root.
sudo mkdir /home/.cache
sudo chmod 777 /home/.cache