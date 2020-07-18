#!/bin/bash

## Create the Virtual Environment ##

python -m venv faangs_venv

# Activate the environment
source faangs_venv/bin/activate

# Add the environment as a Jupyter Notebook Kernel
pip install ipykernel

python -m ipykernel install --user --name=faangs_venv

# To remove from Jupyter Kernel Spec
# jupyter kernelspec uninstall faangs_venv