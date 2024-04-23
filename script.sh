#!/bin/bash

# reorder python import
find NOTAIRFLOW -type f -name "*.py" | xargs reorder-python-imports
