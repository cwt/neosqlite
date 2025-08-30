#!/bin/bash

poetry run black -t py312 -l 80 $(find . -name "*.py")

# Remove trailing whitespace in all .py files
find . -name "*.py" -exec sed -i 's/[[:space:]]*$//' {} \;
