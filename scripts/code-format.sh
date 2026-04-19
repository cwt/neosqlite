#!/bin/bash

poetry run black -t py312 -l 80 $(find . -name "*.py")

# Remove trailing whitespace in all .py files
if [[ "$OSTYPE" == "darwin"* ]]; then
    find . -name "*.py" -exec sed -i '' 's/[[:space:]]*$//' {} \;
else
    find . -name "*.py" -exec sed -i 's/[[:space:]]*$//' {} \;
fi
