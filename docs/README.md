# NeoSQLite Documentation

This directory contains the documentation for the NeoSQLite project, built using Sphinx.

## Building the Documentation

To regenerate the documentation, run:

```bash
python3 regenerate_docs.py
```

This script will:

1. Generate .rst files for all modules in the neosqlite package using sphinx-apidoc
2. Automatically add the `:private-members:` directive to include private methods in the documentation
3. Ensure the required `_static` directory exists
4. Build the HTML documentation

The generated documentation will be available in `build/html/index.html`.

## How It Works

The documentation system works as follows:

1. **Automatic RST Generation**: The `sphinx-apidoc` tool automatically generates .rst files for all Python modules in the neosqlite package.

2. **Private Members Inclusion**: The script automatically adds the `:private-members:` directive to all generated .rst files to ensure private methods (those starting with underscore) are included in the documentation.

3. **Orphaned File Detection**: The script checks for .rst files that correspond to modules no longer present in the codebase and warns about them.

4. **Static Directory Management**: The script automatically creates the required `_static` directory if it doesn't exist.

5. **HTML Generation**: The script builds the HTML documentation using Sphinx.

## Adding New Modules

When you add new modules to the neosqlite package:

1. Simply run `python3 regenerate_docs.py`
2. The script will automatically detect new modules and generate appropriate .rst files
3. Private methods in the new modules will be automatically included in the documentation

## Removing Modules

When you remove modules from the neosqlite package:

1. Simply run `python3 regenerate_docs.py`
2. The script will detect orphaned .rst files and warn you about them
3. You can manually remove the orphaned .rst files if needed

No modifications to the regeneration script are needed when adding or removing modules - it's fully automatic!