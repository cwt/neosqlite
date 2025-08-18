#!/usr/bin/env python3
"""
Script to regenerate Sphinx .rst files and build HTML documentation.

This script uses sphinx-apidoc to synchronize .rst files with the current
structure of the neosqlite package. It also checks for orphaned .rst files
that correspond to modules no longer present in the codebase and warns about them.
Finally, it triggers the Sphinx build process.
"""

import os
import subprocess
import sys
from pathlib import Path

def run_command(command, cwd=None, description=""):
    """Run a shell command and handle errors."""
    print(f"Running: {description or command}")
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True  # Use shell to handle complex commands
        )
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}\n")
        print(f"Stdout:\n{e.stdout}")
        print(f"Stderr:\n{e.stderr}")
        return False

def find_orphaned_rst_files(source_dir: Path, package_dir: Path):
    """
    Find .rst files that might correspond to modules no longer in the package.

    This looks for .rst files that are not 'index.rst' or 'modules.rst' and
    tries to match them to a corresponding .py file in the package.
    """
    orphaned = []
    
    # Get all .py files in the package (relative paths)
    py_files = set()
    package_name = package_dir.name  # e.g., "neosqlite"
    for py_file in package_dir.rglob("*.py"):
        rel_path = py_file.relative_to(package_dir)
        # Convert path separators to dots and remove .py extension
        # e.g., subpkg/module.py -> subpkg.module
        if rel_path.name == "__init__.py":
            module_name = str(rel_path.parent).replace(os.sep, ".")
            if module_name == ".":
                module_name = package_name  # Root package
            else:
                module_name = f"{package_name}.{module_name}"
        else:
            module_path = str(rel_path.with_suffix(""))
            module_name = f"{package_name}.{module_path}" if module_path != "__init__" else package_name
            
        py_files.add(module_name)
            
    # Check .rst files in source_dir
    for rst_file in source_dir.glob("*.rst"):
        if rst_file.name in ["index.rst", "modules.rst"]:
            continue
            
        # Derive potential module name from rst file name
        # e.g., neosqlite.subpkg.module.rst -> neosqlite.subpkg.module
        potential_module_name = rst_file.stem
        
        # Check if this .rst file corresponds to a module that no longer exists
        if potential_module_name not in py_files:
            orphaned.append(rst_file)
            
    return orphaned

def main():
    """Main function to orchestrate documentation regeneration."""
    # --- Configuration ---
    project_root = Path(__file__).parent.parent  # /home/cwt/Projects/neosqlite
    docs_dir = project_root / "docs"
    source_dir = docs_dir / "source"
    package_dir = project_root / "neosqlite" # The package to document

    if not docs_dir.exists():
        print(f"Error: Docs directory not found at {docs_dir}")
        sys.exit(1)
    if not package_dir.exists():
        print(f"Error: Package directory not found at {package_dir}")
        sys.exit(1)

    original_cwd = os.getcwd()
    os.chdir(docs_dir) # Change to docs directory for relative path resolution in sphinx-apidoc

    try:
        # --- Step 1: Find Orphaned .rst Files (Before regeneration) ---
        print("Checking for orphaned .rst files...")
        orphaned_rst_files = find_orphaned_rst_files(source_dir, package_dir)
        if orphaned_rst_files:
            print("\nWarning: Found .rst files that may correspond to modules no longer present:")
            for f in orphaned_rst_files:
                print(f"  - {f.relative_to(docs_dir)}")
            print("Please review and consider removing them manually.\n")
        else:
            print("No orphaned .rst files found.\n")

        # --- Step 2: Run sphinx-apidoc ---
        # -f : Force overwriting existing files
        # -e : Put each module on its own page
        # -o source : Output directory for .rst files
        # ../neosqlite : Path to the package (relative to docs_dir)
        # --implicit-namespaces : If using PEP 420 implicit namespaces
        apidoc_cmd = f"sphinx-apidoc -f -e -o {source_dir.relative_to(docs_dir)} {package_dir}"
        if not run_command(apidoc_cmd, cwd=docs_dir, description="Generating .rst files with sphinx-apidoc"):
            print("Failed to generate .rst files.")
            sys.exit(1)

        print("\n.srst files generated/updated successfully.\n")

        # --- Step 3: Build HTML Documentation ---
        build_cmd = "make html"
        if not run_command(build_cmd, cwd=docs_dir, description="Building HTML documentation"):
            print("Failed to build HTML documentation.")
            sys.exit(1)

        print("\nHTML documentation built successfully.")
        print(f"Output is located at: {docs_dir / 'build' / 'html' / 'index.html'}")

    finally:
        os.chdir(original_cwd) # Restore original working directory

if __name__ == "__main__":
    main()