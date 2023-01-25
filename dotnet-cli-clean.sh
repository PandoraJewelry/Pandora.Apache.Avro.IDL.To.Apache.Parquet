#!/bin/sh

# Clean screen
clear

# Clean previous builds (if any)
dotnet clean --configuration Debug
dotnet clean --configuration Release

# Remove bin build folders
find . -mindepth 1 -name "bin" -type d -print
find . -mindepth 1 -name "bin" -type d -exec rm -rv "{}" \;

# Remove obj build folders
find . -mindepth 1 -name "obj" -type d -print
find . -mindepth 1 -name "obj" -type d -exec rm -rv "{}" \;

# References
# ==========
# 
# - `dotnet clean`
# https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-clean
