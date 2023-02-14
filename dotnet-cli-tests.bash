#!/usr/bin/env bash

# Clean screen
clear

# Ensure pre-step of sample files are being generated
source ./dotnet-cli-prets.bash

# Ensure project can build
source ./dotnet-cli-build.bash

# Change to test projects directory
cd Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests

# Run xUnit test project
dotnet test \
       --configuration Release \
       --logger "console;verbosity=detailed" \
       ./Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests.fsproj

# References
# ==========
# 
# - `dotnet test`
# https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-test
