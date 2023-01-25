#!/bin/sh

# Clean screen
clear

# Ensure project can build
source ./dotnet-cli-build.sh

# Run xUnit test project
dotnet test \
       --configuration Release \
       --logger "console;verbosity=detailed" \
       ./Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests/Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests.fsproj

# References
# ==========
# 
# - `dotnet test`
# https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-test
