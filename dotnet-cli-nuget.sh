#!/bin/sh

# Clean screen
clear

# Ensure project is clean (ensure no .nupkg exist)
source ./dotnet-cli-clean.sh

# Ensure project is build (and packed)
source ./dotnet-cli-build.sh

# Push nuget package to nuget.org (ensure ENV VARS are set before)
source ~/.microsoft/usersecrets/pj/env_vars_nuget_org.sh
dotnet nuget push \
       --skip-duplicate \
       --api-key $NUGET_ORG_KEY \
       --source "https://api.nuget.org/v3/index.json" \
       "Pandora.Apache.Avro.IDL.To.Apache.Parquet/bin/Release/Pandora.Apache.Avro.IDL.To.Apache.Parquet.*.nupkg"

# References
# ==========
# 
# - Quickstart: Create and publish a package (dotnet CLI):
# https://docs.microsoft.com/en-us/nuget/quickstart/create-and-publish-a-package-using-the-dotnet-cli
# 
# - `dotnet nuget push`
# https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-nuget-push
