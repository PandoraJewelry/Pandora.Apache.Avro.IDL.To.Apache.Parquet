#!/usr/bin/env bash

# Clean screen
clear

# Change to dir containing the `avrogen` tool
cd ./Pandora.Apache.Avro.IDL.To.Apache.Parquet.Samples/

# Generate C# files from Schema files
for f in $(find ../avro/avsc/ -name "*.avsc")
do
    echo "Generating C# from $f"
    dotnet avrogen -s "$f" ./AvroSchemas/
done

# Change back to root folder when done
cd ..
