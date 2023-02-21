# Pandora.Apache.Avro.IDL.To.Apache.Parquet

## Table of Contents

0. [Background][toc-background]

0. [How to use the library][toc-how-to-use-the-library]

   i. [Package dependencies][toc-package-imports]
   
   i. [Package imports][toc-package-dependencies]
   
   i. [Generating random AVRO data][toc-generating-random-avro-data]
   
   i. [Logger and DataLakeServiceClient][toc-logger-and-datalakeserviceclient]
   
   i. [Loop-logic][toc-loop-logic]
   
   i. [Delta-control files (optional)][toc-delta-control-files-optional]
   
   i. [Main method][toc-main-method]

0. [Project dependencies][toc-project-dependencies]
   
   0. [Library][toc-avroidl2parquet-lib]
   
   0. [Samples][toc-avroidl2parquet-samples]
   
   0. [Unit Tests][toc-avroidl2parquet-unit-tests]

[toc-background]:                       #background

[toc-how-to-use-the-library]:           #how-to-use-the-library
[toc-package-imports]:                  #package-imports
[toc-package-dependencies]:             #package-dependencies
[toc-generating-random-avro-data]:      #generating-random-avro-data
[toc-logger-and-datalakeserviceclient]: #logger-and-datalakeserviceclient
[toc-loop-logic]:                       #loop-logic
[toc-delta-control-files-optional]:     #delta-control-files-optional
[toc-main-method]:                      #main-method

[toc-project-dependencies]:             #project-dependencies
[toc-avroidl2parquet-lib]:              #library
[toc-avroidl2parquet-samples]:          #samples
[toc-avroidl2parquet-unit-tests]:       #unit-tests

[toc-back-to-toc]: #table-of-contents

## Background

Currently, when working with [Apache Kafka®][apache-kafka] and [Azure
Databricks®][azure-databricks] ([Apache Spark®][apache-spark]), there is a
built-in mechanism to transform [Apache Avro®][apache-avro] data to [Apache
Parquet®][apache-parquet] files. The issue with this approach, if we think in
[medallion lakehouse architecture][medallion-lakehouse-architecture], is that
`AVRO` with nested data, will be persisted in a single `PARQUET` file in the
[bronze layer (full, raw and unprocessed history of each
dataset)][medallion-lakehouse-architecture-bronze] relying on `ArrayType`,
`MapType` and `StructType` to represent the nested data. This will make it a bit
more tedious to post-process data respectively in the following layers: [silver
(validated and deduplicated data)][medallion-lakehouse-architecture-silver] and
[gold (data as knowledge)][medallion-lakehouse-architecture-gold].

| ![Medallion lakehouse architecture](docs/pictures/delta-lake-medallion-architecture-2.jpeg) | 
|:--:| 
| Figure 1: [Delta lake medallion architecture and data mesh][medallion-lakehouse-architecture-data-mesh] |

To avoid this issue, we present an **open-source library**, that will help
transform `AVRO`, with nested data, to multiple `PARQUET` files where each of
the nested data elements will be represented as an extension table (separate
file). This will allow to merge both the **bronze** and **silver** layers
(_full, raw and history of each dataset combined with defined structure,
enforced schemas as well validated and deduplicated data_), to make it easier
for data engineers/scientists and business analysts to combine data with already
known logic (`SQL joins`).

| ![Azure Databricks notebook](docs/pictures/databricks-python-notebook-sql-cell.png) | 
|:--:| 
| Figure 2: Azure Databricks `python` notebook and `SQL` cell |

As two of the **medallion layers** are being combined to a single, it might lead
to the possible **saving of a ⅓ in disk usage**. Furthermore, since we aren't
relying on a naive approach, when flattening and storing data, it could further
lead to **greater savings** and a more **sustainable** and **environmentally
friendly** approach.

| ![Green Software Foundation](docs/pictures/5-25-image-green-goftware_sc_3.png) | 
|:--:| 
| Figure 3: [Green Software Foundation][green-software-foundation] with the Linux Foundation to put sustainability at the core of software engineering |

[Back to TOC][toc-back-to-toc]

[apache-kafka]:                               https://kafka.apache.org/
[azure-databricks]:                           https://azure.microsoft.com/en-us/products/databricks/
[apache-spark]:                               https://spark.apache.org/
[apache-spark]:                               https://spark.apache.org/
[apache-avro]:                                https://avro.apache.org/
[apache-parquet]:                             https://parquet.apache.org/
[medallion-lakehouse-architecture]:           https://docs.databricks.com/lakehouse/medallion.html
[medallion-lakehouse-architecture-bronze]:    https://docs.databricks.com/lakehouse/medallion.html#ingest-raw-data-to-the-bronze-layer
[medallion-lakehouse-architecture-silver]:    https://docs.databricks.com/lakehouse/medallion.html#validate-and-deduplicate-data-in-the-silver-layer
[medallion-lakehouse-architecture-gold]:      https://docs.databricks.com/lakehouse/medallion.html#power-analytics-with-the-gold-layer
[medallion-lakehouse-architecture-data-mesh]: https://www.databricks.com/glossary/medallion-architecture
[green-software-foundation]:                  https://blogs.microsoft.com/blog/2021/05/25/accenture-github-microsoft-and-thoughtworks-launch-the-green-software-foundation-with-the-linux-foundation-to-put-sustainability-at-the-core-of-software-engineering/

## How to use the library

In order to show how to use the library to convert `AVRO` nested data to
`PARQUET` files, we will rely on some succinct demo script snippets. The fully
working script is available at: [./demo/avroidl2parquet.fsx][demo-script].

[Back to TOC][toc-back-to-toc]

[demo-script]: ./demo/avroidl2parquet.fsx

### Package dependencies

```fsharp
#r "nuget: Azure.Storage.Files.DataLake,              12.12.01"
#r "nuget: Microsoft.Extensions.Logging,               7.00.00"
#r "nuget: Newtonsoft.Json,                           13.00.02"
#r "nuget: Pandora.Apache.Avro.IDL.To.Apache.Parquet,  0.11.21"

// Specify the local Sample DLL file
#I @"../Pandora.Apache.Avro.IDL.To.Apache.Parquet.Samples/bin/Release/net6.0/"
#r @"Pandora.Apache.Avro.IDL.To.Apache.Parquet.Samples.dll"
```

For this demo script, besides our own package, we will need the following
Microsoft packages:

* **Azure.Storage.Files.DataLake**: To deliver the created `PARQUET` and
  `CONTROL` files to the delta-lake.
* **Microsoft.Extensions.Logging**: Our library needs an instance of an
  `ILogger`.
  
Furthermore, we will also need:
* **Newtonsoft.Json**: This package is needed to parse and pass the `AVRO`
  schema to transform it into a `PARQUET` schema.
  
And finally, we will be using a local `dotnet` project, containing some of the
`AVRO IDL` test samples, taken from [Apache AVRO on GitHub][avro-github].

[Back to TOC][toc-back-to-toc]

[avro-github]: https://github.com/apache/avro/tree/master/lang/java/compiler/src/test/idl

### Package imports

Once we have added the packages to our script, we can then import the following
`namespaces`:

```fsharp
open Microsoft.Extensions.Logging

open Azure.Storage.Files.DataLake
open Azure.Storage.Files.DataLake.Models

open Newtonsoft.Json
open Newtonsoft.Json.Linq

open Pandora.Apache
open Pandora.Databricks
open Pandora.Utils

open org.apache.avro
open org.apache.avro.test
```

[Back to TOC][toc-back-to-toc]

### Generating random AVRO data

In order to generate random `AVRO IDL` data, we will rely on the following
module, which will serialize the `specific` data-types and deserialize into
`generic` types:

```fsharp
[<RequireQualifiedAccess>]
module Test =
  
  open System.Collections.Generic
  
  let private r = new Random()
  
  // local:
  // - org.apache.avro.Interop
  let private interop () =
    let m = new org.apache.avro.MD5 ()
    m.Value <- Array.init 16 (fun _ -> 0x30uy)
    
    let s = new Node ()
    s.label    <- String.Empty
    s.children <- [| |]
    
    let n = new Node ()
    n.label    <- String.Empty
    n.children <- [| s |]
    
    let f = new Foo ()
    f.label <- "label"
    let d = new Dictionary<string,Foo>()
    d.["foo"] <- f
    
    let i = new Interop ()
    
    i.stringField <- String.Empty
    i.nullField   <- null
    i.mapField    <- d
    i.unionField  <-
      [| 42.0                              :> obj
      ;  [| "bytes is a byte sequence"B |] :> obj
      ;  true                              :> obj
      |][r.Next(0,3)]
    i.enumField   <- Kind.A
    i.fixedField  <- m
    i.recordField <- n
    
    i
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (i.Schema.ToString())
  
  // local:
  // - org.apache.avro.test.TestRecord
  let private testRecord () =
    let m = new org.apache.avro.test.MD5()
    m.Value <- Array.init 16 (fun _ -> 0x30uy)
    
    let t = new TestRecord ()
    
    t.name         <- "name"
    t.kind         <- Kind.BAZ
    t.status       <- Status.A
    t.hash         <- m
    t.nullableHash <-
      [| null
      ;  m
      |][r.Next(0,2)]
    t.value        <- 42.0
    t.average      <- 42.0f
    t.t            <-
      [| Unchecked.defaultof<TimeSpan>
      ;  TimeSpan.Zero
      |][r.Next(0,2)]
    t.l            <- 42L
    t.a            <- [| "string array" |]
    t.prop         <-
      [| null
      ;  "foobar"
      |][r.Next(0,2)]
    
    t
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (t.Schema.ToString())
  
  
  let private cases =
    [| interop
    ;  testRecord
    |]
  
  let randomEvent () =
    let i =
      r.Next
        ( 0
        , Array.length cases
        )
    cases[i] ()
```

[Back to TOC][toc-back-to-toc]

### Logger and DataLakeServiceClient

As our library require to pass an `ILogger` we can easily create one as:

```fsharp
let logger () =
  let lf = new LoggerFactory ()
  lf.CreateLogger ()
```

For the `DataLakeServiceClient` we can create the following value (`dlsc`),
which can then be used in the rest of the script without having to send it as a
function parameter:

```fsharp
let dlsc =
  "AZURE_DATALAKE_ENV_CONN_STR"
  |> Environment.GetEnvironmentVariable
  |> fun connStr ->
    let opts = new DataLakeClientOptions()
    opts.Retry.NetworkTimeout <- TimeSpan.FromMinutes 15 (* In case network is lost *)
    DataLakeServiceClient
      ( connectionString = connStr
      , options          = opts
      )
```

[Back to TOC][toc-back-to-toc]

### Loop-logic

For the recursive and asynchronous `loop` logic, we will pass the created
logger, a cancellation token and the number of data elements to create of a
given `AVRO IDL` instance.

We will then create a `UTC` date and timestamp as well as its representation as
a date-time offset:

```fsharp
let dts = DateTime.UtcNow
let off = new DateTimeOffset(dts)
```

Next step is to define the values for the `environment`, `AST` and `PARQUET`
tables:

```fsharp
let env = Parquet.Schema.Ast.Environment.empty ()
let ast = Parquet.Schema.Ast.empty ()
      
let tabs = Parquet.Tables.empty log ast
```

We create a sequence of `AVRO IDL` test events:

```fsharp
Seq.init n (
  fun _ ->
    Test.randomEvent ()
)
```

and we then transform them to `PARQUET` tables:

```fsharp
…
|> Seq.iteri (
  fun i gen ->
    let idx = i + 1
    let sha =
      gen
      |> Avro.Bytes.Generic.serialize
      |> Hash.SHA256.Bytes.toBytes
    
    let gn  = gen.Schema.Name
    let gns = gen.Schema.Namespace
    
    let fqdn =
      Parquet.Schema.Ast.Fqdn.FQDN
        (      gn
        , Some gns
        )
          
    let (env', ast', es) =
      if not (ast.ContainsKey fqdn) then
        gen.Schema.ToString()
        |> JToken.Parse
        |> Avro.Schema.toParquetSchema log None env ast
      else
        ( env
        , ast
        , Seq.empty
        )
          
    if Seq.isEmpty es then
      let tabs' = Parquet.Tables.update log (Some tabs) ast'
    
      Parquet.Tables.populate
        log
        off
        (Some sha) None None
        ast'
        gen
        gn (Some gns)
        tabs'
      if 0 = idx % m then
        ( Date.timestamp 0
        , sprintf "%032i" i
        )
        ||> sprintf "%s | net.pandora.avroidl2parquet | VERBOSE | DEMO | Generated data items: %s"
        |> Output.stdout
    else
      Date.timestamp 0
      |> printfn "%s | net.pandora.avroidl2parquet | FAILURE | DEMO | Errors:"
      es
      |> Seq.iter (
        fun e ->
          ( Date.timestamp 0
          , e
          )
          ||> sprintf "%s | net.pandora.avroidl2parquet | FAILURE | DEMO | - %s"
          |> Output.stdout
      )
)
```

> **NOTE**: If a given schema is already in the `AST`, we will skip it, as we
> only parsing once a given `AVRO IDL` schema to a `PARQUET` schema.

Once we have generated the `PARQUET` tables, we will transform them to `bytes`
and then store them on the data lake. For this, we will need to define a file
system client:

```fsharp
let fsc =
  "AZURE_DATALAKE_DELTA_BLOB"
  |> Environment.GetEnvironmentVariable
  |> dlsc.GetFileSystemClient
```

Afterwards, we will iterate over the generated tables, which aren't empty,
generate the bytes and then store then in the Azure Tables Storage:

```fsharp
tabs
|> Seq.filter (
  fun table -> 0 < table.Value.Count
)
|> Seq.map (
  fun table ->
    ( table
    , table.Value
      |> Parquet.Tables.toBytes log dts 
    )
)
|> Seq.iter(
  fun (table, parquet) ->
    let ppath =
      Path.Combine
        ( "AZURE_DATALAKE_DELTA_PATH"
          |> Environment.GetEnvironmentVariable
        , table.Key.Replace(".", "/")
        , dts.ToString("yyyy-MM-dd")
          |> sprintf "pj_pds=%s"
        )
          
    (* Submit PARQUET file to Azure Table Storage with enabled Delta Lake *)
    let isppath =
      new DataLakePathClient
        ( fileSystemClient = fsc
        , path             = ppath
        )
      |> fun dlpc ->
        dlpc.ExistsAsync
          ( cancellationToken = ct
          )
        |> Async.AwaitTask
        |> Async.RunSynchronously
  
    if not isppath.Value then
      fsc.CreateDirectoryAsync
        ( path              = ppath
        , cancellationToken = ct
        )
      |> Async.AwaitTask
      |> Async.RunSynchronously
      |> ignore
          
    let pdc =
      ppath
      |> fsc.GetDirectoryClient
  
    let pfc =
      parquet.Key
      |> pdc.GetFileClient
          
    let _ =
            
      use ms = new MemoryStream(parquet.Value)
            
      pfc.UploadAsync
        ( content           = ms
        , overwrite         = false
        , cancellationToken = ct
        )
      |> Async.AwaitTask
      |> Async.RunSynchronously
      |> ignore
          
    ( Date.timestamp 0
    , parquet.Key
    )
    ||> sprintf "%s | net.pandora.avroidl2parquet | VERBOSE | DEMO | Uploaded to the Azure Data Lake: %s"
    |> Output.stdout
)
```

| ![Parquet folder structure on Azure Table Storage](docs/pictures/demo-datalake.png) | 
|:--:| 
| Figure 4: Parquet folder structure on Azure Table Storage |

[Back to TOC][toc-back-to-toc]

### Delta-control files (optional)

With the code above, we will only add `PARQUET` files to the Azure Table
Storage, but if we want to get the benefits of the delta lake, we will need to
provide a `JSONL` control for each of the uploaded `PARQUET` files. This can be
achieved by modifying the code above like this:

```fsharp
tabs
|> Seq.filter (
  fun table -> 0 < table.Value.Count
)
|> Seq.map (
  fun table ->
    ( table
    , table.Value
      |> Parquet.Tables.toBytes log dts 
    )
)
|> Seq.iter(
  fun (table, parquet) ->
    …
    (* Submit PARQUET file to Azure Table Storage with enabled Delta Lake *)
    …
    ( Date.timestamp 0
    , parquet.Key
    )
    ||> sprintf "%s | net.pandora.avroidl2parquet | VERBOSE | DEMO | Uploaded to the Azure Data Lake: %s"
    |> Output.stdout

    (* Submit CONTROL file to Azure Table Storage `_delta_log` folder *)
    let cpath =
      Path.Combine
        ( "AZURE_DATALAKE_DELTA_PATH"
          |> Environment.GetEnvironmentVariable
        , table.Key.Replace(".", "/")
        , "_delta_log"
        )
          
    let iscpath =
      new DataLakePathClient
        ( fileSystemClient = fsc
        , path             = cpath
        )
      |> fun dlpc ->
        dlpc.ExistsAsync
          ( cancellationToken = ct
          )
        |> Async.AwaitTask
        |> Async.RunSynchronously
  
    if not iscpath.Value then
      fsc.CreateDirectoryAsync
        ( path              = cpath
        , cancellationToken = ct
        )
      |> Async.AwaitTask
      |> Async.RunSynchronously
      |> ignore
          
    control log ct fsc dts parquet schema cpath
)
```

where we ensure that a `_delta_log` folder exists and is populated by our
`control` function which takes: the logger, the cancellation token, the
date-timestamp, the parquet filename & bytes key-value pair, the table schema
and the `_delta_log` folder path.

The first thing we need to do, is to find the next index to be used in the delta
lake. It's mandatory that the naming of the sequence of control files is uniform
with no gaps. Once we have found the next index in the sequence, we will
generate a `JSONL` control file and we will try to upload it. As the Azure Table
Storage relies on [optimistic concurrency][optimistic-concurrency], other
process might have added the next control file in the sequence. Therefore, we
will catch the provided error (`Azure.RequestFailedException` or
`System.AggregateException`) and retry with the next index.

```fsharp
try
  let idx =
    fsc.GetPathsAsync
      ( path              = cpath
      , recursive         = false
      , cancellationToken = ct
      )
    |> fun ps ->
      ps.GetAsyncEnumerator()
      |> Seq.unfold(
        fun it ->
          let next =
            it.MoveNextAsync().AsTask()
            |> Async.AwaitTask
            |> Async.RunSynchronously
          if next then
            let name = it.Current.Name
            let json =
              name
              |> Path.GetExtension
              |> ((=) ".json")
            if not json then
              ( -1
              , it
              )
              |> Some
            else
              ( name
                |> Path.GetFileNameWithoutExtension
                |> int
              , it
              )
              |> Some
          else
            None
      )
      |> Seq.fold max (-1)
      |> ((+) 1)
    
  let jsonl =
    DeltaLake.JSONL.init
      ( log )
      ( dts )
      ( parquet.Value.LongLength )
      ( schema.GetDataFields()
        |> DeltaLake.JSONL.Schema.init log
      )
      ( parquet.Key )
      |> DeltaLake.toBytes log idx cpath
    
  let jdc =
    cpath
    |> fsc.GetDirectoryClient
    
  let jfc =
    jsonl.Key
    |> Path.GetFileName
    |> jdc.GetFileClient
    
  let _ =
            
    use ms = new MemoryStream(jsonl.Value)
            
    jfc.UploadAsync
      ( content           = ms
      , overwrite         = false
      , cancellationToken = ct
      )
    |> Async.AwaitTask
    |> Async.RunSynchronously
    |> ignore
    
  ( Date.timestamp 0
  , jsonl.Key
    |> Path.GetFileName
  )
  ||> sprintf "%s | net.pandora.avroidl2parquet | VERBOSE | DEMO | Uploaded to the Azure Data Lake: %s"
  |> Output.stdout
with
  | :? System.AggregateException
  | :? Azure.RequestFailedException ->
    ( Date.timestamp 0
    , parquet.Key
    )
    ||> sprintf "%s | net.pandora.avroidl2parquet | WARNING | DEMO | Upload retrying Azure Data Lake: %s"
    |> Output.stdout
    control log ct fsc dts parquet schema cpath
  | ex ->
    ( Date.timestamp 0
    , ex
    )
    ||> sprintf "%s | net.pandora.avroidl2parquet | FAILURE | DEMO | Unexpected error:\n%A"
    |> failwith
```

| ![JSONL control files in _delta_log folder on Azure Table Storage](docs/pictures/demo-deltalake.png) | 
|:--:| 
| Figure 5: JSONL control files in _delta_log folder on Azure Table Storage |

[Back to TOC][toc-back-to-toc]

[optimistic-concurrency]: https://azure.microsoft.com/en-us/blog/managing-concurrency-in-microsoft-azure-storage-2/

### Main method

We can now bind the loop to a logic function that will help us to shutdown the
script by pressing `ENTER`

```fsharp
let logic log cts amount =
  [ Async.Control.exit cts
  ; loop log cts.Token amount
  ]
  |> Async.Choice

let _ =
  
  Date.timestamp 0
  |> sprintf "%s | net.pandora.avroidl2parquet | STARTED | DEMO"
  |> Output.stdout
  
  try
    
    let sample =
      fsi.CommandLineArgs
      |> Array.skip 1
      |> fun xs ->
        if 0 < Array.length xs then
          xs.[0]
          |> int
        else
          1
    
    let cts = new CancellationTokenSource()
    let log = logger ()
    
    (* Interrupt script by pressing ENTER *)
    Date.timestamp 0
    |> sprintf "%s | net.pandora.avroidl2parquet | VERBOSE | DEMO | Press ENTER to exit"
    |> Output.stdout
    
    logic log cts sample
    |> Async.RunSynchronously
    |> Option.defaultValue ()
    
    Date.timestamp 0
    |> sprintf "%s | net.pandora.avroidl2parquet | STOPPED | DEMO"
    |> Output.stdout
    
    00
  with ex ->
    ( Date.timestamp 0
    , ex
    )
    ||> sprintf "%s | net.pandora.avroidl2parquet | FAILURE | DEMO | Unexpected error:\n%A"
    |> Output.stdout
    -1
```

As mentioned above, the fully working script is available at:
[./demo/avroidl2parquet.fsx][demo-script].

[Back to TOC][toc-back-to-toc]

[demo-script]: ./demo/avroidl2parquet.fsx

## Project dependencies

[Back to TOC][toc-back-to-toc]

### Library

| Dependency | Author | License |
|---| ---| --- |
| FSharp.Core                                 | Microsoft                      | [MIT License](https://github.com/dotnet/fsharp/blob/main/License.txt)
| Apache.Avro                                 | The Apache Software Foundation | [Apache License 2.0](https://github.com/apache/avro/blob/master/LICENSE.txt)
| Newtonsoft.Json                             | James Newton-King              | [MIT License](https://github.com/JamesNK/Newtonsoft.Json/blob/master/LICENSE.md)
| Parquet.Net                                 | Ivan G                         | [MIT License](https://github.com/aloneguid/parquet-dotnet/blob/master/LICENSE)

[Back to TOC][toc-back-to-toc]

### Samples

| Dependency | Author | License |
|---| ---| --- |
| Apache.Avro                                 | The Apache Software Foundation | [Apache License 2.0](https://github.com/apache/avro/blob/master/LICENSE.txt)

[Back to TOC][toc-back-to-toc]

### Unit Tests

| Dependency | Author | License |
|---| ---| --- |
| Microsoft.NET.Test.Sdk                      | Microsoft         | [MIT License](https://github.com/microsoft/vstest/blob/main/LICENSE)
| coverlet.collector                          | .NET foundation   | [MIT License](https://github.com/coverlet-coverage/coverlet/blob/master/LICENSE)
| xunit                                       | .NET foundation   | [Apache License 2.0](https://github.com/xunit/xunit/blob/main/LICENSE)
| xunit.runner.visualstudio                   | .NET foundation   | [Apache License 2.0](https://github.com/xunit/visualstudio.xunit/blob/main/License.txt)

[Back to TOC][toc-back-to-toc]
