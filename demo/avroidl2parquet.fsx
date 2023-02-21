#!/usr/bin/env -S dotnet fsi --langversion:6.0 --mlcompatibility --optimize --warnaserror+:25,26

(* This construct is for ML compatibility. The syntax '(typ,...,typ) ident'
   is not used in F# code. Consider using 'ident<typ,...,typ>' instead. *)
#nowarn "62"

#r "nuget: Azure.Storage.Files.DataLake,              12.12.01"
#r "nuget: Microsoft.Extensions.Logging,               7.00.00"
#r "nuget: Newtonsoft.Json,                           13.00.02"
#r "nuget: Pandora.Apache.Avro.IDL.To.Apache.Parquet,  0.11.21"

// Specify the local Sample DLL file
#I @"../Pandora.Apache.Avro.IDL.To.Apache.Parquet.Samples/bin/Release/net6.0/"
#r @"Pandora.Apache.Avro.IDL.To.Apache.Parquet.Samples.dll"

#time "on"

open System
open System.IO
open System.Threading

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

[<RequireQualifiedAccess>]
module Input =
  
  open System
  
  let wait : ConsoleKey -> unit =
    fun key ->
      let rec aux _ =
        match System.Console.ReadKey(true).Key with
          | k when k = key ->     ()
          | ______________ -> aux ()
      aux ()

[<RequireQualifiedAccess>]
module Async =
  
  open System
  
  [<RequireQualifiedAccess>]
  module Control =
    
    let exit (cts:CancellationTokenSource) =
      async {
        return
          ConsoleKey.Enter
          |> Input.wait
          |> fun _ -> cts.Cancel()
          |> Some
      }

[<RequireQualifiedAccess>]
module Output =
  
  open System
  
  let stdout : 'a -> unit =
    Console.WriteLine
  
  let stderr : 'a -> unit =
    fun x ->
      Console.Error.WriteLine x
  
  let debug : 'a -> unit =
    Diagnostics.Debug.WriteLine
  
  let trace : 'a -> unit =
    Diagnostics.Trace.WriteLine

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

let logger () =
  let lf = new LoggerFactory ()
  lf.CreateLogger ()

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

let rec loop log ct n =
  async {
    try
      ( Date.timestamp 0
      , n
      )
      ||> sprintf "%s | net.pandora.avroidl2parquet | VERBOSE | DEMO | Generate n: %i"
      |> Output.stdout
      
      let m =
        let p = 100
        if p > n then
          1
        else
          n / p
      
      let dts = DateTime.UtcNow
      let off = new DateTimeOffset(dts)
      
      let env = Parquet.Schema.Ast.Environment.empty ()
      let ast = Parquet.Schema.Ast.empty ()
      
      let tabs = Parquet.Tables.empty log ast
          
      Seq.init n (
        fun _ ->
          Test.randomEvent ()
      )
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
      
      let fsc =
        "AZURE_DATALAKE_DELTA_BLOB"
        |> Environment.GetEnvironmentVariable
        |> dlsc.GetFileSystemClient
      
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
      |> Seq.sortBy (fun (table, _) -> table.Key)
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
          
          control log ct fsc dts parquet table.Value.Schema cpath
      )
      
      do! Async.Sleep 1_000 (* Await a second and redo logic *)
      
      return! loop log ct n
    with ex ->
      return!
        sprintf "Unexpected error: %A" ex
        |> failwith
  }

and control log ct fsc dts parquet schema cpath =
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
