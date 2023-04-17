#!/usr/bin/env -S dotnet fsi --langversion:6.0 --mlcompatibility --optimize --warnaserror+:25,26

(* This construct is for ML compatibility. The syntax '(typ,...,typ) ident'
   is not used in F# code. Consider using 'ident<typ,...,typ>' instead. *)
#nowarn "62"

#r "nuget: Microsoft.Extensions.Logging,              7.00.00"
#r "nuget: Newtonsoft.Json,                          13.00.02"
#r "nuget: Pandora.Apache.Avro.IDL.To.Apache.Parquet, 0.11.28"

#time "on"

open System
open System.IO
open System.Text

open Microsoft.Extensions.Logging

open Newtonsoft.Json.Linq

open Pandora.Apache
open Pandora.Utils

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

let logger () =
  let lf = new LoggerFactory ()
  lf.CreateLogger ()

let isNullable = function
  | Parquet.Schema.Ast.Type.NULL      ->
    true
  | Parquet.Schema.Ast.Type.UNION fts ->
    ( fts
      |> Seq.exists (
        function
          | Parquet.Schema.Ast.Type.NULL -> true
          | ____________________________ -> false
      )
    ) &&
    ( fts
      |> Seq.filter (
        function
          | Parquet.Schema.Ast.Type.NULL -> false
          | ____________________________ -> true
      )
      |> Seq.length = 1
    )
  | _________________________________ ->
    false

let rec fieldToType = function
  (* # PRIMITIVE TYPES *)
  (* > NOTE: We can't specify `unspecified` as `null` so we rely on an nullable string *)
  | Parquet.Schema.Ast.Type.NULL           -> "string"
  | Parquet.Schema.Ast.Type.BOOLEAN        -> "boolean"
  | Parquet.Schema.Ast.Type.INT            -> "integer"
  | Parquet.Schema.Ast.Type.LONG           -> "long"
  | Parquet.Schema.Ast.Type.FLOAT          -> "float"
  | Parquet.Schema.Ast.Type.DOUBLE         -> "double"
  | Parquet.Schema.Ast.Type.BYTES          -> "binary"
  | Parquet.Schema.Ast.Type.STRING         -> "string"
  (* # LOGICAL TYPES *)
  | Parquet.Schema.Ast.Type.DATE           -> "date"
  | Parquet.Schema.Ast.Type.DECIMAL   _    -> "decimal"
  | Parquet.Schema.Ast.Type.TIMESTAMP_MS   -> "timestamp"
  | Parquet.Schema.Ast.Type.TIME_MS        -> "long"
  (* # COMPLEX TYPES *)
  (* > NOTE: Array and Maps are re-factored to a Record type *)
  | Parquet.Schema.Ast.Type.ARRAY     _
  | Parquet.Schema.Ast.Type.MAP       _    -> String.Empty
  | Parquet.Schema.Ast.Type.UNION     ts   ->
    ts
    |> Seq.filter (
      function
        | Parquet.Schema.Ast.Type.NULL     -> false
        | ____________________________     -> true
    )
    |> Seq.head
    |> fieldToType
  (* # NAMED SCHEMA TYPES *)
  | Parquet.Schema.Ast.Type.ENUM      fqdn ->
    fqdn
    |> Parquet.Schema.Ast.Fqdn.toString 
    |> sprintf "string (enum = %s)"
  (* > NOTE: Errors are Record types *)
  | Parquet.Schema.Ast.Type.ERROR     _    -> String.Empty
  | Parquet.Schema.Ast.Type.FIXED     _    -> "binary"
  (* > NOTE: Record types are filtered out of field types *)
  | Parquet.Schema.Ast.Type.RECORD    _    -> String.Empty

let _ =

  Date.timestamp 0
  |> sprintf "%s | net.pandora.avroidl2dot | STARTED | DOTS"
  |> Output.stdout
  
  try
    let log = logger ()
    
    Directory.GetFiles
      ( Path.Combine
          ( __SOURCE_DIRECTORY__
          , @"../avro/avsc/"
          )
      // , "*.avsc" // NOTE: Retrieve all AVSC files from the folder
      , @"Interop.avsc"
      , SearchOption.TopDirectoryOnly
      )
    |> Seq.iter(
      fun f ->
        let env = Parquet.Schema.Ast.Environment.empty ()
        let ast = Parquet.Schema.Ast.empty ()
        
        let schema = File.ReadAllText f
        
        ( Date.timestamp                   0
        , Path.GetFileNameWithoutExtension f
        )
        ||> sprintf "%s | net.pandora.avroidl2dot | VERBOSE | DOTS | Creating an ER-diagram for: %s"
        |> Output.stdout
      
        schema
        |> JToken.Parse
        |> Avro.Schema.toParquetSchema log None env ast
        |> fun (env', ast', es) ->
          if Seq.isEmpty es then
            let tabs = Parquet.Tables.update log None ast
            let _ =
              use fs =
                new FileStream
                  ( path =
                      Path.Combine
                        ( __SOURCE_DIRECTORY__
                        , @"dots"
                        , Path.GetFileNameWithoutExtension f
                          |> sprintf "%s.dot" 
                        )
                  , mode = FileMode.OpenOrCreate
                  )
              use sw =
                new StreamWriter
                  ( stream   = fs
                  , encoding = UTF8Encoding false
                  )
              sprintf "digraph er {"
              |> sw.WriteLine
        
              sprintf "  /* Graph */"
              |> sw.WriteLine
              sprintf "  graph[rankdir=RL, overlap=false, splines=polyline]"
              |> sw.WriteLine
              sprintf "  /* Vertices */"
              |> sw.WriteLine
              
              ast'
              |> Seq.sortBy (fun ts -> Parquet.Schema.Ast.Fqdn.``namespace`` ts.Key)
              |> Seq.iter (
                fun ts ->
                  let fqdn = Parquet.Schema.Ast.Fqdn.toString ts.Key
                  let name = fqdn.Replace('.','_')
                  let hash =
                    tabs.[fqdn].Schema.ToString()
                    |> Hash.SHA256.sum
                  seq {
                    yield
                      ( sprintf "%s [shape=record, label=\"%s (%s)|" name fqdn hash
                      )
                    yield
                      ( seq {
                          yield "<pj_uid> pj_uid: bytearray (nullable = false)"
                          yield "<pj_pds> pj_pds: datetimeoffset (nullable = false)"
                          yield "<pj_sha> pj_sha: bytearray (nullable = true)"
                          yield "<pj_dts> pj_dts: datetimeoffset (nullable = false)"
                          yield "<pj_pid> pj_pid: bytearray (nullable = true)"
                          yield "<pj_fid> pj_fid: string (nullable = true)"
                          yield!
                            ( ts.Value
                              |> Seq.filter (
                                fun fs ->
                                  match fs.Value with
                                    | Parquet.Schema.Ast.Type.RECORD _ -> false
                                    | ________________________________ -> true
                              )
                              |> Seq.sortBy (fun fs -> fs.Key)
                              |> Seq.map(
                                fun fs ->
                                  let uid = fs.Key.Replace('.','_')
                                  let typ = fieldToType fs.Value
                                  let fid = 
                                    fs.Key.Replace('.','/')
                                    |> Path.GetFileName
                                  let isn = isNullable fs.Value
                                  sprintf "<%s> %s: %s (nullable = %b)" uid fid typ isn
                              )
                            )
                        }
                        |> Seq.reduce (sprintf "%s|%s")
                      )
                    yield
                      ( sprintf "\"]"
                      )
                  }
                  |> Seq.fold ((+)) String.Empty
                  |> sprintf "  %s"
                  |> sw.WriteLine
              )
        
              sprintf "  /* Edges */"
              |> sw.WriteLine
        
              ast'
              |> Seq.sortBy (fun ts -> Parquet.Schema.Ast.Fqdn.``namespace`` ts.Key)
              |> Seq.iter (
                fun ts ->
                  let fqdn = Parquet.Schema.Ast.Fqdn.toString ts.Key
                  let name = fqdn.Replace('.','_')
                  seq {
                    yield!
                      ( ts.Value
                        |> Seq.map (
                          fun fs ->
                            match fs.Value with
                              | Parquet.Schema.Ast.Type.RECORD (fqdn', otrans) ->
                                Some
                                  ( fs.Key
                                  , fqdn'
                                  , otrans
                                  , isNullable fs.Value
                                  )
                              | ______________________________________________ ->
                                None
                        )
                        |> Seq.choose id
                        |> Seq.sortBy (fun (key,_,_,_) -> key)
                        |> Seq.map(
                          fun (key, fqdn', otrans, nullable) ->
                            let both = "dir=both"
                            let head = "arrowhead=none"
                            let fqdn'' = Parquet.Schema.Ast.Fqdn.toString fqdn'
                            let name'   = fqdn''.Replace('.','_')
                            let card =
                              match otrans with
                                | Some trans ->
                                  match trans with
                                    | Parquet.Schema.Ast.Type.Transformation.NULLABLE -> "noneteeodot"
                                    | Parquet.Schema.Ast.Type.Transformation.ARRAY
                                    | Parquet.Schema.Ast.Type.Transformation.MAP      -> "invodot"
                                    | Parquet.Schema.Ast.Type.Transformation.UNION  _ ->
                                      if nullable then
                                        "noneteeodot"
                                      else
                                        "noneteetee"
                                | None       ->
                                  if nullable then
                                    "noneteeodot"
                                  else
                                    "noneteetee"
                            key.Replace('.','/')
                            |> Path.GetFileName
                            |> sprintf "%s:pj_pid -> %s:pj_uid [ %s, %s, arrowtail=%s, label=\"pj_fid = %s\" ];"
                                name' name
                                head both
                                card
                        )
                      )
                  }
                  |> Seq.fold (sprintf "%s\n  %s") String.Empty
                  |> fun cs ->
                    if String.IsNullOrEmpty cs then
                      ()
                    else
                      cs
                      |> sprintf "  %s"
                      |> sw.WriteLine
              )
        
              sprintf "}"
              |> sw.WriteLine
              
              sw.Flush()
              fs.Flush()
              sw.Close()
              fs.Close()
              
            ( Date.timestamp                   0
            , Path.GetFileNameWithoutExtension f
            )
            ||> sprintf "%s | net.pandora.avroidl2dot | VERBOSE | DOTS | Created the ER-diagram for: %s"
            |> Output.stdout
          else
            es
            |> Seq.iter (
              fun e ->
                ( Date.timestamp 0
                , e
                )
                ||> sprintf "%s | net.pandora.avroidl2dot | FAILURE | DOTS | Unexpected error:\n- %s"
                |> Output.stdout
            )
    )
    
    Date.timestamp 0
    |> sprintf "%s | net.pandora.avroidl2dot | STOPPED | DOTS"
    |> Output.stdout
    
    00
  with ex ->
    ( Date.timestamp 0
    , ex
    )
    ||> sprintf "%s | net.pandora.avroidl2dot | FAILURE | DOTS | Unexpected error:\n%A"
    |> Output.stdout
    -1
