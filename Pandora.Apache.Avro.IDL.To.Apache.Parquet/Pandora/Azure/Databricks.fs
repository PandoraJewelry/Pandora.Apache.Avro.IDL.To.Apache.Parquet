namespace Pandora.Azure

[<RequireQualifiedAccess>]
module Databricks =
  
  [<RequireQualifiedAccess>]
  module Delta =
  
    open System.Collections.Generic
    open System.IO
    
    open Pandora.Utils
    
    [<RequireQualifiedAccess>]
    module JSONL =
      (* https://jsonlines.org/ *)
      
      open System
      open System.Runtime.Serialization
    
      type EmptyObj () =
        do ()
      
      [<RequireQualifiedAccess>]
      module Schema =
        
        [<RequireQualifiedAccess>]
        module Type =
          
          let fromParquet isArray = function // TODO: Refactored
            (* * Delta Lake > Protocol > Schema Serialization Format:
                 - https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
               * Input: namespace Parquet.Data > public enum DataType:
                 - https://github.com/elastacloud/parquet-dotnet/blob/master/src/Parquet/Data/DataType.cs
               * Output: Sparks > Data Types > Supported Data Types:
                 - https://spark.apache.org/docs/latest/sql-ref-datatypes.html
               
               > NOTE: We are limited to types from AVRO IDL flattened (no nesting)
               > into several PARQUET files.
            *)
            | "boolean"                  -> "boolean"
            | "byte"
            | "signedbyte"
            | "unsignedbyte"             -> // TODO: Refactored
              if isArray then
                "binary"
              else
                "byte"
            | "short"
            | "unsignedshort"
            | "int16"
            | "unsignedint16"            -> "short"
            | "int32"                    -> "integer"
            | "int64"                    -> "long"
            | "bytearray"                -> "binary"
            | "float"                    -> "float"
            | "double"                   -> "double"
            | "decimal"                  -> "decimal"
            | "string"                   -> "string"
            | "datetimeoffset"           -> "timestamp"
            //| "timespan"                 -> "integer"
            | "timespan"                 -> "long"
            (* NOTE: We can't specify `unspecified` as `null` *) // TODO: Modified
            | "unspecified"              -> "string"
            | "int96"
            | "interval"    as otherwise
            //| "unspecified" as otherwise
            |                  otherwise ->
              otherwise
              |> sprintf "Azure.Databricks.Delta.JSONL.Schema.Type.fromParquet > otherwise: %s"
              |> failwith
        
        (* # Sample
          {
            "type": "struct",
            "fields": [
              {
                "name": "pj_uid",
                "type": "string",
                "nullable": true,
                "metadata": {}
              },
              {
                "name": "pj_sha",
                "type": "string",
                "nullable": true,
                "metadata": {}
              },
              {
                "name": "pj_dts",
                "type": "timestamp",
                "nullable": true,
                "metadata": {}
              },
              {
                "name": "channel",
                "type": "string",
                "nullable": true,
                "metadata": {}
              },
              {
                "name": "createdOn",
                "type": "timestamp",
                "nullable": true,
                "metadata": {}
              }
            ]
          }
        *)
        
        [<RequireQualifiedAccess>]
        module Field =
          
          (* Serializing F# Record type to JSON includes '@' character after each property
             - https://stackoverflow.com/a/13037901
          *)
          [<DataContract>]
          type t =
            { [<field: DataMember(Name="name")>]
              name     : string
              [<field: DataMember(Name="type")>]
              ``type`` : string
              [<field: DataMember(Name="nullable")>]
              nullable : bool
              [<field: DataMember(Name="metadata")>]
              metadata : EmptyObj
            }
          let init name ``type`` nullable =
            { name     = name
              ``type`` = ``type``
              nullable = nullable
              metadata = new EmptyObj ()
            }
        
        [<DataContract>]
        type t =
          { [<field: DataMember(Name="type")>]
            ``type`` : string
            [<field: DataMember(Name="fields")>]
            fields   : Field.t seq
          }
        
        let init fs =
          { ``type`` = "struct"
            fields   =
              fs
              |> Seq.map (
                fun (name, ``type``, nullable, isArray) -> // TODO: Refactored
                  Field.init name (Type.fromParquet isArray ``type``) nullable
              )
          }
          |> JSON.serialize false true
      
      [<RequireQualifiedAccess>]
      module Protocol =
        
        (* # Sample
          {
            "protocol": {
              "minReaderVersion": 1,
              "minWriterVersion": 2
            }
          }
        *)
        
        [<DataContract>]
        type n =
          { [<field: DataMember(Name="minReaderVersion")>]
            minReaderVersion : int
            [<field: DataMember(Name="minWriterVersion")>]
            minWriterVersion : int
          }
        
        [<DataContract>]
        type t =
          { [<field: DataMember(Name="protocol")>]
            protocol : n
          }
        
        let init protocol =
          { protocol =
              { minReaderVersion = 1
                minWriterVersion = 2
              }
          }
      
      [<RequireQualifiedAccess>]
      module MetaData =
        
        [<RequireQualifiedAccess>]
        module Format =
          
          [<DataContract>]
          type t =
            { [<field: DataMember(Name="provider")>]
              provider : string
              [<field: DataMember(Name="options")>]
              options  : EmptyObj
            }
          
          let init () =
            { provider = "parquet"
              options = new EmptyObj ()
            }
          
        (* # Sample
          {
            "id": "8479489c-543d-4103-9ef9-623bec1198e5",
            "format": {
              "provider": "parquet",
              "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"pj_uid\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pj_sha\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pj_dts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"channel\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"createdOn\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1671030376092
          }
        *)
        
        [<DataContract>]
        type n =
          { [<field: DataMember(Name="id")>]
            id               : Guid
            [<field: DataMember(Name="format")>]
            format           : Format.t
            [<field: DataMember(Name="schemaString")>]
            schemaString     : string
            [<field: DataMember(Name="partitionColumns")>]
            partitionColumns : string seq
            [<field: DataMember(Name="configuration")>]
            configuration    : EmptyObj
            [<field: DataMember(Name="createdTime")>]
            createdTime      : int64
          }
        
        (* # Sample
          {
            "metaData": {
              "id": "8479489c-543d-4103-9ef9-623bec1198e5",
              "format": {
                "provider": "parquet",
                "options": {}
              },
              "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"pj_uid\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pj_sha\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pj_dts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"channel\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"createdOn\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}",
              "partitionColumns": [],
              "configuration": {},
              "createdTime": 1671030376092
            }
          }
        *)
      
        [<DataContract>]
        type t =
          { [<field: DataMember(Name="metaData")>]
            metaData : n
          }
        
        let init mid timestamp (schema:string) =
          let dts =
            timestamp
            |> Date.Unix.fromDateTime
            |> int64
          let pds = "pj_pds"
          { metaData =
              { id               = mid
                format           = Format.init ()
                schemaString     = schema
                partitionColumns = seq { yield pds }
                configuration    = new EmptyObj ()
                createdTime      = dts
              }
          }
      
      [<RequireQualifiedAccess>]
      module Add =
      
        [<RequireQualifiedAccess>]
        module File =
          
          (* # Sample
            {
              "path": "20230116T0708407441014Z_9c662078-056b-4399-90b2-607ad75d5ec5.snappy.parquet",
              "size": 1729,
              "modificationTime": 1671030572000,
              "dataChange": true
            }
          *)
          
          [<DataContract>]
          type t =
            { [<field: DataMember(Name="path")>]
              path             : string
              [<field: DataMember(Name="size")>]
              size             : int64
              [<field: DataMember(Name="modificationTime")>]
              modificationTime : int64
              [<field: DataMember(Name="dataChange")>]
              dataChange       : bool
            }
            
          let init path size timestamp =
            let dts =
              timestamp
              |> Date.Unix.fromDateTime
              |> int64
            let pp =
              Path.Combine
                ( timestamp.ToString("yyyy-MM-dd")
                  |> sprintf "pj_pds=%s"
                , path
                )
            { path             = pp
              size             = size
              modificationTime = dts
              dataChange       = true
            }
        
        (* # Sample
          {
            "add": {
              "path": "20230116T0708407441014Z_9c662078-056b-4399-90b2-607ad75d5ec5.snappy.parquet",
              "size": 1729,
              "modificationTime": 1671030572000,
              "dataChange": true
            }
          }
        *)
        
        [<DataContract>]
        type t =
          { [<field: DataMember(Name="add")>]
            add : File.t
          }
        
        let init path size timestamp =
          { add = File.init path size timestamp
          }
      
      type t =
        Protocol.t   *
        MetaData.t   *
        Add.t
    
      let init timestamp size schema path =
        let mid =
          schema
          |> Hash.SHA256.sum
          |> fun h -> Guid.Parse(h.[0..31])
        
        ( Protocol.init ()
        , MetaData.init mid  timestamp schema
        , Add.init path size timestamp
        )
    
    let toFiles (delta:int64) path ((proto, meta, file):JSONL.t) =
      let i = sprintf "%020i" delta
      let d =
        Path.Combine
          ( path
          , "_delta_log"
          )
      let f =
        Path.Combine
          ( d
          , sprintf "%s.json" i
          )
      d
      |> Directory.CreateDirectory
      |> ignore
      (* ?) Store file addition info in a JSONL file *)
      File.WriteAllLinesAsync
          ( path     = f
          , contents =
              seq {
                yield JSON.serialize false true proto
                yield JSON.serialize false true meta
                yield JSON.serialize false true file
              }
          , encoding = UTF8.noBOM
          )
      |> Async.AwaitTask
      |> Async.RunSynchronously
    
    let toBytes (delta:int64) path ((proto, meta, file):JSONL.t) =
      let i = sprintf "%020i" delta
      let d =
        Path.Combine
          ( path
          , "_delta_log"
          )
      let f =
        Path.Combine
          ( d
          , sprintf "%s.json" i
          )
      use ms = new MemoryStream ()
      use sw = new StreamWriter (ms, UTF8.noBOM)
      seq {
        yield JSON.serialize false true proto
        yield JSON.serialize false true meta
        yield JSON.serialize false true file
      }
      |> Seq.iter(
        fun x ->
          sw.Write x
          sw.Flush()
      )
      ms.Flush()
      new KeyValuePair<string, byte[]>
        ( f
        , ms.ToArray()
        )
