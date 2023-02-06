namespace Pandora.Apache

#nowarn "62"

[<RequireQualifiedAccess>]
module Parquet =
  
  open System
  
  open Pandora.Azure
  open Pandora.Utils

  [<RequireQualifiedAccess>]
  module private Dict =
        
    type ('a, 'b) t = ('a, 'b) System.Collections.Generic.Dictionary
  
  [<RequireQualifiedAccess>]
  module Schema =
    
    [<RequireQualifiedAccess>]
    module rec Ast =
      
      [<RequireQualifiedAccess>]
      module Fqdn =
        
        type t =
          FQDN of
              name        : string *
            ``namespace`` : string option
        
        let name          (FQDN (n,__)) = n
        let ``namespace`` (FQDN (_,ns)) = ns
        
        let toString = function
          | FQDN (n, Some ns) -> sprintf "%s.%s" ns n
          | FQDN (n, None   ) ->                    n
      
      [<RequireQualifiedAccess>]
      module Type =
        
        [<RequireQualifiedAccess>]
        module Transformation =
          
          type t =
            | ARRAY
            | MAP
            | UNION of len:int
        
        type t =
          
          (* # PRIMITIVE TYPES *)
          (* No value *)
          | NULL
          (* A binary value *)
          | BOOLEAN
          (* 32-bit signed integer *)
          | INT
          (* 64-bit signed integer *)
          | LONG
          (* Single precision (32-bit) IEEE 754 floating-point number *)
          | FLOAT
          (* Double precision (64-bit) IEEE 754 floating-point number *)
          | DOUBLE
          (* Sequence of 8-bit unsigned bytes *)
          | BYTES
          (* Unicode character sequence *)
          | STRING
          
          (* # LOGICAL TYPES *)
          (* An int stores the number of days from the unix epoch, 1 January 1970 (ISO calendar). *)
          | DATE
          (* Decimal logical type represents an arbitrary-precision signed
             decimal number of the form unscaled × 10^-scale
          *)
          | DECIMAL of
              precision : int *
              scale     : int option (* If not specified, the scale is 0 *)
          (* A long stores the number of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC *)
          | TIMESTAMP_MS
          (* An int stores the number of milliseconds after midnight, 00:00:00.000 *)
          | TIME_MS
          
          (* # COMPLEX TYPES *)
          (* An array of any type t is denoted array<t>. For example, an
             array of strings is denoted array<string>, and a multidimensional
             array of Foo records would be array<array<Foo>>.
          *)
          | ARRAY of t
          (* Map types are written similarly to array types. An array that
             contains values of type t is written map<t>. As in the JSON
             schema format, all maps contain string-type keys.
          *)
          | MAP   of t
          (* Union types are denoted as union { typeA, typeB, typeC, ... }. *)
          //| UNION of t seq
          | UNION of t array
          
          (* # NAMED SCHEMA TYPES *)
          (* Definitions of named schemata, including:
             - records,
             - errors,
             - enums, and
             - fixeds.
          *)
          | ENUM   of fqdn : Fqdn.t
          | ERROR  of fqdn : Fqdn.t
          | FIXED  of fqdn : Fqdn.t * size  : int
          | RECORD of fqdn : Fqdn.t * trans : Transformation.t option
        
      [<RequireQualifiedAccess>]
      module Environment =
  
        type t = (string, Type.t) Dict.t
        
        let empty () = new t (StringComparer.OrdinalIgnoreCase)
      
      [<RequireQualifiedAccess>]
      module Field =
        
        open Newtonsoft.Json.Linq
        
        open Pandora.Helpers
        
        type t = Type.t
        
        let rec toType ons (env:Ast.Environment.t) (ast:Ast.t) (t:JToken) =
          match t.Type with
            | JTokenType.Object when JSON.memberIsArray  "type" (t :?> JObject) ->
              let o = t :?> JObject
              let (ts, es) =
                JSON.memberTokValues "type" o
                |> Seq.map (toType ons env ast)
                |> fun xs ->
                  ( xs
                    |> Seq.filter (function | Result.Ok    _ -> true | _ -> false)
                  , xs
                    |> Seq.filter (function | Result.Error _ -> true | _ -> false)
                  )
              if Seq.isEmpty es then
                ts
                |> Seq.choose (function | Result.Ok    t -> Some t | _ -> None)
                |> Seq.toArray
                |> Type.UNION
                |> Result.Ok
              else
                es
                |> Seq.choose (function | Result.Error e -> Some e | _ -> None)
                |> Seq.concat
                |> Result.Error
            | JTokenType.Object when JSON.memberIsNested "type" (t :?> JObject) ->
              let o = t :?> JObject
              o.["type"]
              |> toType ons env ast
            | JTokenType.Object when JSON.memberIsString "type" (t :?> JObject) ->
              let o = t :?> JObject
              let ot = JSON.memberStrValue "type" o
              match ot with
                | Some x -> fromString ons env ast (Some o) x
                | ______ -> Result.Error Seq.empty
            | JTokenType.String ->
              let s = t :?> JValue
              s.Value.ToString()
              |> fromString ons env ast None
            | otherwise ->
              seq {
                yield sprintf "Field.toType > otherwise: %s" (t.ToString())
              }
              |> Result.Error
        
        and private fromString ons (env:Ast.Environment.t) (ast:Ast.t) (oo:JObject option) = function
          // Records > field default values:
          // - https://avro.apache.org/docs/1.11.1/specification/#schema-record
          //
          // Specifications > Logical Types:
          // - https://avro.apache.org/docs/1.11.1/specification/#logical-types
          //
          // Some of the logical types supported by Avro’s JSON format are also
          // supported by Avro IDL. The currently supported types are:
          // - https://avro.apache.org/docs/1.11.1/idl-language/#logical-types
          | "null" ->
            Type.NULL
            |> Result.Ok
          | "boolean" ->
            Type.BOOLEAN
            |> Result.Ok
          | "int" ->
            match oo with
              | Some o ->
                match JSON.memberStrValue "logicalType" o with
                  | Some "date"        ->
                    Type.DATE
                    |> Result.Ok
                  | Some "time-millis" ->
                    Type.TIME_MS
                    |> Result.Ok
                  | __________________ ->
                    Type.INT
                    |> Result.Ok
              | None ->
                Type.INT
                |> Result.Ok
          | "long" ->
            match oo with
              | Some o ->
                match JSON.memberStrValue "logicalType" o with
                  | Some "timestamp-millis" ->
                    Type.TIMESTAMP_MS
                    |> Result.Ok
                  | __________________ ->
                    Type.LONG
                    |> Result.Ok
              | None ->
                Type.LONG
                |> Result.Ok
          | "float" ->
            Type.FLOAT
            |> Result.Ok
          | "double" ->
            Type.DOUBLE
            |> Result.Ok
          | "bytes" ->
            match oo with
              | Some o ->
                match JSON.memberStrValue "logicalType" o, JSON.memberIntValue "precision" o with
                  | Some "decimal", Some p ->
                    let s =
                      match JSON.memberIntValue "scale" o with
                        | Some x -> x
                        | None   -> 0
                    (      p
                    , Some s
                    )
                    |> Type.DECIMAL
                    |> Result.Ok
                  | ______________________ ->
                    Type.BYTES
                    |> Result.Ok
              | None ->
                Type.BYTES
                |> Result.Ok
          | "string" ->
            Type.STRING
            |> Result.Ok
          | "array" ->
            match oo with
              | Some o ->
                match toType ons env ast o.["items"] with
                  | Result.Ok t ->
                    t
                    |> Type.ARRAY
                    |> Result.Ok
                  | Result.Error es ->
                    es
                    |> Result.Error
              | None ->
                seq {
                  yield sprintf "Type.fromString > `array` is missing the `items` type attribute"
                }
                |> Result.Error
          | "map" ->
            match oo with
              | Some o ->
                match toType ons env ast o.["values"] with
                  | Result.Ok t ->
                    t
                    |> Type.MAP
                    |> Result.Ok
                  | Result.Error es ->
                    es
                    |> Result.Error
              | None ->
                seq {
                  yield sprintf "Type.fromString > `map` is missing the `values` type attribute"
                }
                |> Result.Error
          | "enum" ->
            match oo with
              | Some o ->
                Fqdn.FQDN
                  ( JSON.memberStrValue "name" o
                    |> Option.defaultValue String.Empty
                  , match JSON.memberStrValue "namespace" o with
                      | Some ns -> Some ns
                      | None    -> ons
                  )
                |> Type.ENUM
                |> Result.Ok
              | None ->
                seq {
                  yield sprintf "Type.fromString > `enum` is missing the `name` type attribute"
                }
                |> Result.Error
          | "error" ->
            match oo with
              | Some o ->
                Fqdn.FQDN
                  ( JSON.memberStrValue "name" o
                    |> Option.defaultValue String.Empty
                  , match JSON.memberStrValue "namespace" o with
                      | Some ns -> Some ns
                      | None    -> ons
                  )
                |> Type.ERROR
                |> Result.Ok
              | None ->
                seq {
                  yield sprintf "Type.fromString > `error` is missing the `name` type attribute"
                }
                |> Result.Error
          | "fixed" ->
            match oo with
              | Some o ->
                match JSON.memberStrValue "name" o, JSON.memberIntValue "size" o with
                  | Some n, Some s ->
                    ( 
                      Fqdn.FQDN
                        ( n
                        , match JSON.memberStrValue "namespace" o with
                          | Some ns -> Some ns
                          | None    -> ons
                        )
                    , s
                    )
                    |> Type.FIXED
                    |> Result.Ok
                  | ______________ ->
                    seq {
                      yield sprintf "Type.fromString > `fixed` is missing the `size` type attribute: %s" (o.ToString())
                    }
                    |> Result.Error
              | None ->
                seq {
                  yield sprintf "Type.fromString > `fixed` is missing the `size` type attribute: %s"  (oo.ToString())
                }
                |> Result.Error
          | "record" ->
            match oo with
              | Some o ->
                ( Fqdn.FQDN
                    ( JSON.memberStrValue "name" o
                      |> Option.defaultValue String.Empty
                    , match JSON.memberStrValue "namespace" o with
                        | Some ns -> Some ns
                        | None    -> ons
                    )
                , None
                )
                |> Type.RECORD
                |> Result.Ok
              | None ->
                seq {
                  yield sprintf "Type.fromString > `record` is missing the `name` type attribute"
                }
                |> Result.Error
          | namedSchema ->
            let fqdnSepNS =
              Fqdn.FQDN
                ( namedSchema
                , ons
                )
              |> Fqdn.toString
            let fqdnComNS =
              Fqdn.FQDN
                ( namedSchema
                , None
                )
              |> Fqdn.toString
            if   env.ContainsKey fqdnSepNS then
              env.[fqdnSepNS]
              |> Result.Ok
            elif env.ContainsKey fqdnComNS then
              env.[fqdnComNS]
              |> Result.Ok
            else
              seq {
                yield sprintf "Type.fromString > namedSchema: %s" namedSchema
              }
              |> Result.Error
      
      [<RequireQualifiedAccess>]
      module Table =
        
        type t = (string, Field.t) Dict.t
        
        let empty () = new t ()
      
      type t = (Fqdn.t, Table.t) Dict.t
      
      let empty () = new t ()
  
  [<RequireQualifiedAccess>]
  module Tables =
    
    open System.IO
    open System.Collections.Generic
    
    open Parquet
    open Parquet.Schema
    open Parquet.Rows
    
    [<RequireQualifiedAccess>]
    module Bytes =
      
      type t = (string, byte[]) Dict.t
      
      let empty () = new t (StringComparer.OrdinalIgnoreCase)
    
    type t = (string, Table) Dict.t
    
    let private uuid name =
      // Parquet Logical Type Definitions > UUID:
      // - https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#uuid
      //
      // Guid.ToByteArray Method:
      // - https://learn.microsoft.com/en-us/dotnet/api/system.guid.tobytearray
      new DataField
        ( name     = name
        , dataType = DataType.ByteArray
        , hasNulls = false
        ) :> Field
    
    let private label name =
      new DataField
        ( name     = name
        , dataType = DataType.String
        , hasNulls = true
        , isArray  = false
        ) :> Field
    
    let private timestamp time name =
      new DateTimeDataField
        ( name     = name
        , format   =
            if time then
              DateTimeFormat.DateAndTime
            else
              DateTimeFormat.Date
        , hasNulls = false
        ) :> Field
    
    let private uid () =
      // Random UUID, as bytes
      uuid "pj_uid"
    
    let private sha () =
      // Use the 32 bytes of the SHA256 hash of the raw AVRO payload
      uuid "pj_sha"
    
    let private dts () =
      (* Date-time stamp *)
      timestamp true "pj_dts"
    
    let private pds () =
      (* Partition date stamp:
         * The Foundation of Your Lakehouse Starts With Delta Lake > Generated columns:
         - https://www.databricks.com/blog/2021/12/01/the-foundation-of-your-lakehouse-starts-with-delta-lake.html
      *)
      timestamp false "pj_pds"
    
    let private pid () =
      uuid "pj_pid"
    
    let private fid () =
      label "pj_fid"
    
    let private isNullable = function
      | Schema.Ast.Type.NULL      -> // TODO: Added
        true
      | Schema.Ast.Type.UNION fts ->
        ( fts
          |> Seq.exists (
            function
              | Schema.Ast.Type.NULL -> true
              | ____________________ -> false
          )
        ) &&
        ( fts
          |> Seq.filter (
            function
              | Schema.Ast.Type.NULL -> false
              | ____________________ -> true
          )
          |> Seq.length = 1
        )
      | _________________________ ->
        false

    let rec isPrimitive = function
      | Schema.Ast.Type.ARRAY        _
      | Schema.Ast.Type.MAP          _
      | Schema.Ast.Type.ERROR        _
      | Schema.Ast.Type.RECORD       _  ->
        false
      | Schema.Ast.Type.UNION fts as ft ->
        (* Sample of a nullable primitive type:
           - `"type" : [ "null", "string" ]`.
        *)
        isNullable ft &&
        (Seq.fold (fun acc x -> acc && isPrimitive x) true fts)
      | _______________________________ ->
        true
    
    let isNullableRecord = function
      | Schema.Ast.Type.UNION fts as ft ->
        isNullable ft &&
        (Seq.exists (function | Schema.Ast.Type.RECORD _ -> true | _ -> false) fts)
      | _______________________________ ->
        false
    
    let private isComplexNested = function
      | Schema.Ast.Type.ARRAY _
      | Schema.Ast.Type.MAP   _ -> true
      | _______________________ -> false
    
    let rec private isFlatArrayPrimitive = function
      | Schema.Ast.Type.ARRAY ft  -> isPrimitive ft || isFlatArrayPrimitive ft
      | _________________________ -> false
    
    let private isComplexPrimitive = function
      | Schema.Ast.Type.ARRAY ft
      | Schema.Ast.Type.MAP   ft -> isPrimitive ft
      | ________________________ -> false
    
    let private schemaFieldHlp name nullable datatype =
      new DataField
        ( name     = name
        , dataType = datatype
        , hasNulls = nullable
        ) :> Field
    
    let rec private schemaField unionNullable name (ft:Schema.Ast.Type.t) =
      if isPrimitive ft then
        let nullable = unionNullable || isNullable ft // TODO: Refactored
        match ft with
          | Schema.Ast.Type.NULL ->
            DataType.String // TODO: Changed
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.BOOLEAN ->
            DataType.Boolean
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.INT ->
            DataType.Int32
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.LONG ->
            DataType.Int64
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.FLOAT ->
            DataType.Float
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.DOUBLE ->
            DataType.Double
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.BYTES ->
            // TODO: Refactored
            DataType.ByteArray
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.STRING ->
            DataType.String
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.DATE ->
            new DateTimeDataField
              ( name     = name
              , format   = DateTimeFormat.Date
              , hasNulls = nullable
              ) :> Field
            |> Some
          | Schema.Ast.Type.DECIMAL (p, so) ->
            let s =
              match so with
                | Some v -> v
                | None   -> 0
            new DecimalDataField
              ( name                   = name
              , precision              = p
              , scale                  = s
              , forceByteArrayEncoding = false
              , hasNulls               = nullable
              ) :> Field
            |> Some
          | Schema.Ast.Type.TIMESTAMP_MS ->
            new DateTimeDataField
              ( name   = name
              , format = DateTimeFormat.DateAndTime
              , hasNulls = nullable
              ) :> Field
            |> Some
          | Schema.Ast.Type.TIME_MS ->
            DataType.TimeSpan
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.UNION fts  ->
            fts
            |> Seq.filter (
              function
                | Schema.Ast.Type.NULL -> false
                | ____________________ -> true
            )
            |> Seq.map (schemaField nullable name)
            |> Seq.head (* Never empty cos of `isPrimitive ft` *)
          | Schema.Ast.Type.ENUM _  ->
            DataType.String
            |> schemaFieldHlp name nullable
            |> Some
          | Schema.Ast.Type.FIXED _ ->
            DataType.ByteArray
            |> schemaFieldHlp name nullable
            |> Some
          | _______________________ ->
            None
      else
        None

    let rec private nestedNamedSchemas = function
      | Schema.Ast.Type.ARRAY  ft
      | Schema.Ast.Type.MAP    ft ->
        nestedNamedSchemas ft
      | Schema.Ast.Type.ERROR   fqdn
      | Schema.Ast.Type.RECORD (fqdn, _) ->
        seq {
          yield fqdn
        }
      | Schema.Ast.Type.UNION  fts  ->
        fts
        |> Seq.map nestedNamedSchemas
        |> Seq.concat
      | __________________________  ->
        Seq.empty

    let private schemaFields isext ons (tab:Schema.Ast.Table.t) =
      let ns =
        match ons with
          | Some v -> sprintf "%s." v
          | None   -> String.Empty
      
      let fs =
        tab
        |> Seq.filter (fun kv -> isPrimitive kv.Value)
        |> Seq.sortBy (fun kv -> kv.Key)
        |> Seq.map(
          fun kv ->
            let key =
              kv.Key.Replace
                ( ns
                , String.Empty
                )
            schemaField false key kv.Value
        )
        |> Seq.choose id
        |> Seq.toList
      if isext then
        (* REMARK: If `extended` by `union`, do we need `fid`? Already part of filename *)
        uid () :: pds () :: pid () :: fid () :: fs
      else
        uid () :: pds () :: sha () :: dts () :: fs

    let update otabs (ast:Schema.Ast.t) =
      let tabs =
        match otabs with
          | Some ts -> ts
          | None    -> new t (StringComparer.OrdinalIgnoreCase)
      let fqdns =
        ast.Values
        |> Seq.map (
          fun kv ->
            kv.Values
            |> Seq.map nestedNamedSchemas
            |> Seq.concat
        )
        |> Seq.concat
        |> Seq.fold(
          fun acc x ->
            acc |> Set.add x
        ) Set.empty
      ast
      |> Seq.sortBy (fun kv -> kv.Key)
      |> Seq.iter (
        fun kv ->
          let key =
            kv.Key
            |> Schema.Ast.Fqdn.toString
          let ons =
            kv.Key
            |> Schema.Ast.Fqdn.``namespace``
          let ext =
            Set.contains kv.Key fqdns
          if not (tabs.ContainsKey key) then
            tabs.[key] <-
              ( new Table
                  ( new ParquetSchema
                      ( fields = schemaFields ext ons kv.Value
                      )
                  )
              )
      )
      tabs
    
    let empty (ast:Schema.Ast.t) =
      update None ast

    let rec private primitive2obj (v:obj) (t:Schema.Ast.Type.t) = // TODO: Changed
      if null = v then 
        v
      else
        match t with
          | Schema.Ast.Type.NULL         ->
            null // TODO: Changed
          | Schema.Ast.Type.BOOLEAN
          | Schema.Ast.Type.INT
          | Schema.Ast.Type.LONG
          | Schema.Ast.Type.FLOAT
          | Schema.Ast.Type.DOUBLE
          | Schema.Ast.Type.BYTES
          | Schema.Ast.Type.STRING
          | Schema.Ast.Type.TIME_MS -> // TODO: Added extra clauses
            v
          | Schema.Ast.Type.DECIMAL _  ->
            v :?> Avro.AvroDecimal
            |> Avro.AvroDecimal.ToDecimal             :> obj
          | Schema.Ast.Type.ENUM    _    ->
            (v :?> Avro.Generic.GenericEnum).Value    :> obj
          | Schema.Ast.Type.FIXED   _    ->
            (v :?> Avro.Generic.GenericFixed).Value   :> obj
          | Schema.Ast.Type.DATE
          | Schema.Ast.Type.TIMESTAMP_MS ->
            new DateTimeOffset(v :?> System.DateTime) :> obj
          | Schema.Ast.Type.UNION   nt    ->
            (* NOTE: If a `union` is passed, we know it's a nullable-primitive *)
            nt
            |> Seq.filter (
              function
                | Schema.Ast.Type.NULL -> false
                | ____________________ -> true
            )
            |> Seq.head
            |> primitive2obj v
          | Schema.Ast.Type.ERROR  _
          | Schema.Ast.Type.RECORD _
          | Schema.Ast.Type.MAP    _
          | Schema.Ast.Type.ARRAY  _ as otherwise ->
            sprintf "Parquet.Tables.primitive2obj:\n-Type: %A\n-Value: %A" otherwise v
            |> failwith

    let private compUnionSchemaTypes a b =
      match a, b with
        (* NOTE: We can't extract the `precision` with `reflection` *)
        |   Schema.Ast.Type.UNION [| Schema.Ast.Type.NULL; Schema.Ast.Type.DECIMAL (_, osa)|]
          , Schema.Ast.Type.UNION [| Schema.Ast.Type.NULL; Schema.Ast.Type.DECIMAL (_, osb)|] ->
          osa = osb
        | ___________________________________________________________________________________ ->
          a = b

    let rec populate
      (dto:DateTimeOffset)
      osha opid ofid
      (ast:Schema.Ast.t)
      (avro:Avro.Generic.GenericRecord)
      asn asns
      (tables:t) =
        // use `isPrimitive` to filter out what to fill for a given record
        let uid = Guid.NewGuid().ToByteArray()
        let (ps, cs) =
          let fqdn =
            ( asn
            , asns
            )
            |> Schema.Ast.Fqdn.FQDN
          ast.[fqdn]
          |> fun fs ->
            ( fs
              |> Seq.filter (fun f -> isPrimitive f.Value)
              |> Seq.sortBy (fun f ->             f.Key)
              |> Seq.map(
                fun kv ->
                  let key =
                    kv.Key.Replace
                      ( asns
                        |> Option.defaultValue String.Empty
                        |> sprintf "%s."
                      , String.Empty
                      )
                  match avro.TryGetValue key with
                    | (true,  v) -> primitive2obj v kv.Value
                    | (false, _) -> null // TODO: Changed
              )
            , fs
              |> Seq.filter (fun f -> not (isPrimitive f.Value))
              |> Seq.choose (
                fun f ->
                  match f.Value with
                    | Schema.Ast.Type.ERROR  _
                    | Schema.Ast.Type.RECORD _ -> Some f
                    | ________________________ -> None
              )
            )
        
        (* Child record/error types *)
        cs
        |> Seq.iter(
          fun kv ->
            let key =
              kv.Key.Replace
                ( asns
                  |> Option.defaultValue String.Empty
                  |> sprintf "%s."
                , String.Empty
                )
            match avro.TryGetValue key with
              | (true,  v) ->
                match kv.Value with
                  | Schema.Ast.Type.RECORD (fqdn, Some Schema.Ast.Type.Transformation.ARRAY) ->
                    popuArray dto uid key ast (v :?> IEnumerable<_>)       fqdn tables
                  | Schema.Ast.Type.RECORD (fqdn, Some Schema.Ast.Type.Transformation.MAP)   ->
                    popuMap   dto uid key ast (v :?> Dictionary<string,_>) fqdn tables
                  | Schema.Ast.Type.RECORD (fqdn, Some (Schema.Ast.Type.Transformation.UNION len)) ->
                    popuUnion dto uid key ast  v                           fqdn tables len
                  | Schema.Ast.Type.ERROR    fqdn
                  | Schema.Ast.Type.RECORD  (fqdn,_)                                         ->
                    let (n', ons') =
                      ( Schema.Ast.Fqdn.name          fqdn
                      , Schema.Ast.Fqdn.``namespace`` fqdn
                      )
                    populate dto None (Some uid) (Some key) ast (v :?> Avro.Generic.GenericRecord) n' ons' tables
                  | ________________________ -> ()
              | (false, _) -> ()
        )
        
        let vs : obj seq =
          seq {
            (* "pj_uid" *)
            yield  uid
            (* "pj_pds" *)
            yield  dto
            if not (None = osha) then
              (* "pj_sha" *)
              yield Option.defaultValue [| |] osha
              (* "pj_dts" *)
              yield dto
            if not (None = opid) then
              (* "pj_pid" *)
              yield Option.defaultValue [| |] opid
            if not (None = ofid) then
              (* "pj_fid" *)
              yield Option.defaultValue String.Empty ofid
            yield! ps
          }
        
        let row = new Row(values = vs)
        let tfn =
          match asns with
            | Some ns -> sprintf "%s.%s" ns asn
            | None    -> asn
        try
          tables.[tfn].Add(item = row)
        with ex ->
          printfn "> DEBUG | tfn : %A" tfn
          printfn "> DEBUG | ex  : %A" ex

    and private popuArray dto uid key (ast:Schema.Ast.t) (vs : IEnumerable<_>) fqdn (tables:t) =
      let fv =
        fqdn
        |> Schema.Ast.Fqdn.``namespace``
        |> Option.defaultValue String.Empty
        |> sprintf "%s.item"
      if ast.[fqdn].ContainsKey fv then
        let ft = ast.[fqdn].[fv]
        vs
        |> Seq.map (
          fun nv ->
            let vs : obj seq =
              seq {
                (* "pj_uid" *)
                yield  Guid.NewGuid().ToByteArray()
                (* "pj_pds" *)
                yield  dto
                (* "pj_pid" *)
                yield  uid
                (* "pj_fid" *)
                yield  key
                (* "item" *)
                yield (primitive2obj nv ft)
              }
            new Row
              ( values = vs
              )
        )
        |> Seq.iter(
          fun row ->
            let tfn =
              fqdn
              |> Schema.Ast.Fqdn.toString
            tables.[tfn].Add(item = row)
        )
      else
        let (n', ons') =
          ( Schema.Ast.Fqdn.name          fqdn
          , Schema.Ast.Fqdn.``namespace`` fqdn
          )
        (vs :?> IEnumerable<obj>)
        |> Seq.iter (
          fun nv ->
            populate dto None (Some uid) (Some key) ast (nv :?> Avro.Generic.GenericRecord) n' ons' tables
        )

    and private popuMap dto uid key (ast:Schema.Ast.t) (kvs : Dictionary<string,_>) fqdn (tables:t) =
      let fv =
        fqdn
        |> Schema.Ast.Fqdn.``namespace``
        |> Option.defaultValue String.Empty
        |> sprintf "%s.value"
      if ast.[fqdn].ContainsKey fv then
        let uid' = Guid.NewGuid().ToByteArray()
        let ft   = ast.[fqdn].[fv]
        let ip   = isPrimitive ft
        kvs
        |> Seq.map (
          fun nkv ->
            let vs : obj seq =
              seq {
                (* "pj_uid" *)
                yield  uid'
                (* "pj_pds" *)
                yield  dto
                (* "pj_pid" *)
                yield  uid
                (* "pj_fid" *)
                yield  key
                (* "key" *)
                yield                  nkv.Key
                (* "value" *)
                if ip then
                  yield (primitive2obj nkv.Value ft)
                (* nested "value" record *)
                else
                  match ft with
                    | Schema.Ast.Type.RECORD (fqdn',_) ->
                      let (n', ons') =
                        ( Schema.Ast.Fqdn.name          fqdn'
                        , Schema.Ast.Fqdn.``namespace`` fqdn'
                        )
                      let nv = nkv.Value :> obj :?> Avro.Generic.GenericRecord
                      populate dto None (Some uid') (Some "value") ast nv n' ons' tables
                    | ________________________________ ->
                      ()
              }
            new Row
              ( values = vs
              )
        )
        |> Seq.iter(
          fun row ->
            let tfn =
              fqdn
              |> Schema.Ast.Fqdn.toString
            tables.[tfn].Add(item = row)
        )
      else
        let (n', ons') =
          ( Schema.Ast.Fqdn.name          fqdn
          , Schema.Ast.Fqdn.``namespace`` fqdn
          )
        (kvs :?> Dictionary<string, obj>)
        |> Seq.iter (
          fun nkv ->
            let nv = nkv.Value :?> Avro.Generic.GenericRecord
            populate dto None (Some uid) (Some key) ast nv n' ons' tables
        )

    and private popuUnion dto uid key (ast:Schema.Ast.t) v fqdn (tables:t) len =
      let fs =
        ( fun i ->
            fqdn
            |> Schema.Ast.Fqdn.``namespace``
            |> Option.defaultValue String.Empty
            |> fun x -> sprintf "%s.type%i" x i
        )
        |> Seq.init len
      let ts =
        fs
        |> Seq.map (
          fun fv -> ast.[fqdn].[fv]
        )
      (* NOTE: With primitives, we use `reflection`. With `complex` we just retrieve FQDN
         and then match against the filtered (Seq.iter i _)
         
         REMARK: Since we have transformed `arrays`, `maps` and `unions` into `records` we
         will only either have (nullable) `primitives` or `record` types. Pretty nice !!!
      *)
      match union2schemaType ast ts v with
        | Some rt ->
          let (ps,cs) =
            ( ts
              |> Seq.filter(
                fun x ->
                  match x with
                    | Schema.Ast.Type.ERROR  _
                    | Schema.Ast.Type.RECORD _ -> false
                    | ________________________ -> true
              )
            , ts
              |> Seq.filter(
                fun x ->
                  match x with
                    | Schema.Ast.Type.ERROR  _
                    | Schema.Ast.Type.RECORD _ -> true
                    | ________________________ -> false
              )
            )
                          
          let vs : obj seq =
            seq {
              (* "pj_uid" *)
              yield  Guid.NewGuid().ToByteArray()
              (* "pj_pds" *)
              yield  dto
              (* "pj_pid" *)
              yield  uid
              (* "pj_fid" *)
              yield  key
              (* "type1" … "typeN" *)
              yield!
                ( ps
                  |> Seq.map(
                    fun x ->
                      if compUnionSchemaTypes rt x then
                        primitive2obj v rt
                      else
                        null
                  )
                )
            }
                        
          let tfn =
            fqdn
            |> Schema.Ast.Fqdn.toString
          tables.[tfn].Add
            ( item =
                new Row
                  ( values = vs
                  )                            
            )

          cs
          |> Seq.iter(
            fun x ->
              if compUnionSchemaTypes rt x then
                match rt with

                  | Schema.Ast.Type.RECORD (fqdn', Some Schema.Ast.Type.Transformation.ARRAY) ->
                    let key' =
                      let pn = Schema.Ast.Fqdn.name fqdn
                      let cn = Schema.Ast.Fqdn.name fqdn'
                      cn.Replace
                        ( pn
                        , String.Empty
                        )
                      |> fun cs ->
                        if String.length cs > 2 then
                          cs.[0..0].ToLowerInvariant() + cs.[1..]
                        else
                          cs.ToLowerInvariant()
                          
                    popuArray dto uid key' ast (v :?> IEnumerable<_>) fqdn' tables

                  | Schema.Ast.Type.RECORD (fqdn', Some Schema.Ast.Type.Transformation.MAP)  ->
                    let key' =
                      let pn = Schema.Ast.Fqdn.name fqdn
                      let cn = Schema.Ast.Fqdn.name fqdn'
                      cn.Replace
                        ( pn
                        , String.Empty
                        )
                      |> fun cs ->
                        if String.length cs > 2 then
                          cs.[0..0].ToLowerInvariant() + cs.[1..]
                        else
                          cs.ToLowerInvariant()
                          
                    popuMap dto uid key' ast (v :?> Dictionary<string,_>) fqdn' tables

                  | Schema.Ast.Type.RECORD (fqdn', Some (Schema.Ast.Type.Transformation.UNION _))  ->
                    (* NOTE: Nested unions are not supported
                       `avro-tools.jar idl2schemata …`
                       > Exception in thread "main" org.apache.avro.AvroRuntimeException: Nested union
                    *)
                    (* TODO: Log error message here *)
                    ()
                  | Schema.Ast.Type.RECORD (fqdn', None)                                           ->
                    let (n', ons') =
                      ( Schema.Ast.Fqdn.name          fqdn'
                      , Schema.Ast.Fqdn.``namespace`` fqdn'
                      )

                    populate dto None (Some uid) (Some key) ast (v  :?> Avro.Generic.GenericRecord) n' ons' tables
                  | ______________________________________________________________________________ ->
                    ()
          )
        | None ->
          ()

    and private union2schemaType (ast:Schema.Ast.t) (ts:Schema.Ast.Type.t seq) (o:obj) =
      match o with
        | :? (System.Byte[])                                           ->
          union2schemaTypeHelper o
        | :? IEnumerable<        obj> as a  ->
          (* NOTE: When no element in array, we don't really care which array it is *)
          if Seq.isEmpty a then
            None
          else
            let nrt =
              Avro.Reflect.ArrayHelper(a).Enumerable
              |> Seq.cast<_>
              |> Seq.head
              |> union2schemaType ast ts
            let rts =
              ts
              |> Seq.filter(
                fun x ->
                  match x with
                    | Schema.Ast.Type.RECORD (fqdn, Some Schema.Ast.Type.Transformation.ARRAY) ->
                      let ns =
                        fqdn
                        |> Schema.Ast.Fqdn.``namespace``
                        |> Option.defaultValue String.Empty
                      Some ast.[fqdn].[sprintf "%s.item" ns] = nrt
                    | ________________________________________________________________________ -> false
              )
            if Seq.isEmpty rts then
              None
            else
              rts
              |> Seq.head
              |> Some
        | :? IDictionary<string, obj> as m  ->
          if Seq.isEmpty m then
            None
          else
            let nrt =
              Avro.Reflect.ArrayHelper(m).Enumerable
              |> Seq.cast<KeyValuePair<string,_>>
              |> Seq.map (fun kv -> kv.Value)
              |> Seq.head
              |> union2schemaType ast ts
            let rts =
              ts
              |> Seq.filter(
                fun x ->
                  match x with
                    | Schema.Ast.Type.RECORD (_, Some Schema.Ast.Type.Transformation.MAP) -> true
                    | ___________________________________________________________________ -> false
              )
            if Seq.isEmpty rts then
              None
            else
              rts
              |> Seq.head
              |> Some
        | :? Avro.Generic.GenericRecord                          as r  ->
          let rts =
            ts
            |> Seq.filter(
              fun x ->
                match x with
                  | Schema.Ast.Type.RECORD (fqdn, None) ->
                    (* NOTE: If the `reflected` values FQDN matches with any of the stored types *)
                    let fqdn' =
                      (      r.Schema.Name
                      , Some r.Schema.Namespace
                      )
                      |> Schema.Ast.Fqdn.FQDN
                    fqdn = fqdn'
                  | ___________________________________ ->
                    false
            )
          if Seq.isEmpty rts then
            None
          else
            rts
            |> Seq.head
            |> Some
        | otherwise (* just an object value, pass to `helper` *)      ->
          union2schemaTypeHelper otherwise
          |> Option.bind(
            fun nt ->
              seq {
                yield Schema.Ast.Type.NULL
                yield nt
              }
              |> Seq.toArray
              |> Schema.Ast.Type.UNION
              |> Some
          )

    and private union2schemaTypeHelper (o:obj) =
      match o with
        (* NOTE: Exhaustive pattern-matching only to AVRO IDL types *)
        | :?  System.Boolean ->
          Schema.Ast.Type.BOOLEAN
          |> Some
        | :?  System.Int32 ->
          Schema.Ast.Type.INT
          |> Some
        | :?  System.Int64 ->
          Schema.Ast.Type.LONG
          |> Some
        | :?  System.Single ->
          Schema.Ast.Type.FLOAT
          |> Some
        | :?  System.Double ->
          Schema.Ast.Type.DOUBLE
          |> Some
        | :? (System.Byte[]) ->
          Schema.Ast.Type.BYTES
          |> Some
        | :?  System.String ->
          Schema.Ast.Type.STRING
          |> Some
        | :?  System.TimeSpan ->
          Schema.Ast.Type.TIME_MS
          |> Some
        | :?  Avro.AvroDecimal ->
          (* NOTE: To reflect the type, we default to `zero` in `precision`. It
             will not have an impact as this is just to match wit the schema
             type of the `AST`. But `scale` is MANDATORY, otherwise it will
             fail when (de)/serializing AVRO IDL data:
             - https://avro.apache.org/docs/current/api/csharp/html/structAvro_1_1AvroDecimal.html
          *)
          let ad = o :?> Avro.AvroDecimal
          (      0
          , Some ad.Scale
          )
          |> Schema.Ast.Type.DECIMAL
          |> Some
        | :?  Avro.Generic.GenericEnum ->
          let ge = o :?> Avro.Generic.GenericEnum
          (      ge.Schema.Name
          , Some ge.Schema.Namespace
          )
          |> Schema.Ast.Fqdn.FQDN
          |> Schema.Ast.Type.ENUM
          |> Some
        | :?  Avro.Generic.GenericFixed ->
          let gf = o :?> Avro.Generic.GenericFixed
          ( (      gf.Schema.Name
            , Some gf.Schema.Namespace
            )
            |> Schema.Ast.Fqdn.FQDN
          , gf.Value.Length
          )
          |> Schema.Ast.Type.FIXED
          |> Some
        | :?  System.DateTime ->
          let dt = o :?> System.DateTime
          if dt.Equals(dt.Date) then
            Schema.Ast.Type.DATE
            |> Some
          else
            Schema.Ast.Type.TIMESTAMP_MS
            |> Some
        | _ ->
          None
    
    let toFiles (dts:DateTime) (tables:t) path =
      tables
      |> Seq.filter(
        fun t ->
          (* NOTE: Only convert tables with elements to files *)
          0 < t.Value.Count
      )
      |> Seq.iter(
        fun t ->
          let dp =
            Path.Combine
              ( path
              , t.Key.Replace(".", "/")
              )
          let pp =
            Path.Combine
              ( dp
              , dts.Date.ToString("yyyy-MM-dd")
                |> sprintf "pj_pds=%s"
              )
          let fn =
            ( Date.Filename.fromDateTime dts
            , Guid.NewGuid()
            ) 
            ||> sprintf "%s_%A.snappy.parquet"
          let fp =
            Path.Combine
              ( pp
              , fn
              )
          use ms = new MemoryStream ()
          let _ =
            (* Add to ensure .Dispose is called *)
            use ws =
              ParquetWriter.CreateAsync
                ( schema = t.Value.Schema
                , output = ms
                )
              |> Async.AwaitTask
              |> Async.RunSynchronously
            //ws.CompressionMethod <- CompressionMethod.Gzip // 10k -> 57M
            ws.CompressionMethod <- CompressionMethod.Snappy // 10k -> 58M
            ws.WriteAsync(table = t.Value)
            |> Async.AwaitTask
            |> Async.RunSynchronously
          (* 0) Ensure output folder exists *)
          pp
          |> Directory.CreateDirectory
          |> ignore
          (* ?) Store data in a PARQUET file *)
          let bs = ms.ToArray()
          File.WriteAllBytesAsync
            ( path  = fp
            , bytes = bs
            )
          |> Async.AwaitTask
          |> Async.RunSynchronously
          ms.Flush()
          (* ?) Create `_delta_log` control file *)
        
          Databricks.Delta.JSONL.init
            ( dts )
            ( bs.LongLength )
            ( t.Value.Schema.GetDataFields()
              |> Seq.map(
                fun (f) ->
                  ( f.Name
                  , f.DataType.ToString().ToLowerInvariant()
                  , f.HasNulls
                  , f.IsArray
                  )
              )
              |> Databricks.Delta.JSONL.Schema.init
            )
            ( fn )
          |> Databricks.Delta.toFiles 0 dp
      )
    
    let toBytes (dts:DateTime) (tables:t) =
      let dict = Bytes.empty ()
      tables
      |> Seq.filter(
        fun t ->
          (* NOTE: Only convert tables with elements to files *)
          0 < t.Value.Count
      )
      |> Seq.sortBy (fun kv -> kv.Key)
      |> Seq.iter(
        fun t ->
          let fn =
            ( Date.Filename.fromDateTime dts
            , Guid.NewGuid()
            ) 
            ||> sprintf "%s_%A.snappy.parquet"
          use ms = new MemoryStream ()
          let _ =
            (* Add to ensure .Dispose is called *)
            use ws =
              ParquetWriter.CreateAsync
                ( schema = t.Value.Schema
                , output = ms
                )
              |> Async.AwaitTask
              |> Async.RunSynchronously
            //ws.CompressionMethod <- CompressionMethod.Gzip // 10k -> 57M
            ws.CompressionMethod <- CompressionMethod.Snappy // 10k -> 58M
            ws.WriteAsync(table = t.Value)
            |> Async.AwaitTask
            |> Async.RunSynchronously
          dict.[fn] <- ms.ToArray()
          ms.Flush()
      )
      dict
