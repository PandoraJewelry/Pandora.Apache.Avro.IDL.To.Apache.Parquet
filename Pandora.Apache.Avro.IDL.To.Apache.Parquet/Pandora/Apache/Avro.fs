namespace Pandora.Apache

[<RequireQualifiedAccess>]
module Avro =
  
  [<RequireQualifiedAccess>]
  module Schema =
    
    open System
  
    open Microsoft.Extensions.Logging
    
    open Newtonsoft.Json.Linq
    
    open Pandora.Helpers
    open Pandora.Utils
    
    type env = Parquet.Schema.Ast.Environment.t
    type ast = Parquet.Schema.Ast.t
    
    let private toPascalCase (str:string) =
      match String.length str with
        | 0 -> String.Empty
        | 1 -> str.ToUpperInvariant()
        | _ -> str.[0..0].ToUpperInvariant() + str.[1..]
    
    let rec toParquetSchema (logger:ILogger) ons (env:env) (ast:ast) (t:JToken) =
      try
        match t.Type with
          | JTokenType.Object when JSON.memberIsArray  "type" (t :?> JObject) ->
            let o    = t :?> JObject
            let ons' =
              match JSON.memberStrValue "namespace" o with
                | Some ns -> Some ns
                | None    -> ons
            let (env', ast', errs) =
              JSON.memberTokValues "type" o
              |> Seq.fold (
               fun (e, a, es) x ->
                 let (e', a', es') = toParquetSchema logger ons' e a x
                 ( e'
                 , a'
                 , seq {
                     yield! es
                     yield! es'
                   }
                 )
              ) (env, ast, Seq.empty)
            ( env'
            , ast'
            , errs
            )
          | JTokenType.Object when JSON.memberIsNested "type" (t :?> JObject) ->
            let o    = t :?> JObject
            let ons' =
              match JSON.memberStrValue "namespace" o with
                | Some ns -> Some ns
                | None    -> ons
            o.["type"]
            |> toParquetSchema logger ons' env ast
          | JTokenType.Object when JSON.memberIsString "type" (t :?> JObject) ->
            let o    = t :?> JObject
            let ons' =
              match JSON.memberStrValue "namespace" o with
                | Some ns -> Some ns
                | None    -> ons
            let rft = Parquet.Schema.Ast.Field.toType ons' env ast t
            if (function | Result.Error _ -> true | _ -> false) rft then
              ( env
              , ast
              , seq {
                  yield
                    ( sprintf "toParquetSchema > Object when JSON.memberIsString: %A" (rft)
                    )
                }
              )
            else
              match rft with
                | Result.Ok (Parquet.Schema.Ast.Type.ARRAY _) ->
                  o.["items"]
                  |> toParquetSchema logger ons' env ast
                | Result.Ok (Parquet.Schema.Ast.Type.MAP   _) ->
                  o.["values"]
                  |> toParquetSchema logger ons' env ast
                | Result.Ok (Parquet.Schema.Ast.Type.ENUM   fqdn    as ft)
                | Result.Ok (Parquet.Schema.Ast.Type.FIXED (fqdn,_) as ft) ->
                  let fqdn' =
                    fqdn
                    |> Parquet.Schema.Ast.Fqdn.toString
                  
                  if not (env.ContainsKey fqdn') then
                    env.[fqdn'] <- ft
                  
                  ( env
                  , ast
                  , Seq.empty
                  )
                | Result.Ok ((Parquet.Schema.Ast.Type.RECORD (fqdn,_)) as ft)
                | Result.Ok ((Parquet.Schema.Ast.Type.ERROR   fqdn)    as ft)  ->
                  let fqdn' =
                    fqdn
                    |> Parquet.Schema.Ast.Fqdn.toString
                  
                  if not (env.ContainsKey fqdn') then
                    env.[fqdn'] <- ft
                  
                  let (xs, ys, zs) = common logger ons' env ast o
                  ( xs
                  , ys
                  , seq {
                      yield! zs
                    }
                  )
                | ________________________________________________________ ->
                  ( env
                  , ast
                  , Seq.empty
                  )
          | JTokenType.String ->
            match Parquet.Schema.Ast.Field.toType ons env ast t with
              | Result.Ok _ ->
                  ( env
                  , ast
                  , Seq.empty
                  )
              | Result.Error es ->
                  ( env
                  , ast
                  , seq { 
                      yield! es
                    }
                  )
          | otherwise ->
            ( env
            , ast
            , seq {
                yield
                  ( sprintf "Apache.Avro.Schema.toParquetSchema > otherwise: %A" otherwise
                  )
              }
            )
      with ex ->
        "Unexpected error at Apache.Avro.Schema.toParquetSchema"
        |> Log.error logger ex
        raise ex
    
    and private common (logger:ILogger) ons (env:env) (ast:ast) (o:JObject) =
      try
        let ons' =
          match JSON.memberStrValue "namespace" o with
            | Some ns -> Some ns
            | None    -> ons
        match JSON.memberStrValue "name" o with
          | Some uid ->
            let fqdn =
              Parquet.Schema.Ast.Fqdn.FQDN
                ( uid
                , ons'
                )
            
            if not (ast.ContainsKey fqdn) then
              ast.[fqdn] <- Parquet.Schema.Ast.Table.empty ()
            
            let (env', ast', errs) =
              JSON.memberTokValues "fields" o
              |> Seq.fold (
               fun (e, a, es) o ->
                 let (e', a', es') = field logger fqdn ons' env a (o :?> JObject)
                 ( e'
                 , a'
                 , seq {
                     yield! es
                     yield! es'
                   }
                 )
              ) (env, ast, Seq.empty)
            
            ( env'
            , ast'
            , errs
            )
          | None     ->
            ( env
            , ast
            , seq {
                yield
                  ( sprintf "Apache.Avro.Schema.common > missing uid"
                  )
              }
            )
      with ex ->
        "Unexpected error at Apache.Avro.Schema.common"
        |> Log.error logger ex
        raise ex
    
    and private field (logger:ILogger) pid ons (env:env) (ast:ast) (t:JToken) =
      try
        let o    = t :?> JObject (* All fields are JObject *)
        let ons' =
          match JSON.memberStrValue "namespace" o with
            | Some ns -> Some ns
            | None    -> ons
        let rft = Parquet.Schema.Ast.Field.toType ons' env ast t
        let (o', rft') =
          (* Tranform complex fields to records:
             - array of 't     => record {               item  : 't                    }
             - map   of 't     => record { key : string, value : 't                    }
             - union of 't seq => record {               type0 : 't_0, …, typeN : 't_n }
             
             > NOTE: An array of records, should be flattened to a record.
             > NOTE: All fields in a union MUST be `nullable`, except for (complex) nested types.
          *)
          match rft with
            | Result.Ok (Parquet.Schema.Ast.Type.ARRAY (Parquet.Schema.Ast.Type.RECORD (fqdn,_))) ->
              (* Tranform field from
                 ```json
                 {
                   "name" : "arrayField",
                   "type" :
                   {
                       "type"  : "array",
                       "items" :
                       {
                         "type"   : "record",
                         "name"   : "RecordType",
                         "fields" :
                           [
                             {
                               "name" : "label",
                               "type" : "string"
                             }
                           ]
                       }
                   }
                 }
                 ```
                 to:
                 ```json
                 {
                   "name" : "arrayField",
                   "type" :
                   {
                     "type"   : "record",
                     "name"   : "RecordType",
                     "fields" :
                       [
                         {
                           "name" : "label",
                           "type" : "string"
                         }
                       ]
                   }
                 }
                 ```
              *)
              let fn =
                match JSON.memberStrValue "name" o with
                  | Some str -> str
                  | None     -> String.Empty
              let ar2r =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , fn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , o.["type"].["items"]
                            )
                        )
                    }
                  )
              ( ar2r
              , Parquet.Schema.Ast.Type.RECORD
                  ( fqdn
                  , Some Parquet.Schema.Ast.Type.Transformation.ARRAY
                  )
                |> Result.Ok
              )
            | Result.Ok (Parquet.Schema.Ast.Type.ARRAY _) ->
              (* Tranform field from
                 ```json
                 {
                   "name" : "arrayField",
                   "type" : {
                     "type"  : "array",
                     "items" : "string"
                   }
                 }
                 ```
                 to:
                 ```json
                 {
                   "name" : "arrayField",
                   "type" : 
                   {
                     "type"   : "record",
                     "name"   : "PARENT_RECORD_NAME_ArrayField"
                     "fields" :
                     [
                       {
                         "name" : "item",
                         "type" : "string"
                       }
                     ]
                   }
                 }
                 ```
              *)
              let fn =
                match JSON.memberStrValue "name" o with
                  | Some str -> str
                  | None     -> String.Empty
              let rn =
                pid
                |> Parquet.Schema.Ast.Fqdn.name
              let cn =
                match JSON.memberStrValue "name" o with
                  | Some str -> toPascalCase str
                  | None     -> String.Empty
              let vf =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , "item"
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , o.["type"].["items"]
                            )
                        )
                    }
                  ) :> JToken
              let fs =
                new JArray
                  ( seq {
                      yield vf
                    }
                  ) :> JToken
              let rt =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "type"
                            , "record"
                            )
                        )
                      yield
                        ( new JProperty
                            ( "name"
                            , rn + cn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "fields"
                            , fs
                            )
                        )
                    }
                  )
              let a2r =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , fn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , rt
                            )
                        )
                    }
                  )
              ( a2r
              , Parquet.Schema.Ast.Type.RECORD
                  ( Parquet.Schema.Ast.Fqdn.FQDN
                      ( rn + cn
                      , ons'
                      )
                  , Some Parquet.Schema.Ast.Type.Transformation.ARRAY
                  )
                |> Result.Ok
              )
            | Result.Ok (Parquet.Schema.Ast.Type.MAP   _) ->
              (* Tranform field from
                 ```json
                 {
                   "name" : "mapField",
                   "type" :
                   {
                     "type"   : "map",
                     "values" : "string"
                   }
                 }
                 ```
                 to:
                 ```json
                 {
                   "name" : "mapField",
                   "type" : 
                   {
                     "type"   : "record",
                     "name"   : "PARENT_RECORD_NAME_MapField"
                     "fields" :
                     [
                       {
                         "name" : "key",
                         "type" : "string"
                       },
                       {
                         "name" : "value",
                         "type" : "string"
                       }
                     ]
                   }
                 }
                 ```
              *)
              let fn =
                match JSON.memberStrValue "name" o with
                  | Some str -> str
                  | None     -> String.Empty
              let rn =
                pid
                |> Parquet.Schema.Ast.Fqdn.name
              let cn =
                match JSON.memberStrValue "name" o with
                  | Some str -> toPascalCase str
                  | None     -> String.Empty
              let kf =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , "key"
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , "string"
                            )
                        )
                    }
                  ) :> JToken
              let vf =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , "value"
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , o.["type"].["values"]
                            )
                        )
                    }
                  ) :> JToken
              let fs =
                new JArray
                  ( seq {
                      yield kf
                      yield vf
                    }
                  ) :> JToken
              let rt =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "type"
                            , "record"
                            )
                        )
                      yield
                        ( new JProperty
                            ( "name"
                            , rn + cn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "fields"
                            , fs
                            )
                        )
                    }
                  )
              let m2r =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , fn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , rt
                            )
                        )
                    }
                  )
              ( m2r
              , Parquet.Schema.Ast.Type.RECORD
                  ( Parquet.Schema.Ast.Fqdn.FQDN
                      ( rn + cn
                      , ons'
                      )
                  , Some Parquet.Schema.Ast.Type.Transformation.MAP
                  )
                |> Result.Ok
              )
            | Result.Ok (Parquet.Schema.Ast.Type.UNION _ as ft) when (Parquet.Tables.isNullableRecord ft) ->
              (* Tranform field from
                 ```json
                 {
                   "name" : "unionField",
                   "type" :
                     [ "null",
                       {
                         "type"   : "record",
                         "name"   : "RecordType",
                         "fields" :
                           [
                             {
                               "name" : "label",
                               "type" : "string"
                             }
                           ]
                       }
                     ]
                 }
                 ```
                 to:
                 ```json
                 {
                   "name" : "unionField",
                   "type" :
                     {
                       "type"   : "record",
                       "name"   : "RecordType",
                       "fields" :
                         [
                           {
                             "name" : "label",
                             "type" : "string"
                           }
                         ]
                     }
                 }
                 ```
              *)
              let fn =
                match JSON.memberStrValue "name" o with
                  | Some str -> str
                  | None     -> String.Empty
              let (fqdn, nt) =
                (o.["type"] :?> JArray)
                (* NOTE: Ensure no `null` type are present *)
                |> Seq.filter(
                  fun nt ->
                    not (Result.Ok Parquet.Schema.Ast.Type.NULL = Parquet.Schema.Ast.Field.toType ons' env ast nt)
                )
                |> Seq.map(
                  fun nt ->
                    let rft = Parquet.Schema.Ast.Field.toType ons' env ast nt
                    match rft with
                      | Result.Ok (Parquet.Schema.Ast.Type.RECORD (fqdn,_)) ->
                        Some (fqdn,nt)
                      | _ -> None
                )
                |> Seq.choose id
                |> Seq.head
              let nr2r =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , fn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , nt
                            )
                        )
                    }
                  )
              ( nr2r
              , Parquet.Schema.Ast.Type.RECORD
                  ( fqdn
                  , Some Parquet.Schema.Ast.Type.Transformation.NULLABLE
                  )
                |> Result.Ok
              )
            | Result.Ok (Parquet.Schema.Ast.Type.UNION _ as ft) when not (Parquet.Tables.isPrimitive ft) ->
              (* Tranform field from
                 ```json
                 {
                   "name" : "unionField",
                   "type" :
                     [ "boolean",
                       "double",
                       {
                         "type"  : "array",
                         "items" : "bytes"
                       }
                     ]
                 }
                 ```
                 to:
                 ```json
                 {
                   "name" : "unionField",
                   "type" :
                   {
                     "type"   : "record",
                     "name"   : "PARENT_RECORD_NAME_UnionField"
                     "fields" :
                     [
                       {
                         "name" : "type0",
                         "type" : [ "null", "boolean" ]
                       },
                       {
                         "name" : "type1",
                         "type" : [ "null", "double" ]
                       },
                       {
                         "name" : "type2",
                         "type" : 
                           {
                             "type"  : "array",
                             "items" : "bytes"
                           }
                       }
                     ]
                   }
                 }
                 ```
                 NOTE: All fields MUST be NULLABLE, except (complex) nested types.
              *)
              let fn =
                match JSON.memberStrValue "name" o with
                  | Some str -> str
                  | None     -> String.Empty
              let rn =
                pid
                |> Parquet.Schema.Ast.Fqdn.name
              let cn =
                match JSON.memberStrValue "name" o with
                  | Some str -> toPascalCase str
                  | None     -> String.Empty
              let ts =
                (o.["type"] :?> JArray)
                (* NOTE: Ensure no `null` type are present *)
                |> Seq.filter(
                  fun nt ->
                    not (Result.Ok Parquet.Schema.Ast.Type.NULL = Parquet.Schema.Ast.Field.toType ons' env ast nt)
                )
                (* NOTE: We don't need to check if a specific type appears multiple times. Not permitted by AVRO *)
              let ps =
                ts
                |> Seq.mapi(
                  fun i nt ->
                    let rft = Parquet.Schema.Ast.Field.toType ons' env ast nt
                    let ps =
                      seq {
                        match rft with
                          | Result.Ok nft ->
                            yield
                              ( new JProperty
                                  ( "name"
                                  , sprintf "type%i" i
                                  )
                              )
                            yield
                              ( if Parquet.Tables.isPrimitive nft then
                                  new JProperty
                                    ( "type"
                                    , new JArray
                                        ( seq {
                                            yield (JValue "null" :> JToken)
                                            yield  nt
                                          }
                                        )
                                    )
                                else
                                  new JProperty
                                    ( "type"
                                    , nt
                                    )
                              )
                          | _ ->
                            ()
                      }
                    new JObject(ps)
                )
              let fs =
                new JArray
                  ( seq {
                      yield ps
                    }
                  ) :> JToken
              let rt =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "type"
                            , "record"
                            )
                        )
                      yield
                        ( new JProperty
                            ( "name"
                            , rn + cn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "fields"
                            , fs
                            )
                        )
                    }
                  )
              let u2r =
                new JObject
                  ( seq {
                      yield
                        ( new JProperty
                            ( "name"
                            , fn
                            )
                        )
                      yield
                        ( new JProperty
                            ( "type"
                            , rt
                            )
                        )
                    }
                  )
              ( u2r
              , Parquet.Schema.Ast.Type.RECORD
                  ( Parquet.Schema.Ast.Fqdn.FQDN
                      ( rn + cn
                      , ons'
                      )
                  , ts
                    |> Seq.length
                    |> Parquet.Schema.Ast.Type.Transformation.UNION
                    |> Some 
                  )
                |> Result.Ok
              )
            | _ ->
              ( o
              , rft
              )
        
        match JSON.memberStrValue "name" o', rft' with
          | Some uid, Result.Ok    ft ->
            
            let fqdn =
              Parquet.Schema.Ast.Fqdn.FQDN
                ( uid
                , ons'
                )
              |> Parquet.Schema.Ast.Fqdn.toString
            
            if not (ast.[pid].ContainsKey fqdn) then
              ast.[pid].[fqdn] <- ft
            
            if isReference o' ft then
              ( env
              , ast
              , Seq.empty
              )
            else
              let (xs, ys, zs) = toParquetSchema logger ons' env ast o'
              ( xs
              , ys
              , seq {
                  yield! zs
                }
              )
          | None    , Result.Ok    __ ->
            ( env
            , ast
            , seq {
                yield
                  ( sprintf "field > missing `uid`"
                  )
              }
            )
          | Some ___, Result.Error es ->
            ( env
            , ast
            , seq {
                yield! es
              }
            )
          | _________________________ ->
            ( env
            , ast
            , seq {
                yield
                  ( sprintf "field > missing both `uid` and `type`"
                  )
              }
            )
      with ex ->
        "Unexpected error at Apache.Avro.Schema.field"
        |> Log.error logger ex
        raise ex
    
    and private isReference (o:JObject) = function
      | Parquet.Schema.Ast.Type.RECORD (fqdn,_)
      | Parquet.Schema.Ast.Type.ERROR   fqdn    ->
        fqdn
        |> Parquet.Schema.Ast.Fqdn.toString
        |> Some
        |> (=) (JSON.memberStrValue "type" o)
      | _______________________________________ -> false
  
  [<RequireQualifiedAccess>]
  module Bytes =
    
    open System.IO
    
    open Avro.IO
    open Avro.Specific
    
    [<RequireQualifiedAccess>]
    module Specific =
      
      let serialize<'a when 'a :> ISpecificRecord and 'a : (new : unit -> 'a)>
        :  'a
        -> byte array =
          fun value ->
            use mem = new MemoryStream  (   )
            let enc = new BinaryEncoder (mem)
            let ser = new SpecificDefaultWriter(value.Schema)
            ser.Write(value, enc)
            mem.ToArray()
    
    [<RequireQualifiedAccess>]
    module Generic =
      
      open Avro
      open Avro.Generic
      
      let serialize<'a when 'a :> GenericRecord>
        :  'a
        -> byte array =
          fun value ->
            use mem = new MemoryStream  (   )
            let enc = new BinaryEncoder (mem)
            let ser = new GenericDatumWriter<'a>(value.Schema)
            ser.Write(value, enc)
            mem.ToArray()
      
      let deserialize
        :  string
        -> byte array
        -> GenericRecord =
          fun schema bs ->
            let grs = RecordSchema.Parse (json = schema)
            use mem = new MemoryStream   (bs)
            let dec = new BinaryDecoder  (mem)
            let gen = new GenericRecord  (RecordSchema.Parse(json = schema) :?> RecordSchema)
            let des = new GenericDatumReader<GenericRecord>(grs, grs)
            des.Read (null, dec)
  
  [<RequireQualifiedAccess>]
  module JSON =
    
    [<RequireQualifiedAccess>]
    module Specific =
        
      open System.Reflection
      
      open Newtonsoft.Json
      open Newtonsoft.Json.Converters
      open Newtonsoft.Json.Serialization
      
      type private IgnoreAvroSchemaContractResolver () =
        inherit DefaultContractResolver ()
        override __.CreateProperty
          ( mi : MemberInfo
          , ms : MemberSerialization
          ) =
          let p = base.CreateProperty(mi, ms)
          let t = (mi :?> PropertyInfo).PropertyType
          
          if mi.MemberType = MemberTypes.Property && typedefof<Avro.Schema> = t then
            p.Ignored <- true
          p
      
      let serialize : 'a -> string =
        fun value ->
          let settings = new JsonSerializerSettings()
          settings.DefaultValueHandling <- DefaultValueHandling.Include
          settings.ContractResolver     <- new IgnoreAvroSchemaContractResolver()
          settings.Converters           <- [| new StringEnumConverter() |]
          
          JsonConvert.SerializeObject
            ( value      = value
            , formatting = Formatting.Indented
            , settings   = settings
            )
      
      let deserialize<'a>
        :  string
        -> 'a =
          fun json ->
            try
              JsonConvert.DeserializeObject<'a>(json)
            with ex ->
              failwith ex.Message
