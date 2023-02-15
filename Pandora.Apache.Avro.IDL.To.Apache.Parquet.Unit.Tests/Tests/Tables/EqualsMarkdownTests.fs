namespace Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests.Tests.Tables

open System
open System.Collections.Generic
open System.IO

open Xunit

open Microsoft.Extensions.Logging

open Newtonsoft.Json.Linq

open Pandora.Apache
open Pandora.Utils

open org.apache.avro
open org.apache.avro.gen
open org.apache.avro.ipc.specific
open org.apache.avro.test
open org.foo

type EqualsMarkdownTests () =
  
  let logger =
    let lf = new LoggerFactory ()
    lf.CreateLogger ()
  
  let bar () =
    let f = new org.apache.avro.ipc.specific.Foo ()
    let b = new                              Bar ()
    b.foo <- f
    
    b
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (b.Schema.ToString())
  
  let foo () =
    let f = new org.apache.avro.Foo ()
    f.label <- "foo"
    
    f
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (f.Schema.ToString())
  
  let interop () =
    let f = new org.apache.avro.Foo ()
    f.label <- "foo"
    
    let d = new Dictionary<string, org.apache.avro.Foo> ()
    d.["foo"] <- f
    
    let m = new org.apache.avro.MD5 ()
    m.Value <- Array.init 16 (fun _ -> 0x30uy)
    
    let c = new Node ()
    c.label    <- "children"
    c.children <- [| |]
    
    let n = new Node ()
    n.label    <- "node"
    n.children <- [| c |]
    
    let i = new Interop ()
    i.intField    <- 42 
    i.longField   <- 42L
    i.stringField <- "interop"
    i.boolField   <- true
    i.floatField  <- 0.42f
    i.doubleField <- 0.42
    i.nullField   <- null
    
    i.mapField    <- d
    i.unionField  <- [| "interop"B |] // 42.0 // true //
    i.enumField   <- Kind.A
    i.fixedField  <- m
    i.recordField <- n
    
    i
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (i.Schema.ToString())
  
  let ``method`` () =
    let m = new Method ()
    m.declaringClass <- "class"
    m.methodName     <- "name"
    
    m
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (m.Schema.ToString())
  
  let node () =
    let c = new Node ()
    c.label    <- "children"
    c.children <- [| |]
    
    let n = new Node ()
    n.label    <- "node"
    n.children <- [| c |]
    
    n
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (n.Schema.ToString())
  
  let sampleNode () =
    let m = new Method ()
    m.declaringClass <- "class"
    m.methodName     <- "name"
    
    let s = new SampleNode ()
    s.count    <- 42
    s.subNodes <- [| |]
    
    let p = new SamplePair ()
    p.``method`` <- m
    p.node       <- s
    
    let n = new SampleNode ()
    n.count    <- 42
    n.subNodes <- [| p |]
    
    n
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (n.Schema.ToString())
  
  let samplePair () =
    let m = new Method ()
    m.declaringClass <- "class"
    m.methodName     <- "name"
    
    let n = new SampleNode ()
    n.count    <- 42
    n.subNodes <- [| |]
    
    let p = new SamplePair ()
    p.``method`` <- m
    p.node       <- n
    
    p
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (p.Schema.ToString())
  
  let selfRef () =
    let n = new SelfRef ()
    n.something <- "nested"
    n.subNodes  <- [| |]
    
    let r = new SelfRef ()
    r.something <- "something"
    r.subNodes  <- [| n |]
    
    r
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (r.Schema.ToString())
  
  let testError () =
    let e = new TestError ()
    e.message <- "message"
    
    e
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (e.Schema.ToString())
  
  let testRecord () =
    let m = new org.apache.avro.test.MD5()
    m.Value <- Array.init 16 (fun _ -> 0x30uy)
    
    let t = new TestRecord ()
    
    t.name         <- "name"
    t.kind         <- Kind.BAZ
    t.status       <- Status.A
    t.hash         <- m
    t.nullableHash <- null
    t.value        <- 42.0
    t.average      <- 42.0f
    t.t            <- TimeSpan.Zero
    t.l            <- 42L
    t.a            <- [| "a" |]
    t.prop         <- "prop"
    
    t
    |> Avro.Bytes.Specific.serialize
    |> Avro.Bytes.Generic.deserialize (t.Schema.ToString())
  
  let tabHelper name =
    let env = Parquet.Schema.Ast.Environment.empty ()
    let ast = Parquet.Schema.Ast.empty ()
    
    let dts = DateTime.UtcNow
    let off = new DateTimeOffset(dts)
    
    let tabs = Parquet.Tables.empty logger ast
    
    let gen =
      match name with
        | "Bar"        -> bar        ()
        | "Foo"        -> foo        ()
        | "Interop"    -> interop    ()
        | "Method"     -> ``method`` ()
        | "Node"       -> node       ()
        | "SampleNode" -> sampleNode ()
        | "SamplePair" -> samplePair ()
        | "SelfRef"    -> selfRef    ()
        | "TestError"  -> testError  ()
        | "TestRecord" -> testRecord ()
        | ____________ ->
          failwith "Generated C# classes not available"
    
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
    
    gen.Schema.ToString()
    |> JToken.Parse
    |> Avro.Schema.toParquetSchema logger None env ast
    |> fun (env', ast', es) ->
      ast'
      |> Parquet.Tables.update
          logger
          (Some tabs)
      |> Parquet.Tables.populate
          logger
          off
          (Some sha) None None
          ast'
          gen
          gn (Some gns)
      
      let smd =
        File.ReadAllLines
          ( path =
              Path.Combine
                ( __SOURCE_DIRECTORY__
                , @"../../../test/tab/"
                , sprintf "%s.md" name
                )
          , encoding = UTF8.noBOM
          )
      let dmd =
        ( seq {
            yield "# Tables"
            yield!
              ( tabs
                |> Seq.filter (fun kv -> kv.Value.Count > 0)
                |> Seq.sortBy (fun kv -> kv.Key)
                |> Seq.map (
                  fun kv ->
                    seq {
                      yield
                        ( sprintf "## %s" kv.Key
                        )
                      yield
                        ( "### Fields"
                        )
                      yield!
                        ( kv.Value.Schema.GetDataFields()
                          |> Seq.map(
                            fun df ->
                              ( df.Name
                              , df.DataType.ToString().ToLowerInvariant()
                              , df.HasNulls
                              )
                              |||> sprintf "* %s: %s (nullable = %b)"
                          )
                        )
                    }
                )
                |> Seq.concat
              )
          }
        )
        |> Seq.toArray
      
      Assert.True
        ( Seq.isEmpty es && smd = dmd
        )

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Bar () =
    tabHelper "Bar"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Foo () =
    tabHelper "Foo"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Interop () =
    tabHelper "Interop"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Method () =
    tabHelper "Method"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Node () =
    tabHelper "Node"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SampleNode () =
    tabHelper "SampleNode"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SamplePair () =
    tabHelper "SamplePair"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SelfRef () =
    tabHelper "SelfRef"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.TestError () =
    tabHelper "TestError"

  [<Fact>]
  [<Trait
      ( "Tables"
      , "EqualsMarkdownTests"
      )
   >]
  member __.TestRecord () =
    tabHelper "TestRecord"
