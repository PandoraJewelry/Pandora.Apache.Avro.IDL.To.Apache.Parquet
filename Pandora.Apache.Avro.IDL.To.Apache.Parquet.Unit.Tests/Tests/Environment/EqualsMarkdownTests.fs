namespace Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests.Tests.Environment

open System
open System.IO

open Xunit

open Microsoft.Extensions.Logging

open Newtonsoft.Json.Linq

open Pandora.Apache
open Pandora.Utils

type EqualsMarkdownTests () =
  
  let logger =
    let lf = new LoggerFactory ()
    lf.CreateLogger ()

  let envHelper name =
    let env = Parquet.Schema.Ast.Environment.empty ()
    let ast = Parquet.Schema.Ast.empty ()    
    Path.Combine
      ( __SOURCE_DIRECTORY__
      , @"../../../avro/avsc/"
      , sprintf "%s.avsc" name
      )
    |> File.ReadAllText
    |> JToken.Parse
    |> Avro.Schema.toParquetSchema logger None env ast
    |> fun (env', _, es) ->
      let smd =
        File.ReadAllLines
          ( path =
              Path.Combine
                ( __SOURCE_DIRECTORY__
                , @"../../../test/env/"
                , sprintf "%s.md" name
                )
          , encoding = UTF8.noBOM
          )
      let dmd =
        ( seq {
            yield "# Environment"
            yield!
              ( env'
                |> Seq.sortBy (fun kv -> kv.Key)
                |> Seq.map (
                  fun kv ->
                    seq {
                      yield
                        ( kv.Key
                          |> sprintf "* KEY: %s"
                        )
                      yield
                        ( String.Join
                            ( " "
                            , (sprintf "%A" kv.Value)
                                .Replace(Environment.NewLine, String.Empty)
                                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                            )
                          |> sprintf "* VAL: %s"
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
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Bar () =
    envHelper "Bar"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Foo () =
    envHelper "Foo"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Interop () =
    envHelper "Interop"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Method () =
    envHelper "Method"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Node () =
    envHelper "Node"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SampleNode () =
    envHelper "SampleNode"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SamplePair () =
    envHelper "SamplePair"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SelfRef () =
    envHelper "SelfRef"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.TestError () =
    envHelper "TestError"
  
  [<Fact>]
  [<Trait
      ( "Environment"
      , "EqualsMarkdownTests"
      )
   >]
  member __.TestRecord () =
    envHelper "TestRecord"
