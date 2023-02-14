namespace Pandora.Apache.Avro.IDL.To.Apache.Parquet.Unit.Tests.Tests.Ast

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

  let astHelper name =
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
    |> fun (_, ast', es) ->
      let smd =
        File.ReadAllLines
          ( path =
              Path.Combine
                ( __SOURCE_DIRECTORY__
                , @"../../../test/ast/"
                , sprintf "%s.md" name
                )
          , encoding = UTF8.noBOM
          )
      let dmd =
        ( seq {
            yield "# AST"
            yield!
              ( ast'
                |> Seq.sortBy (fun ts -> ts.Key)
                |> Seq.map (
                  fun ts ->
                    seq {
                      yield
                        ( String.Join
                            ( " "
                            , (sprintf "## %A" ts.Key)
                                .Replace(Environment.NewLine, String.Empty)
                                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                            )
                        )
                      yield!
                        ( ts.Value
                          |> Seq.sortBy (fun fs -> fs.Key)
                          |> Seq.map(
                            fun fs ->
                              ( String.Join
                                  ( " "
                                  , (sprintf "%A" fs.Value)
                                      .Replace(Environment.NewLine, String.Empty)
                                      .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                                  )
                                |> sprintf "* %s: %s" fs.Key
                              )
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
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Bar () =
    astHelper "Bar"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Foo () =
    astHelper "Foo"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Interop () =
    astHelper "Interop"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Method () =
    astHelper "Method"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.Node () =
    astHelper "Node"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SampleNode () =
    astHelper "SampleNode"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SamplePair () =
    astHelper "SamplePair"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.SelfRef () =
    astHelper "SelfRef"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.TestError () =
    astHelper "TestError"
  
  [<Fact>]
  [<Trait
      ( "Ast"
      , "EqualsMarkdownTests"
      )
   >]
  member __.TestRecord () =
    astHelper "TestRecord"
