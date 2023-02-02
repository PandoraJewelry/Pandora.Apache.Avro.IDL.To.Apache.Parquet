namespace Pandora.Utilities

[<RequireQualifiedAccess>]
module Date =
  
  open System
  
  let timestamp : _ -> string =
    fun _ ->
      DateTime.UtcNow.ToString("o")
  
  let utcnow : double -> DateTime =
    fun seconds ->
      let ts = DateTime.UtcNow
      seconds
      |> ts.AddSeconds
  
  [<RequireQualifiedAccess>]
  module Filename =
    
    let private mask = "yyyyMMddTHHmmssfffffffZ"
      
    let timestamp : _ -> string =
      fun _ ->
        DateTime.UtcNow.ToString(mask)
    
    let fromDateTime : DateTime -> string =
      fun datetime ->
        datetime.ToUniversalTime().ToString(mask)
  
  [<RequireQualifiedAccess>]
  module Unix =
  
    let epoch : double -> DateTime =
      DateTime.UnixEpoch.AddSeconds
    
    let fromDateTime : DateTime -> double =
      fun datetime ->
        let diff = datetime.ToUniversalTime() - DateTime.UnixEpoch
        diff.TotalSeconds
        |> Math.Round
