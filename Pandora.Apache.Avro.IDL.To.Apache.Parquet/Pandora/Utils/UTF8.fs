namespace Pandora.Utils

[<RequireQualifiedAccess>]
module UTF8 =
  
  open System.Text
  
  let noBOM = UTF8Encoding false
