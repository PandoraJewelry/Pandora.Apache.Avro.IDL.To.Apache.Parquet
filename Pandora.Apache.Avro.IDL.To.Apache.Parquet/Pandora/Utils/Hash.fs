namespace Pandora.Utils

[<RequireQualifiedAccess>]
module Hash =
  
  [<RequireQualifiedAccess>]
  module SHA256 =
    
    open System.Security.Cryptography
    open System.Text
  
    [<RequireQualifiedAccess>]
    module Bytes =
      
      let toBytes bs =
        use sha = SHA256.Create()
        sha.ComputeHash(buffer = bs)
      
      let toString =
        toBytes
        >> Array.map    (sprintf "%02x")
        >> Array.reduce (sprintf "%s%s")
    
    (* Terminal vs .NET implementation
    
    [T480:~]$ echo -n 'foobar' | sha256sum
    c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2
    
    > Hash.SHA256.sum "foobar";;
    val it: string =
      "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"
    *)
    
    let sum str =
      Encoding.UTF8.GetBytes(s = str)
      |> Bytes.toString
