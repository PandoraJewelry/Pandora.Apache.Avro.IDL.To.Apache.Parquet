namespace Pandora.Utils

[<RequireQualifiedAccess>]
module Hash =
  
  open System.Text
  
  [<RequireQualifiedAccess>]
  module SHA256 =
    
    open System.Security.Cryptography
  
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
  
  [<RequireQualifiedAccess>]
  module Klondike =
    
    open System
    
    (* SHA256 vs Klondike hash
    
    Total bits for a SHA256 string are 64 hex chars values times log of the 16
    different hex values divided by log 2, which results in 256 bits:
    
    > 64. * log 16. / log 2.;;
    val it: float = 256.0
    
    The Klondike hash, will only need 43 chars values of the 62 chars below to
    reach an equivalent amount of bits to SHA256:
    
    > 43. * log 62. / log 2.;;         
    val it: float = 256.0304413
    
    Therefore, by using this SHA256 hash transformer, we are not increasing
    the chance of collisions while gaining 22 characters less for each hash.
    
    This approach was suggested by Francisco Blas Izquierdo Riera, from Chalmers
    https://www.chalmers.se/en/persons/blas/, at foss-north 2023.
    
    *)
    
    let private chars =
      [| 'P'; 'A'; 'N'; 'D'; 'O'; 'R'; 'a'
            ; 'b'; 'c'; 'd'; 'e'; 'f'; 'g'; 'h'; 'i'; 'j'; 'k'; 'l'; 'm'
       ; 'n'; 'o'; 'p'; 'q'; 'r'; 's'; 't'; 'u'; 'v'; 'w'; 'x'; 'y'; 'z'
            ; 'B'; 'C'     ; 'E'; 'F'; 'G'; 'H'; 'I'; 'j'; 'K'; 'L'; 'M'
                      ; 'Q'     ; 'S'; 'T'; 'U'; 'V'; 'W'; 'X'; 'Y'; 'Z'
       ; '0'; '1'; '2'; '3'; '4'; '5'; '6'; '7'; '8'; '9';
      |]
    let private tmp = Array.length chars |> double
    let private len = bigint tmp
      
    let private steps bits = log 2. * (bits |> double) / log tmp |> ceil |> int
      
    let rec private helper acc i n =
      if 0 = i then
        acc
        |> List.rev
        |> List.map string
        |> List.fold (+) ""
      else
        helper (chars.[n % len |> int] :: acc) (i - 1) (n / len)
  
    [<RequireQualifiedAccess>]
    module SHA256 =
      
      let fromBytes : byte[] -> string =
           Numerics.BigInteger
        >> Numerics.BigInteger.Abs
        >> helper [] (steps 256)
        
      let sum str =
        Encoding.UTF8.GetBytes(s = str)
        |> SHA256.Bytes.toBytes
        |> fromBytes
