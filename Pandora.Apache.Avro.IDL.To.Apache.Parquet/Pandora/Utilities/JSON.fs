namespace Pandora.Utilities

[<RequireQualifiedAccess>]
module JSON =
    
  open Newtonsoft.Json
  open Newtonsoft.Json.Converters
  open Newtonsoft.Json.Serialization
  
  let serialize : bool -> bool -> 'a -> string =
    fun indented enum2str value ->
      let settings = new JsonSerializerSettings()
      settings.DefaultValueHandling <- DefaultValueHandling.Include
      if enum2str then
        settings.Converters         <- [| new StringEnumConverter() |]
      JsonConvert.SerializeObject
        ( value
        , if indented then
            Formatting.Indented
          else
            Formatting.None
        , settings
        )
  
  let deserialize<'a> : string -> 'a =
    fun json ->
      try
        JsonConvert.DeserializeObject<'a>(json)
      with ex ->
        failwith ex.Message
