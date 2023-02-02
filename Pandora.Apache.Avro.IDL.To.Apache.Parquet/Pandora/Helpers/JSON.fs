namespace Pandora.Helpers

[<RequireQualifiedAccess>]
module JSON =
  
  open Newtonsoft.Json.Linq
  
  // JTokenType Enumeration:
  // - https://www.newtonsoft.com/json/help/html/T_Newtonsoft_Json_Linq_JTokenType.htm
  let memberIsNested label (o:JObject) =
    o.ContainsKey label &&
    match o.[label].Type with
      | JTokenType.None        -> (*  0 No token type has been set. *)
        false
      | JTokenType.Object      -> (*  1 A JSON object. *)
        true
      | JTokenType.Array          (*  2 A JSON array. *)
      | JTokenType.Constructor    (*  3 A JSON constructor. *)
      | JTokenType.Property       (*  4 A JSON object property. *)
      | JTokenType.Comment        (*  5 A comment. *)
      | JTokenType.Integer        (*  6 An integer value. *)
      | JTokenType.Float          (*  7 A float value. *)
      | JTokenType.String         (*  8 A string value. *)
      | JTokenType.Boolean        (*  9 A boolean value. *)
      | JTokenType.Null           (* 10 A null value. *)
      | JTokenType.Undefined      (* 11 An undefined value. *)
      | JTokenType.Date           (* 12 A date value. *)
      | JTokenType.Raw            (* 13 A raw JSON value. *)
      | JTokenType.Bytes          (* 14 A collection of bytes value. *)
      | JTokenType.Guid           (* 15 A Guid value. *)
      | JTokenType.Uri            (* 16 A Uri value. *)
      | JTokenType.TimeSpan    -> (* 17 A TimeSpan value.  *)
        false
      | ______________________ ->
        false
  
  let memberIsString label (o:JObject) =
    o.ContainsKey label &&
    match o.[label].Type with
      | JTokenType.None           (*  0 No token type has been set. *)
      | JTokenType.Object         (*  1 A JSON object. *)
      | JTokenType.Array          (*  2 A JSON array. *)
      | JTokenType.Constructor    (*  3 A JSON constructor. *)
      | JTokenType.Property       (*  4 A JSON object property. *)
      | JTokenType.Comment        (*  5 A comment. *)
      | JTokenType.Integer        (*  6 An integer value. *)
      | JTokenType.Float       -> (*  7 A float value. *)
        false
      | JTokenType.String      -> (*  8 A string value. *)
        true
      | JTokenType.Boolean        (*  9 A boolean value. *)
      | JTokenType.Null           (* 10 A null value. *)
      | JTokenType.Undefined      (* 11 An undefined value. *)
      | JTokenType.Date           (* 12 A date value. *)
      | JTokenType.Raw            (* 13 A raw JSON value. *)
      | JTokenType.Bytes          (* 14 A collection of bytes value. *)
      | JTokenType.Guid           (* 15 A Guid value. *)
      | JTokenType.Uri            (* 16 A Uri value. *)
      | JTokenType.TimeSpan    -> (* 17 A TimeSpan value.  *)
        false
      | ______________________ ->
        false
  
  let memberIsArray label (o:JObject) =
    o.ContainsKey label &&
    match o.[label].Type with
      | JTokenType.None           (*  0 No token type has been set. *)
      | JTokenType.Object      -> (*  1 A JSON object. *)
        false
      | JTokenType.Array       -> (*  2 A JSON array. *)
        true
      | JTokenType.Constructor    (*  3 A JSON constructor. *)
      | JTokenType.Property       (*  4 A JSON object property. *)
      | JTokenType.Comment        (*  5 A comment. *)
      | JTokenType.Integer        (*  6 An integer value. *)
      | JTokenType.Float          (*  7 A float value. *)
      | JTokenType.String         (*  8 A string value. *)
      | JTokenType.Boolean        (*  9 A boolean value. *)
      | JTokenType.Null           (* 10 A null value. *)
      | JTokenType.Undefined      (* 11 An undefined value. *)
      | JTokenType.Date           (* 12 A date value. *)
      | JTokenType.Raw            (* 13 A raw JSON value. *)
      | JTokenType.Bytes          (* 14 A collection of bytes value. *)
      | JTokenType.Guid           (* 15 A Guid value. *)
      | JTokenType.Uri            (* 16 A Uri value. *)
      | JTokenType.TimeSpan    -> (* 17 A TimeSpan value.  *)
        false
      | ______________________ ->
        false
  
  let memberStrValue label (o:JObject) =
    if o.ContainsKey label then
      match o.[label] with
        | :? JValue as token ->
          match token.Value with
            | :? string as str -> Some str
            | ________________ -> None
        | __________________ -> None
    else
      None
  
  let memberIntValue label (o:JObject) =
    if o.ContainsKey label then
      match o.[label] with
        | :? JValue as token ->
          match token.Value with
            | :? int   as n -> Some      n
            | :? int64 as n -> Some (int n)
            | _____________ -> None
        | __________________ -> None
    else
      None
  
  let memberTokValues label (o:JObject) =
    if o.ContainsKey label then
      match o.[label] with
        | :? JArray as xs -> Seq.map id xs
        | _______________ -> Seq.empty
    else
      Seq.empty
