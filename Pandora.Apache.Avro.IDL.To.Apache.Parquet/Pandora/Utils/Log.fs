namespace Pandora.Utils

[<RequireQualifiedAccess>]
module Log =
  
  open System
  
  open Microsoft.Extensions.Logging
  
  let private helper (logger:ILogger) (level:LogLevel) msg (oex:System.Exception option) =
    let l =
      sprintf "%A" level
    let m =
      sprintf "%s | net.pandora.avroidl2parquet | %s | %s"
        ( Date.timestamp 0 )
        ( l.PadRight
            ( 011
            , ' '
            )
        )
        ( msg )
    match level with
      (* LogLevel Enum > Fields:
         - https://learn.microsoft.com/en-us/dotnet/api/microsoft.extensions.logging.loglevel
      *)
      | LogLevel.Error       ->
        match oex with
          | None    -> ()
          | Some ex -> 
            let st = ex.StackTrace
            let em =
              sprintf "%s%s%s"
                ( m )
                ( Environment.NewLine )
                ( st )
            logger.LogError
              ( ``exception`` = ex
              , message       = em
              )
      | LogLevel.Information ->
        logger.LogInformation
          ( message = m
          )
      | LogLevel.Warning     ->
        logger.LogWarning
          ( message = m
          )
      | ____________________ ->
        ()
  
  let info (logger:ILogger) msg =
    helper logger LogLevel.Information msg None
  
  let warn (logger:ILogger) msg =
    helper logger LogLevel.Warning msg None
  
  let error (logger:ILogger) (ex:System.Exception) msg =
    helper logger LogLevel.Error msg (Some ex)
