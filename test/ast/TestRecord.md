# AST
## FQDN ("TestRecord", Some "org.apache.avro.test")
* org.apache.avro.test.a: RECORD (FQDN ("TestRecordA", Some "org.apache.avro.test"), Some ARRAY)
* org.apache.avro.test.average: FLOAT
* org.apache.avro.test.hash: FIXED (FQDN ("MD5", Some "org.apache.avro.test"), 16)
* org.apache.avro.test.kind: ENUM (FQDN ("Kind", Some "org.apache.avro.test"))
* org.apache.avro.test.l: LONG
* org.apache.avro.test.name: STRING
* org.apache.avro.test.nullableHash: UNION [|NULL; FIXED (FQDN ("MD5", Some "org.apache.avro.test"), 16)|]
* org.apache.avro.test.prop: UNION [|NULL; STRING|]
* org.apache.avro.test.status: ENUM (FQDN ("Status", Some "org.apache.avro.test"))
* org.apache.avro.test.t: UNION [|TIME_MS; NULL|]
* org.apache.avro.test.value: DOUBLE
## FQDN ("TestRecordA", Some "org.apache.avro.test")
* org.apache.avro.test.item: STRING
