# AST
## FQDN ("Method", Some "org.apache.avro.gen")
* org.apache.avro.gen.declaringClass: STRING
* org.apache.avro.gen.methodName: STRING
## FQDN ("SampleNode", Some "org.apache.avro.gen")
* org.apache.avro.gen.count: INT
* org.apache.avro.gen.subNodes: RECORD (FQDN ("SamplePair", Some "org.apache.avro.gen"), Some ARRAY)
## FQDN ("SamplePair", Some "org.apache.avro.gen")
* org.apache.avro.gen.method: RECORD (FQDN ("Method", Some "org.apache.avro.gen"), None)
* org.apache.avro.gen.node: RECORD (FQDN ("SampleNode", Some "org.apache.avro.gen"), None)
