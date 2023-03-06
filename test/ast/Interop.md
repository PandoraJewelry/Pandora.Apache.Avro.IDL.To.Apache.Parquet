# AST
## FQDN ("Foo", Some "org.apache.avro")
* org.apache.avro.label: STRING
## FQDN ("Interop", Some "org.apache.avro")
* org.apache.avro.boolField: BOOLEAN
* org.apache.avro.doubleField: DOUBLE
* org.apache.avro.enumField: ENUM (FQDN ("Kind", Some "org.apache.avro"))
* org.apache.avro.fixedField: FIXED (FQDN ("MD5", Some "org.apache.avro"), 16)
* org.apache.avro.floatField: FLOAT
* org.apache.avro.intField: INT
* org.apache.avro.longField: LONG
* org.apache.avro.mapField: RECORD (FQDN ("InteropMapField", Some "org.apache.avro"), Some MAP)
* org.apache.avro.nullField: NULL
* org.apache.avro.recordField: RECORD (FQDN ("Node", Some "org.apache.avro"), None)
* org.apache.avro.stringField: STRING
* org.apache.avro.unionField: RECORD (FQDN ("InteropUnionField", Some "org.apache.avro"), Some (UNION 3))
## FQDN ("InteropMapField", Some "org.apache.avro")
* org.apache.avro.key: STRING
* org.apache.avro.value: RECORD (FQDN ("Foo", Some "org.apache.avro"), None)
## FQDN ("InteropUnionField", Some "org.apache.avro")
* org.apache.avro.type0: UNION [|NULL; BOOLEAN|]
* org.apache.avro.type1: UNION [|NULL; DOUBLE|]
* org.apache.avro.type2: RECORD (FQDN ("InteropUnionFieldType2", Some "org.apache.avro"), Some ARRAY)
## FQDN ("InteropUnionFieldType2", Some "org.apache.avro")
* org.apache.avro.item: BYTES
## FQDN ("Node", Some "org.apache.avro")
* org.apache.avro.children: RECORD (FQDN ("Node", Some "org.apache.avro"), Some ARRAY)
* org.apache.avro.label: STRING
