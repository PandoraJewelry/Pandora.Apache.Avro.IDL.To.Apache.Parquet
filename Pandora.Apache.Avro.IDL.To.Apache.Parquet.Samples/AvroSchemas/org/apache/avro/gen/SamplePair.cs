// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen, version 1.11.1
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace org.apache.avro.gen
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using global::Avro;
	using global::Avro.Specific;
	
	[global::System.CodeDom.Compiler.GeneratedCodeAttribute("avrogen", "1.11.1")]
	public partial class SamplePair : global::Avro.Specific.ISpecificRecord
	{
		public static global::Avro.Schema _SCHEMA = global::Avro.Schema.Parse(@"{""type"":""record"",""name"":""SamplePair"",""namespace"":""org.apache.avro.gen"",""fields"":[{""name"":""method"",""type"":{""type"":""record"",""name"":""Method"",""namespace"":""org.apache.avro.gen"",""fields"":[{""name"":""declaringClass"",""type"":{""type"":""string"",""testAttribute"":""testValue""}},{""name"":""methodName"",""type"":""string""}]}},{""name"":""node"",""type"":{""type"":""record"",""name"":""SampleNode"",""namespace"":""org.apache.avro.gen"",""fields"":[{""name"":""count"",""default"":0,""type"":""int""},{""name"":""subNodes"",""type"":{""type"":""array"",""items"":""SamplePair""}}]}}]}");
		private org.apache.avro.gen.Method _method;
		private org.apache.avro.gen.SampleNode _node;
		public virtual global::Avro.Schema Schema
		{
			get
			{
				return SamplePair._SCHEMA;
			}
		}
		public org.apache.avro.gen.Method method
		{
			get
			{
				return this._method;
			}
			set
			{
				this._method = value;
			}
		}
		public org.apache.avro.gen.SampleNode node
		{
			get
			{
				return this._node;
			}
			set
			{
				this._node = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
			case 0: return this.method;
			case 1: return this.node;
			default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
			case 0: this.method = (org.apache.avro.gen.Method)fieldValue; break;
			case 1: this.node = (org.apache.avro.gen.SampleNode)fieldValue; break;
			default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
