// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

val schema=new Schema(Types.NestedField.required(1,"id",Types.IntegerType.get()),Types.NestedField.optional(2,"st",Types.StructType.of(Types.NestedField.optional(3,"v",Types.VariantType.get()))))
val t=VariantFixture.create(schema)
val st=GenericRecord.create(schema.findType("st").asStructType());st.setField("v",VariantFixture.variant(Variants.of(42)))
VariantFixture.write(t,Seq(Seq[Any](1,st)))
VariantFixture.remember(t)
println("IRU9_NESTED_OK")
