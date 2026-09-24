/-
 - Licensed to the Apache Software Foundation (ASF) under one
 - or more contributor license agreements.  See the NOTICE file
 - distributed with this work for additional information
 - regarding copyright ownership.  The ASF licenses this file
 - to you under the Apache License, Version 2.0 (the
 - "License"); you may not use this file except in compliance
 - with the License.  You may obtain a copy of the License at
 -
 -     http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -/
import CassandraTypes.Compatibility

/-!
# Executable conformance checks

Each `example` below is a fact that Cassandra's Java implementation exhibits,
discharged by evaluating the model (`by decide`).  Together they pin the model
to the real behaviour of `org.apache.cassandra.db.marshal`.
-/

namespace CassandraTypes
namespace CQLType

private abbrev ascii : CQLType := native .ascii
private abbrev text : CQLType := native .text
private abbrev blob : CQLType := native .blob
private abbrev int32 : CQLType := native .int32
private abbrev bigint : CQLType := native .bigint
private abbrev varint : CQLType := native .varint
private abbrev timestamp : CQLType := native .timestamp
private abbrev legacyDate : CQLType := native .legacyDate
private abbrev sdate : CQLType := native .date
private abbrev uuid : CQLType := native .uuid
private abbrev timeuuid : CQLType := native .timeuuid
private abbrev duration : CQLType := native .duration
private abbrev float : CQLType := native .float

/-! ## Scalars: comparison-compatibility (`isCompatibleWith`) -/

-- `BytesType` may replace ascii/utf8 (all byte-ordered) and itself, nothing else.
example : isCompatibleWith blob ascii = true := by decide
example : isCompatibleWith blob text = true := by decide
example : isCompatibleWith blob blob = true := by decide
example : isCompatibleWith blob int32 = false := by decide

-- Every ascii string is utf8, but not conversely.
example : isCompatibleWith text ascii = true := by decide
example : isCompatibleWith ascii text = false := by decide
example : isCompatibleWith text blob = false := by decide

-- `DateType` <-> `TimestampType` may be swapped (comparison-compatible).
example : isCompatibleWith legacyDate timestamp = true := by decide
example : isCompatibleWith timestamp legacyDate = true := by decide

-- Distinct numeric types are never comparison-compatible (they sort differently).
example : isCompatibleWith bigint timestamp = false := by decide
example : isCompatibleWith int32 varint = false := by decide

/-! ## Scalars: value-compatibility (`isValueCompatibleWith`) -/

-- `BytesType` can read the bytes of anything; the reverse does not hold.
example : isValueCompatibleWith blob int32 = true := by decide
example : isValueCompatibleWith blob (tuple [int32, text]) = true := by decide
example : isValueCompatibleWith int32 blob = false := by decide

-- `LongType`/`TimestampType`/`DateType` share an 8-byte layout.
example : isValueCompatibleWith bigint timestamp = true := by decide
example : isValueCompatibleWith timestamp bigint = true := by decide
example : isValueCompatibleWith bigint legacyDate = true := by decide

-- `IntegerType` (varint) can read int and bigint layouts.
example : isValueCompatibleWith varint int32 = true := by decide
example : isValueCompatibleWith varint bigint = true := by decide
example : isValueCompatibleWith int32 varint = false := by decide

-- `SimpleDateType` (date) shares int's 4-byte layout; `UUIDType` reads timeuuid.
example : isValueCompatibleWith sdate int32 = true := by decide
example : isValueCompatibleWith uuid timeuuid = true := by decide

-- `DurationType` is value-compatible only with itself.
example : isValueCompatibleWith duration bigint = false := by decide

/-! ## ReversedType -/

example : isCompatibleWith (reversed int32) (reversed int32) = true := by decide
-- A reversed type is compatible only with another reversed type.
example : isCompatibleWith (reversed int32) int32 = false := by decide
example : isCompatibleWith int32 (reversed int32) = false := by decide
-- Value-compatibility strips `ReversedType` from both sides first.
example : isValueCompatibleWith (reversed bigint) (reversed timestamp) = true := by decide
example : isValueCompatibleWith (reversed bigint) timestamp = true := by decide

/-! ## Collections (frozen vs multi-cell) -/

-- Frozen collections are compatible iff their element types are.
example : isCompatibleWith (clist false text) (clist false ascii) = true := by decide
example : isCompatibleWith (clist false ascii) (clist false text) = false := by decide

-- Multi-cell and frozen collections are never mutually compatible.
example : isCompatibleWith (clist true ascii) (clist false ascii) = false := by decide

-- A multi-cell list's cell values only need to be *serialization*-compatible,
-- so `list<blob>` may follow `list<ascii>`.
example : isCompatibleWith (clist true blob) (clist true ascii) = true := by decide

-- Frozen maps: keys must be comparison-compatible, values only value-compatible.
example :
    isValueCompatibleWith (cmap false ascii bigint) (cmap false ascii timestamp) = true := by decide
example :
    isCompatibleWith (cmap false ascii bigint) (cmap false ascii timestamp) = false := by decide

/-! ## Tuples and UDTs -/

-- Extending a tuple with new trailing components is allowed; dropping is not.
example : isCompatibleWith (tuple [int32, text]) (tuple [int32]) = true := by decide
example : isCompatibleWith (tuple [int32]) (tuple [int32, text]) = false := by decide
example : isCompatibleWith (tuple [text]) (tuple [ascii]) = true := by decide
-- Tuple value-compatibility descends via value-compatibility.
example : isValueCompatibleWith (tuple [bigint]) (tuple [timestamp]) = true := by decide
example : isCompatibleWith (tuple [bigint]) (tuple [timestamp]) = false := by decide

private abbrev udtA : CQLType := udt "ks" "point" false ["x"] [int32]
private abbrev udtAB : CQLType := udt "ks" "point" false ["x", "y"] [int32, text]
private abbrev udtOtherKs : CQLType := udt "ks2" "point" false ["x"] [int32]

-- A UDT may gain trailing fields, but the old definition may not have extras.
example : isValueCompatibleWith udtAB udtA = true := by decide
example : isValueCompatibleWith udtA udtAB = false := by decide
-- UDTs in different keyspaces are never value-compatible.
example : isValueCompatibleWith udtA udtOtherKs = false := by decide
-- Comparison-compatibility of UDTs is exact structural equality.
example : isCompatibleWith udtA udtA = true := by decide
example : isCompatibleWith udtAB udtA = false := by decide

/-! ## Vectors -/

example : isCompatibleWith (vector float 3) (vector float 3) = true := by decide
example : isCompatibleWith (vector float 3) (vector float 4) = false := by decide

/-! ## Serialization-compatibility is stricter than value-compatibility -/

-- `SimpleDateType` can read an int value, but the two have different fixed
-- lengths (4 vs variable), so they are not serialization-compatible.
example : isValueCompatibleWith sdate int32 = true := by decide
example : isSerializationCompatibleWith sdate int32 = false := by decide

-- `BytesType` is never serialization-compatible with a collection or UDT.
example : isSerializationCompatibleWith blob ascii = true := by decide
example : isSerializationCompatibleWith blob (clist false ascii) = false := by decide
example : isSerializationCompatibleWith blob udtA = false := by decide

end CQLType
end CassandraTypes
