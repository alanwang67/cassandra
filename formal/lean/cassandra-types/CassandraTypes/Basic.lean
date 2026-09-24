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

/-!
# The syntax of Cassandra's CQL type system

This module formalizes the *structure* of Apache Cassandra's type system, as
implemented by the `org.apache.cassandra.db.marshal.AbstractType` hierarchy.

Every Cassandra column type is an `AbstractType`.  The concrete subclasses fall
into a handful of shapes:

* **native / scalar** types (`Int32Type`, `UTF8Type`, `BytesType`, ...), modelled
  by `Native`;
* the `ReversedType` wrapper used for descending clustering columns;
* the collection types `ListType`, `SetType`, `MapType`, each of which may be
  *multi-cell* (a live, updatable collection) or *frozen* (`!isMultiCell`);
* `TupleType`;
* `UserType` (UDTs), which may also be multi-cell or frozen;
* `VectorType`.

`CQLType` below is a faithful abstract-syntax model of that hierarchy.  The
per-type metadata that the storage engine reads off each `AbstractType`
(`valueLengthIfFixed`, `isMultiCell`, `isCollection`, ...) is modelled by the
functions in this file.  The type-relations (`isCompatibleWith`,
`isValueCompatibleWith`, `isSerializationCompatibleWith`) live in
`CassandraTypes.Compatibility`.
-/

namespace CassandraTypes

/-- Mirrors `AbstractType.ComparisonType`: how a serialized value is ordered. -/
inductive Comparison where
  /-- `NOT_COMPARABLE`: values of this type may never be compared. -/
  | notComparable
  /-- `BYTE_ORDER`: ordered by the unsigned lexicographic order of its bytes. -/
  | byteOrder
  /-- `CUSTOM`: ordered by a bespoke `compareCustom` implementation. -/
  | custom
  deriving DecidableEq, BEq, Repr, Inhabited

/--
The native (scalar) CQL types.  Each constructor corresponds to a concrete
`AbstractType` singleton in `org.apache.cassandra.db.marshal`.

`legacyDate` is `DateType` (the deprecated 8-byte timestamp), while `date` is the
modern `SimpleDateType`; the two are deliberately distinct because they have
different compatibility rules.
-/
inductive Native where
  | ascii       -- AsciiType
  | text        -- UTF8Type
  | blob        -- BytesType
  | boolean     -- BooleanType
  | tinyint     -- ByteType
  | smallint    -- ShortType
  | int32       -- Int32Type
  | bigint      -- LongType
  | varint      -- IntegerType
  | float       -- FloatType
  | double      -- DoubleType
  | decimal     -- DecimalType
  | timestamp   -- TimestampType
  | legacyDate  -- DateType
  | date        -- SimpleDateType
  | time        -- TimeType
  | uuid        -- UUIDType
  | timeuuid    -- TimeUUIDType
  | inet        -- InetAddressType
  | duration    -- DurationType
  | counter     -- CounterColumnType
  | empty       -- EmptyType
  deriving DecidableEq, BEq, Repr, Inhabited

namespace Native

/-- `AbstractType.comparisonType` for each native type. -/
def comparison : Native → Comparison
  | ascii | text | blob | inet | date | time | legacyDate | duration => .byteOrder
  | counter => .notComparable
  | _ => .custom

/-- `AbstractType.isByteOrderComparable`. -/
def isByteOrderComparable (t : Native) : Bool := t.comparison == .byteOrder

/--
`AbstractType.valueLengthIfFixed`, as `some n` for fixed-length types and `none`
for variable-length types (Cassandra encodes the latter as `-1`).  Only the
native types that override `valueLengthIfFixed` in the Java source are fixed.
-/
def valueLen : Native → Option Nat
  | boolean => some 1
  | int32 | float => some 4
  | bigint | double | timestamp | legacyDate => some 8
  | uuid | timeuuid => some 16
  | _ => none

/-- `AbstractType.allowsEmpty`: whether empty bytes are a valid encoding. -/
def allowsEmpty : Native → Bool
  | ascii | text | blob | int32 | timestamp | uuid | timeuuid => true
  | _ => false

/-- `AbstractType.isEmptyValueMeaningless`: whether empty bytes behave like null. -/
def emptyValueMeaningless : Native → Bool
  | int32 | legacyDate => true
  | _ => false

/--
`AbstractType.isCompatibleWith` restricted to native types.

The default (`AbstractType`) behaviour is equality; the special cases mirror the
overrides in `BytesType`, `UTF8Type`, `DateType` and `TimestampType`.
-/
def compatWith : Native → Native → Bool
  | blob, prev => prev == blob || prev == ascii || prev == text
  | text, prev => prev == text || prev == ascii
  | legacyDate, prev => prev == legacyDate || prev == timestamp
  | timestamp, prev => prev == timestamp || prev == legacyDate
  | t, prev => t == prev

/--
`AbstractType.isValueCompatibleWithInternal` restricted to native types.

Where a native type does not override value-compatibility it defaults to
`isCompatibleWith` (`compatWith`), exactly as `AbstractType` does.
-/
def valueCompatI : Native → Native → Bool
  | blob, _ => true
  | bigint, o => o == bigint || o == legacyDate || o == timestamp
  | varint, o => o == varint || o == int32 || o == bigint || o == legacyDate || o == timestamp
  | legacyDate, o => o == legacyDate || o == timestamp || o == bigint
  | timestamp, o => o == timestamp || o == legacyDate || o == bigint
  | time, o => o == time || o == bigint
  | uuid, o => o == uuid || o == timeuuid
  | date, o => o == date || o == int32
  | duration, o => o == duration
  | t, o => compatWith t o

/-- The derived `BEq` on `Native` is reflexive. -/
theorem beq_self (a : Native) : (a == a) = true := by cases a <;> rfl

end Native

/--
Abstract syntax of a Cassandra column type (`AbstractType`).

`multiCell` flags distinguish a live collection/UDT from its frozen counterpart
(`isMultiCell` in Java; `frozen = !multiCell`).  UDTs carry their keyspace and
name because `UserType` identity and compatibility depend on them.
-/
inductive CQLType where
  /-- A native scalar type. -/
  | native (t : Native)
  /-- `ReversedType`: reverses the sort order of its (scalar) base type. -/
  | reversed (base : CQLType)
  /-- `ListType` (`multiCell = true` unless frozen). -/
  | clist (multiCell : Bool) (elem : CQLType)
  /-- `SetType`. -/
  | cset (multiCell : Bool) (elem : CQLType)
  /-- `MapType`. -/
  | cmap (multiCell : Bool) (keys values : CQLType)
  /-- `TupleType`: always frozen. -/
  | tuple (fields : List CQLType)
  /-- `UserType`. -/
  | udt (keyspace name : String) (multiCell : Bool) (fieldNames : List String) (fields : List CQLType)
  /-- `VectorType`: a fixed number of elements of a single type. -/
  | vector (elem : CQLType) (dim : Nat)
  deriving Repr, Inhabited

namespace CQLType

/-
Structural equality on `CQLType`, mirroring the `equals` methods of the
`AbstractType` subclasses.  Provided by hand because Lean's `DecidableEq`
deriving handler does not cover this nested inductive.
-/
mutual
def beq : CQLType → CQLType → Bool
  | native a, native b => a == b
  | reversed a, reversed b => beq a b
  | clist m e, clist m' e' => m == m' && beq e e'
  | cset m e, cset m' e' => m == m' && beq e e'
  | cmap m k v, cmap m' k' v' => m == m' && beq k k' && beq v v'
  | tuple fs, tuple gs => beqList fs gs
  | udt ks nm mc fns fts, udt ks' nm' mc' fns' fts' =>
      ks == ks' && nm == nm' && mc == mc' && fns == fns' && beqList fts fts'
  | vector e d, vector e' d' => d == d' && beq e e'
  | _, _ => false

def beqList : List CQLType → List CQLType → Bool
  | [], [] => true
  | x :: xs, y :: ys => beq x y && beqList xs ys
  | _, _ => false
end

instance : BEq CQLType := ⟨beq⟩

/-- `AbstractType.isReversed`. -/
def isReversed : CQLType → Bool
  | reversed _ => true
  | _ => false

/--
Strip a single `ReversedType` wrapper, mirroring the unwrapping that
`AbstractType.isValueCompatibleWith` performs on both arguments.  In Cassandra a
`ReversedType`'s base is never itself reversed.
-/
def unwrapReversed : CQLType → CQLType
  | reversed b => b
  | t => t

/-- `AbstractType.isMultiCell`. -/
def isMultiCell : CQLType → Bool
  | clist mc _ => mc
  | cset mc _ => mc
  | cmap mc _ _ => mc
  | udt _ _ mc _ _ => mc
  | _ => false

/-- `AbstractType.isCollection`. -/
def isCollection : CQLType → Bool
  | clist _ _ => true
  | cset _ _ => true
  | cmap _ _ _ => true
  | _ => false

/-- `AbstractType.isUDT`. -/
def isUDT : CQLType → Bool
  | udt _ _ _ _ _ => true
  | _ => false

/-- `AbstractType.isTuple`. -/
def isTuple : CQLType → Bool
  | tuple _ => true
  | _ => false

/--
`AbstractType.valueLengthIfFixed`, as an `Option Nat`.

`ReversedType` delegates to its base; a `VectorType` of a fixed-length element is
fixed with length `element-length * dimension`; everything else that is not a
fixed native type is variable length.
-/
def valueLen : CQLType → Option Nat
  | native t => t.valueLen
  | reversed b => valueLen b
  | vector e dim =>
      match valueLen e with
      | some l => some (l * dim)
      | none => none
  | _ => none

end CQLType

end CassandraTypes
