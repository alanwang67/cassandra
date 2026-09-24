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
import CassandraTypes.Basic

/-!
# Type-compatibility relations

Cassandra defines three relations on `AbstractType`, used when a column's type is
changed via `ALTER TABLE` and when checking CQL assignability:

* `isCompatibleWith` — *comparison*-compatibility: `this` may safely replace
  `previous` in a clustering position, i.e. it validates the same values and
  orders them identically.
* `isValueCompatibleWith` — the weaker *value*-compatibility: values written
  under `previous` can still be read (but not necessarily ordered) under `this`.
* `isSerializationCompatibleWith` — value-compatibility that additionally
  preserves the cell wire-encoding (fixed length and multi-cell-ness).

These relations are mutually recursive, so we compute all three in a single pass
via `core`.  `core t s = (isCompatibleWith, isValueCompatibleWithInternal,
isSerializationCompatibleWith)`, where the arguments are assumed to already have
any top-level `ReversedType` stripped (Cassandra only uses `ReversedType` at the
top level, on scalar base types).  By construction the third component always has
the shape `vci && extra`, which makes `isSerializationCompatibleWith` imply
`isValueCompatibleWith`.
-/

namespace CassandraTypes
namespace CQLType

mutual

/--
Compute `(isCompatibleWith, isValueCompatibleWithInternal,
isSerializationCompatibleWith)` for two (reversed-stripped) types.

Each branch mirrors the corresponding `AbstractType` subclass:
`BytesType`/`UTF8Type`/... (native), `CollectionType` and its `ListType`,
`SetType`, `MapType` subclasses, `TupleType`, `UserType` and `VectorType`.
-/
def core (t s : CQLType) : Bool × Bool × Bool :=
  match t with
  | .native nt =>
      match s with
      | .native ns =>
          let cw := Native.compatWith nt ns
          let vci := Native.valueCompatI nt ns
          let ser := vci && (nt.valueLen == ns.valueLen)
          (cw, vci, ser)
      | s =>
          -- native vs non-native: only BytesType (`blob`) can read a non-native value
          let vci := nt == Native.blob
          let extra := (!s.isCollection) && (!s.isUDT)
                        && (nt.valueLen == s.valueLen) && (false == s.isMultiCell)
          (false, vci, vci && extra)
  | .reversed a =>
      match s with
      | .reversed b =>
          let sub := core a b
          let vci := sub.1
          (sub.1, vci, vci && ((reversed a).valueLen == (reversed b).valueLen))
      | _ => (false, false, false)
  | .clist mc e =>
      match s with
      | .clist mc' e' =>
          let sub := core e e'
          let same := mc == mc'
          let cw := same && (if mc then sub.2.2 else sub.1)
          let vci := if mc then cw else (same && sub.2.1)
          (cw, vci, vci && sub.2.2)
      | _ => (false, false, false)
  | .cset mc e =>
      match s with
      | .cset mc' e' =>
          let sub := core e e'
          let same := mc == mc'
          let cw := same && sub.1
          (cw, cw, cw && true)
      | _ => (false, false, false)
  | .cmap mc k v =>
      match s with
      | .cmap mc' k' v' =>
          let subK := core k k'
          let subV := core v v'
          let same := mc == mc'
          let cw := same && (if mc then (subK.1 && subV.2.2) else (subK.1 && subV.1))
          let vci := if mc then cw else (same && subK.1 && subV.2.1)
          (cw, vci, vci && subV.2.2)
      | _ => (false, false, false)
  | .tuple fs =>
      match s with
      | .tuple gs =>
          let cw := allCw fs gs
          let vci := allVci fs gs
          (cw, vci, vci && true)
      | _ => (false, false, false)
  | .udt ks nm mc fns fts =>
      match s with
      | .udt ks' nm' mc' fns' fts' =>
          let cw := beq (udt ks nm mc fns fts) (udt ks' nm' mc' fns' fts')
          let vci := (mc == mc') && (ks == ks') && allCw fts fts'
          (cw, vci, vci && (mc == mc'))
      | _ => (false, false, false)
  | .vector e dim =>
      match s with
      | .vector e' dim' =>
          let cw := beq (vector e dim) (vector e' dim')
          let vci := cw
          (cw, vci, vci && ((vector e dim).valueLen == (vector e' dim').valueLen))
      | _ => (false, false, false)

/-- Pairwise `isCompatibleWith` over field lists; `true` iff `fs` extends `gs`
(`fs.length ≥ gs.length`) and matches componentwise.  Mirrors the loops in
`TupleType`/`UserType`. -/
def allCw : List CQLType → List CQLType → Bool
  | _, [] => true
  | [], _ :: _ => false
  | f :: fs, g :: gs => (core f g).1 && allCw fs gs

/-- Pairwise `isValueCompatibleWith` over field lists (used by `TupleType`). -/
def allVci : List CQLType → List CQLType → Bool
  | _, [] => true
  | [], _ :: _ => false
  | f :: fs, g :: gs => (core f g).2.1 && allVci fs gs

end

/-- `AbstractType.isCompatibleWith`. -/
def isCompatibleWith (t s : CQLType) : Bool := (core t s).1

/-- `AbstractType.isValueCompatibleWith`: strip a top-level `ReversedType` from
both sides, then take value-compatibility. -/
def isValueCompatibleWith (t s : CQLType) : Bool :=
  (core t.unwrapReversed s.unwrapReversed).2.1

/-- `AbstractType.isSerializationCompatibleWith`. -/
def isSerializationCompatibleWith (t s : CQLType) : Bool :=
  (core t.unwrapReversed s.unwrapReversed).2.2

/-- `AbstractType.testAssignment` outcome, restricted to the two "assignable"
verdicts we can decide structurally. -/
def isAssignableFrom (receiver source : CQLType) : Bool :=
  receiver == source || isValueCompatibleWith receiver source

end CQLType
end CassandraTypes
