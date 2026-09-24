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
# Meta-theorems about the compatibility relations

The `AbstractType` documentation states two invariants that every compatibility
relation must satisfy:

* *"a type should be compatible with at least itself"* — reflexivity, proved here
  as `isCompatibleWith_refl`, `isValueCompatibleWith_refl` and
  `isSerializationCompatibleWith_refl`;
* *serialization-compatibility is a strengthening of value-compatibility* — proved
  as `isSerializationCompatibleWith_imp_value`.
-/

namespace CassandraTypes
namespace CQLType

/-! ## Structural equality is reflexive -/

mutual
theorem beq_refl (t : CQLType) : beq t t = true := by
  cases t with
  | native a => simpa [beq] using Native.beq_self a
  | reversed a => simp [beq, beq_refl a]
  | clist m e => simp [beq, beq_refl e]
  | cset m e => simp [beq, beq_refl e]
  | cmap m k v => simp [beq, beq_refl k, beq_refl v]
  | tuple fs => simp [beq, beqList_refl fs]
  | udt ks nm mc fns fts => simp [beq, beqList_refl fts]
  | vector e d => simp [beq, beq_refl e]

theorem beqList_refl (xs : List CQLType) : beqList xs xs = true := by
  cases xs with
  | nil => simp [beqList]
  | cons x xs => simp [beqList, beq_refl x, beqList_refl xs]
end

/-! ## Reflexivity of the three compatibility relations -/

mutual
/-- All three relations hold between a type and itself. -/
theorem core_refl (t : CQLType) :
    (core t t).1 = true ∧ (core t t).2.1 = true ∧ (core t t).2.2 = true := by
  cases t with
  | native a => cases a <;> decide
  | reversed a =>
      have h := core_refl a
      simp [core, h.1]
  | clist m e =>
      have h := core_refl e
      simp [core, h.1, h.2.1, h.2.2]
  | cset m e =>
      have h := core_refl e
      simp [core, h.1]
  | cmap m k v =>
      have hk := core_refl k
      have hv := core_refl v
      simp [core, hk.1, hv.1, hv.2.1, hv.2.2]
  | tuple fs =>
      simp [core, allCw_refl fs, allVci_refl fs]
  | udt ks nm mc fns fts =>
      have hb := beq_refl (udt ks nm mc fns fts)
      simp [core, hb, allCw_refl fts]
  | vector e d =>
      have hb := beq_refl (vector e d)
      simp [core, hb]

theorem allCw_refl (xs : List CQLType) : allCw xs xs = true := by
  cases xs with
  | nil => simp [allCw]
  | cons x xs => simp [allCw, (core_refl x).1, allCw_refl xs]

theorem allVci_refl (xs : List CQLType) : allVci xs xs = true := by
  cases xs with
  | nil => simp [allVci]
  | cons x xs => simp [allVci, (core_refl x).2.1, allVci_refl xs]
end

/-- `isCompatibleWith` is reflexive: every type may replace itself. -/
theorem isCompatibleWith_refl (t : CQLType) : isCompatibleWith t t = true :=
  (core_refl t).1

/-- `isValueCompatibleWith` is reflexive. -/
theorem isValueCompatibleWith_refl (t : CQLType) : isValueCompatibleWith t t = true :=
  (core_refl t.unwrapReversed).2.1

/-- `isSerializationCompatibleWith` is reflexive. -/
theorem isSerializationCompatibleWith_refl (t : CQLType) :
    isSerializationCompatibleWith t t = true :=
  (core_refl t.unwrapReversed).2.2

/-! ## Serialization-compatibility implies value-compatibility -/

/-- In every branch of `core`, the serialization component has the form
`valueCompat && extra`, hence entails the value-compatibility component. -/
theorem core_ser_imp_vci (a b : CQLType) :
    (core a b).2.2 = true → (core a b).2.1 = true := by
  cases a <;> cases b <;> intro h <;> simp_all [core] <;> split <;> simp_all

/-- `isSerializationCompatibleWith` is a strengthening of
`isValueCompatibleWith`, matching the Java class hierarchy where the former is
defined in terms of the latter. -/
theorem isSerializationCompatibleWith_imp_value (t s : CQLType) :
    isSerializationCompatibleWith t s = true → isValueCompatibleWith t s = true :=
  core_ser_imp_vci t.unwrapReversed s.unwrapReversed

end CQLType
end CassandraTypes
