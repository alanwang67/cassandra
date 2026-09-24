# A Lean 4 formalization of Cassandra's CQL type system

This directory contains a self-contained [Lean 4](https://leanprover.github.io/)
formalization of the *type system* implemented by the
`org.apache.cassandra.db.marshal.AbstractType` hierarchy — the classes that
describe every CQL column type, decide how values are ordered and serialized, and
determine when one type may replace another under `ALTER TABLE` or in CQL
assignments.

The model is deliberately dependency-free (Lean core only, no Mathlib) so it
builds quickly and is easy to audit.

## What is modelled

`CassandraTypes/Basic.lean` defines the abstract syntax of a column type,
`CQLType`, mirroring the `AbstractType` subclasses:

| Lean constructor | Cassandra type(s) |
| --- | --- |
| `native t` (see `Native`) | `Int32Type`, `LongType`, `IntegerType`, `UTF8Type`, `AsciiType`, `BytesType`, `TimestampType`, `DateType`, `SimpleDateType`, `TimeType`, `UUIDType`, `TimeUUIDType`, `DurationType`, ... |
| `reversed base` | `ReversedType` |
| `clist multiCell elem` | `ListType` |
| `cset multiCell elem` | `SetType` |
| `cmap multiCell keys values` | `MapType` |
| `tuple fields` | `TupleType` |
| `udt keyspace name multiCell fieldNames fields` | `UserType` |
| `vector elem dim` | `VectorType` |

Alongside the syntax, `Basic.lean` models the per-type metadata the storage
engine reads off each `AbstractType`: `comparison` (`ComparisonType`),
`valueLen` (`valueLengthIfFixed`), `isMultiCell`, `isCollection`, `isUDT`,
`allowsEmpty` and `isEmptyValueMeaningless`.

## The compatibility relations

`CassandraTypes/Compatibility.lean` formalizes the three relations Cassandra uses
to decide whether one type may safely replace another:

* `isCompatibleWith` — *comparison* compatibility (validates the same values and
  orders them identically);
* `isValueCompatibleWith` — the weaker *value* compatibility (values can still be
  read, but not necessarily ordered);
* `isSerializationCompatibleWith` — value compatibility that additionally
  preserves the cell wire-encoding (fixed length and multi-cell-ness).

Because these relations are mutually recursive in the Java source, they are
computed together in a single structurally-recursive pass, `core`, whose result
is the triple `(isCompatibleWith, isValueCompatibleWithInternal,
isSerializationCompatibleWith)`. `ReversedType` is stripped at the top level
exactly as `AbstractType.isValueCompatibleWith` does.

Every branch faithfully reproduces the corresponding Java override, including the
subtle ones:

* `BytesType` is value-compatible with *anything* but serialization-compatible
  only with non-collection, non-UDT scalars;
* `UTF8Type` accepts `AsciiType`, but not the reverse;
* `DateType` and `TimestampType` are mutually comparison-compatible;
* `IntegerType` (varint) can read `int`/`bigint` layouts;
* frozen collections compare their elements, while multi-cell collections require
  the value comparator to be *serialization*-compatible;
* frozen maps require keys to be comparison-compatible but values only
  value-compatible;
* tuples/UDTs may gain trailing fields but not lose them.

## What is proved

`CassandraTypes/Theorems.lean` proves the invariants stated in the `AbstractType`
Javadoc:

* `isCompatibleWith_refl`, `isValueCompatibleWith_refl`,
  `isSerializationCompatibleWith_refl` — *"a type should be compatible with at
  least itself"*;
* `isSerializationCompatibleWith_imp_value` — serialization compatibility is a
  strengthening of value compatibility (matching the Java class hierarchy, where
  the former is defined in terms of the latter).

These theorems depend only on Lean's three standard axioms (`propext`,
`Classical.choice`, `Quot.sound`) — there are no `sorry`s.

`CassandraTypes/Examples.lean` is an executable conformance suite: ~45
`example`s, each discharged by `decide`, pinning the model to concrete behaviours
of the real implementation (e.g. `isValueCompatibleWith blob int32 = true`,
`isCompatibleWith ascii text = false`).

## Building

Install the Lean toolchain with [`elan`](https://github.com/leanprover/elan);
the pinned version is in `lean-toolchain`. Then:

```bash
lake build
```

A successful build type-checks the model, the proofs, and the conformance suite.

## Scope and simplifications

* The model captures type *structure*, *metadata*, and the *compatibility*
  relations. It does not model value serialization bytes, the concrete
  comparators, or CQL parsing.
* `ReversedType` is modelled as a top-level wrapper over scalar base types, as it
  is used for clustering columns in Cassandra.
* A representative (not exhaustive) set of native types is included; adding more
  is a matter of extending `Native` and its metadata tables.
