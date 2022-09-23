from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from observer.domain import vocab_dictionary as V


def build_field(term, field_type, metadata, nullable) -> StructField:
    return StructField(term, field_type, metadata=metadata, nullable=nullable)


def build_string_field(vocab, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab)
    return build_field(term, StringType(), metadata=meta, nullable=nullable)


def build_decimal_field(vocab, decimal_type, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab)
    return build_field(term, decimal_type, metadata=meta, nullable=nullable)


def build_array_field(vocab, struct_type, nullable) -> StructField:
    term, meta = V.term_and_meta(vocab)
    return build_field(term, ArrayType(struct_type), metadata=meta, nullable=nullable)


def build_struct_field(vocab: str, struct_type: StructType, nullable: bool) -> StructField:
    term, meta = V.term_and_meta(vocab)
    return build_field(term, struct_type, metadata=meta, nullable=nullable)

at_id = build_string_field("*.@id", nullable=False)

optional_at_id = build_string_field("*.@id", nullable=True)

at_type = build_string_field("*.@type", nullable=False)

label = build_string_field("*.lcc-lr:hasTag", nullable=True)

type_id_label_struct = StructType([
    at_id,
    at_type,
    label
])

type_id_struct = StructType([
    at_id,
    at_type,
])

type_label_struct = StructType([
    at_type,
    label
])

id_struct = StructType([
    at_id
])
