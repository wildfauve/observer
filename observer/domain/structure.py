from typing import Dict, List, Union, Tuple, Callable
from dataclasses import dataclass
from pyspark.sql.types import StructField

from observer.domain import schema
from observer.util import monad


def default_cell_builder(cell):
    return cell.props


class RootParent:
    column_name = "__RootMeta__"

    def __init__(self, meta: str, run: schema.Run):
        self.meta = meta
        self.run = run

    def meta_props(self):
        return {'columnName': self.column_name, 'meta': self.meta, 'lineage_id': self.run.trace}

    def identity_props(self):
        return {self.column_name: {'identity': self.meta, 'lineage_id': self.run.trace}}


@dataclass
class Column:
    schema: StructField
    cell_builder: Callable = default_cell_builder
    validator: Callable = None


class Cell:
    def __init__(self, column: Column, props: Union[Dict, List, Tuple, str], identity: str = None):
        self.column = column
        self.props = props
        self.parent = None
        self.identity = identity
        self._validations_results = None
        pass

    def validation_results(self):
        if self._validations_results:
            return self._validations_results
        self._validations_results = self.column.validator(self) if self.column.validator else monad.Right(None)
        return self._validations_results

    def has_parent(self, parent):
        self.parent = parent
        return self

    def build(self):
        return self.column.cell_builder(self)

    def to_dict(self):
        return self.props

    def as_cell_dict(self):
        return {self.column_name(): self.props}

    def cell_dict_with_errors(self):
        return {**self.as_cell_dict(), "validationErrors": self.validation_results().lift()}

    def root_parent(self):
        if not self.parent:
            return None
        if isinstance(self.parent, RootParent):
            return self.parent
        return self.parent.root_parent()

    def meta_props(self):
        return {'columnName': self.column_name(), 'identity': self.identity}

    def identity_props(self):
        return {self.column_name(): {'identity': self.identity}}

    def column_name(self):
        return self.column.schema.name

    def parents_meta_props(self, meta_props: dict = {}):
        if not self.parent:
            return meta_props
        if isinstance(self.parent, RootParent):
            return {**meta_props, **self.parent.identity_props()}
        return self.parent.parents_meta_props({**meta_props, **self.parent.identity_props()})
