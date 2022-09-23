from typing import List
import json

from observer.util import singleton


class SumCounter:

    def __init__(self, name, unit: int = 1):
        self.name = name
        self.unit = unit
        self.dimensions = {'sum': 0}

    def inc(self, dimensions: List = []):
        self.dimensions['sum'] += self.unit
        [self.inc_dimension(dimension) for dimension in dimensions]
        return self

    def inc_dimension(self, dimension):
        if self.dimensions.get(dimension, None):
            self.dimensions[dimension] += self.unit
        else:
            self.dimensions[dimension] = self.unit
        pass

    def to_dict(self):
        return {
            self.name: self.dimensions
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    def count(self):
        return self.dimensions['sum']

    # def spark_struct(self):
    #     return StructType().add(self.name, MapType(StringType(), IntegerType()))


class MetricProvider(singleton.Singleton):
    metrics = {}

    def create_sum_counter(self, name, unit):
        metric = SumCounter(name, unit)
        self.metrics[name] = metric
        return metric

    def metric(self, name):
        return self.metrics.get(name, None)


def metric_provider():
    return MetricProvider()
