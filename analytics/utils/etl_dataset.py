from dataclasses import dataclass
from typing import List
import duckdb


@dataclass
class ETLDataSet:
    """
    Conteneur de données pour DuckDB avec métadonnées.
    """
    name: str
    curr_data: duckdb.DuckDBPyRelation
    primary_keys: List[str]
    storage_path: str
    data_format: str 
    database: str
    partition_keys: List[str]