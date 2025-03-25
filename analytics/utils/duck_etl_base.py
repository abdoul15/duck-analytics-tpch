from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Type

import duckdb
import great_expectations as gx

from analytics.utils.etl_dataset import ETLDataSet 

class InValidDataException(Exception):
    pass

class TableETL(ABC):
    def __init__(
        self,
        duck_conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[TableETL]]],
        name: str,
        primary_keys: List[str],
        storage_path: str,
        data_format: str,
        database: str,
        partition_keys: List[str],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        self.conn = duck_conn
        self.upstream_table_names = upstream_table_names
        self.name = name
        self.primary_keys = primary_keys
        self.storage_path = storage_path
        self.data_format = data_format
        self.database = database
        self.partition_keys = partition_keys
        self.run_upstream = run_upstream
        self.load_data = load_data
        self.curr_data = None

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataSet]:
        pass

    @abstractmethod
    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        pass

    def validate(self, data: ETLDataSet) -> bool:
        """
        Valide les données avec Great Expectations.
        Pour DuckDB, on utilisera la conversion vers Pandas temporairement ici si nécessaire.
        """
        ge_path = 'tpch_etl_pipeline/gx'
        expc_json_path = f'{ge_path}/expectations/{self.name}.json'
        file_path = Path(expc_json_path)

        if not file_path.exists():
            return True  # Pas de suite d'attentes, on valide par défaut

        try:
            context = gx.get_context(context_root_dir=os.path.join(os.getcwd(), ge_path))
            datasource = context.sources.add_or_update_pandas(name='temp_duckdb_datasource')
            asset = datasource.add_dataframe_asset(name=self.name)

            # DuckDB relation to Pandas just for validation
            pandas_df = data.curr_data.df()

            batch_request = asset.build_batch_request(dataframe=pandas_df)

            checkpoint = context.add_or_update_checkpoint(
                name='temp_checkpoint',
                validations=[
                    {
                        'batch_request': batch_request,
                        'expectation_suite_name': self.name,
                    }
                ],
            )

            result = checkpoint.run()
            return result.success
        except Exception as e:
            print(f"Validation error for {self.name}: {e}")
            return False

    def load(self, data: ETLDataSet) -> None:
        """
        Écrit les données transformées en Parquet dans S3 via DuckDB.
        """

        # 1. Créer une vue temporaire sur les données à sauvegarder
        data.curr_data.create_view("tmp_to_write", replace=True)

        # 2. Ajouter partitionnement si nécessaire
        partition_clause = ""
        if data.partition_keys:
            partition_cols = ", ".join(data.partition_keys)
            partition_clause = f", PARTITION_BY ({partition_cols})"

        # 3. Construire la requête COPY
        query = f"""
        COPY tmp_to_write
        TO '{data.storage_path}'
        (FORMAT '{data.data_format}'{partition_clause}, OVERWRITE_OR_IGNORE);
        """

        self.conn.execute(query)




    def run(self) -> None:
        """
        Exécute le processus ETL complet pour la table.

        Cette méthode orchestre l'exécution du processus ETL complet:
        1. Extrait les données des sources en amont
        2. Transforme les données extraites
        3. Valide les données transformées (si activé)
        4. Charge les données transformées dans le stockage (si activé)

        Returns:
            ETLDataSet: Un objet ETLDataSet contenant les données finales
        """
        transformed_data = self.transform_upstream(self.extract_upstream())
        if not self.validate(transformed_data):
            raise InValidDataException(
                f'The {self.name} dataset did not pass validation, please'
                ' check the validation results for more information'
            )
        if self.load_data:
            self.load(transformed_data)
    

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        pass
