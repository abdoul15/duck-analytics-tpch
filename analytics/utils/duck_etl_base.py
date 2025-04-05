from __future__ import annotations

import os
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Type
from dotenv import load_dotenv # Importer load_dotenv

import duckdb

from analytics.utils.etl_dataset import ETLDataSet



class InValidDataException(Exception):
    pass

class Table(ABC):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]],
        layer: str,
        name: str,
        primary_keys: List[str],
        data_format: str,
        database: str,
        partition_keys: List[str],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        self.logger = logging.getLogger(f"etl.{self.__class__.__name__}")
        self.conn = conn
        self.upstream_table_names = upstream_table_names
        self.name = name
        self.primary_keys = primary_keys
        # Lire le bucket depuis l'env var
        load_dotenv()
        s3_bucket = os.getenv("S3_BUCKET")
        if not s3_bucket:
            raise ValueError("La variable d'environnement S3_BUCKET n'est pas définie.")
        self.storage_path = f"s3://{s3_bucket}/{layer}/{name}"
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
        Valide les données 
        
        Args:
            data: ETLDataSet à valider
            
        Returns:
            bool: True si la validation réussit, False sinon
        """
        return True


    def load(self, data: ETLDataSet) -> None:
        """
        Écrit les données transformées en Parquet dans S3 via DuckDB.
        """

        data.curr_data.create_view("tmp_to_write", replace=True)

        partition_clause = ""
        if data.partition_keys:
            partition_cols = ", ".join(data.partition_keys)
            partition_clause = f", PARTITION_BY ({partition_cols})"

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
        self.logger.info(f"Début du traitement ETL pour {self.name}")
        try:
            self.logger.debug("Extraction des données en amont")
            upstream_data = self.extract_upstream()
            
            self.logger.debug("Transformation des données")
            transformed_data = self.transform_upstream(upstream_data)
        
            # if not self.validate(transformed_data):
            #     self.logger.error(f"Validation failed for {self.name}")
            #     raise InValidDataException(
            #         f'The {self.name} dataset did not pass validation, please'
            #         ' check the validation results for more information'
            #     )
            
            self.curr_data = transformed_data.curr_data
            
            if self.load_data:
                self.logger.debug(f"Chargement des données pour {self.name}")
                self.load(transformed_data)
            
            self.logger.info(f"Traitement ETL terminé avec succès pour {self.name}")
        except Exception as e:
            self.logger.error(f"Erreur lors du traitement ETL pour {self.name}: {str(e)}")
            raise
    

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        pass
