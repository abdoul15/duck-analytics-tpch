from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.duck_etl_base import Table
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_database import get_table_from_db


class CustomerBronze(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = None,
        name: str = 'customer',
        primary_keys: List[str] = ['c_custkey'],
        storage_path: str = 's3://duckdb-bucket-tpch/bronze/customer',
        data_format: str = 'parquet',
        database: str = 'tpchdb',
        partition_keys: List[str] = ['etl_inserted'],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        super().__init__(
            conn,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        table_name = 'public.customer'

        relation = get_table_from_db(
            conn=self.conn,
            table_name=table_name
        )

        return [
            ETLDataSet(
                name=self.name,
                curr_data=relation,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )
        ]

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        input_data = upstream_datasets[0].curr_data
        current_ts = datetime.now().isoformat()

        # Crée une vue temporaire à partir de la relation
        input_data.create_view("tmp_customer_input", replace=True)

        # Utilise FROM tmp_customer_input pour ajouter la colonne
        transformed = self.conn.from_query(f"""
            SELECT *,
                TIMESTAMP '{current_ts}' AS etl_inserted
            FROM tmp_customer_input
        """)

        return ETLDataSet(
            name=self.name,
            curr_data=transformed,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        # Si load_data=False, on utilise la relation en mémoire
        if not self.load_data and self.curr_data is not None:
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        base_path = self.storage_path.rstrip('/')

        if partition_values:
            partition_path = "/".join(
                [f"{k}={v}" for k, v in partition_values.items()]
            )
            full_path = f"{base_path}/{partition_path}"
        else:
            # Lire toutes les partitions
            full_path = f"{base_path}/*"

            all_data = self.conn.read_parquet(f"{full_path}/*.parquet",hive_partitioning=True)
            all_data.create_view("all_partitions", replace=True)

            latest = self.conn.execute("SELECT max(etl_inserted) FROM all_partitions").fetchone()[0]
            return_relation = self.conn.from_query(
                f"SELECT * FROM all_partitions WHERE etl_inserted = '{latest}'"
            )

            return ETLDataSet(
                name=self.name,
                curr_data=return_relation,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        # Lecture d'une partition spécifique
        relation = self.conn.read_parquet(f"{full_path}/*.parquet", hive_partitioning=True)

        return ETLDataSet(
            name=self.name,
            curr_data=relation,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
