from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.duck_etl_base import Table
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_database import get_table_from_db


class OrdersBronze(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = None,
        name: str = 'orders',
        primary_keys: List[str] = ['o_orderkey'],
        data_format: str = 'parquet',
        database: str = 'tpchdb',
        partition_keys: List[str] = ['etl_inserted'],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        super().__init__(
            conn=conn,
            upstream_table_names=upstream_table_names,
            layer='bronze',
            name=name,
            primary_keys=primary_keys,
            data_format=data_format,
            database=database,
            partition_keys=partition_keys,
            run_upstream=run_upstream,
            load_data=load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        table_name = 'public.orders'
        relation = get_table_from_db(
            conn=self.conn,
            table_name=table_name,
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
        input_relation = upstream_datasets[0].curr_data
        view_name ="tmp_orders_input"
        input_relation.create_view(view_name, replace=True)
        
        etl_date = datetime.now().isoformat()
        
        transformed_query = f"""
            SELECT *, '{etl_date}' AS etl_inserted
            FROM tmp_orders_input
        """
        transformed_relation = self.conn.from_query(transformed_query)
        self.curr_data = transformed_relation

        return ETLDataSet(
            name=self.name,
            curr_data=transformed_relation,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        
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

        base_path = self.storage_path.rstrip("/")

        if partition_values:
            # Lire partition spécifique
            partition_path = "/".join([f"{k}={v}" for k, v in partition_values.items()])
            full_path = f"{base_path}/{partition_path}/*.parquet"
            relation = self.conn.read_parquet(full_path, hive_partitioning=True)

        else:
            # Lire la dernière partition dynamiquement sans créer une vue ou DF
            latest_partition_query = f"""
                SELECT max(etl_inserted) AS max_partition
                FROM read_parquet('{base_path}/*/*.parquet', hive_partitioning=true)
            """
            latest_partition = self.conn.execute(latest_partition_query).fetchone()[0]

            if latest_partition is None:
                raise ValueError(f"Aucune partition trouvée dans {base_path}")

            # Lire les données de la dernière partition
            relation = self.conn.from_query(f"""
                SELECT *
                FROM read_parquet('{base_path}/*/*.parquet', hive_partitioning=true)
                WHERE etl_inserted = '{latest_partition}'
            """)

        return ETLDataSet(
            name=self.name,
            curr_data=relation,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
