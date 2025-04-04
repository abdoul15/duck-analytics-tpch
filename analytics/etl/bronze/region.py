from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.duck_etl_base import Table
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_database import get_table_from_db


class RegionBronze(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = None,
        name: str = 'region',
        primary_keys: List[str] = ['r_regionkey'],
        storage_path: str = 's3://duckdb-bucket-tpch/bronze/region',
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
        table_name = 'public.region'
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
        input_relation.create_view("tmp_region_input", replace=True)

        etl_date = datetime.now().isoformat()

        transformed_query = f"""
            SELECT *, '{etl_date}' AS etl_inserted
            FROM tmp_region_input
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

        base_path = self.storage_path.rstrip('/')

        if partition_values:
            partition_path = "/".join([f"{k}={v}" for k, v in partition_values.items()])
            full_path = f"{base_path}/{partition_path}"
        else:
            full_path = f"{base_path}/*"
            all_data = self.conn.read_parquet(f"{full_path}/*.parquet", hive_partitioning=True)
            all_data.create_view("all_region_partitions", replace=True)

            latest = self.conn.execute("SELECT max(etl_inserted) FROM all_region_partitions").fetchone()[0]
            filtered = self.conn.from_query(
                f"SELECT * FROM all_region_partitions WHERE etl_inserted = '{latest}'"
            )

            return ETLDataSet(
                name=self.name,
                curr_data=filtered,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

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
