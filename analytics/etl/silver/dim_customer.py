from datetime import datetime
from typing import Dict, List, Optional, Type, Any

import duckdb
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_etl_base import Table
from analytics.etl.bronze.customer import CustomerBronze
from analytics.etl.bronze.nation import NationBronze
from analytics.etl.bronze.region import RegionBronze


class DimCustomerSilver(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = [
            CustomerBronze,
            NationBronze,
            RegionBronze,
        ],
        name: str = 'dim_customer',
        primary_keys: List[str] = ['customer_key'],
        data_format: str = 'parquet',
        database: str = 'tpchdb',
        partition_keys: List[str] = ['etl_inserted'],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        super().__init__(
            conn=conn,
            upstream_table_names=upstream_table_names,
            layer='silver',
            name=name,
            primary_keys=primary_keys,
            data_format=data_format,
            database=database,
            partition_keys=partition_keys,
            run_upstream=run_upstream,
            load_data=load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        upstream_etl_datasets = []

        for TableClass in self.upstream_table_names:
            etl_instance = TableClass(
                conn=self.conn,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                etl_instance.run()

            upstream_etl_datasets.append(etl_instance.read())

        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        customer_data = upstream_datasets[0].curr_data
        nation_data = upstream_datasets[1].curr_data
        region_data = upstream_datasets[2].curr_data

        customer_data.create_view("customer_temp", replace=True)
        nation_data.create_view("nation_temp", replace=True)
        region_data.create_view("region_temp", replace=True)

        etl_inserted = datetime.now().isoformat()

        transformed_query = f"""
            WITH geo_data AS (
                SELECT
                    n.n_nationkey,
                    n.n_name AS nation_name,
                    r.r_name AS region_name
                FROM nation_temp n
                LEFT JOIN region_temp r ON n.n_regionkey = r.r_regionkey
            )
            SELECT
                c.c_custkey AS customer_key,
                c.c_name AS customer_name,
                c.c_address AS street_address,
                g.nation_name,
                g.region_name,
                c.c_address || ', ' || g.nation_name || ', ' || g.region_name AS full_address,
                c.c_phone AS phone_number,
                c.c_acctbal AS account_balance,
                c.c_mktsegment AS market_segment,
                '{etl_inserted}' AS etl_inserted
            FROM customer_temp c
            LEFT JOIN geo_data g ON c.c_nationkey = g.n_nationkey
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
