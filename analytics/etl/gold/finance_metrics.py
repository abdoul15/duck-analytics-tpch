from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_etl_base import Table
from analytics.etl.gold.wide_order_details import WideOrderDetailsGold


class FinanceMetricsGold(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = [
            WideOrderDetailsGold
        ],
        name: str = 'finance_metrics',
        primary_keys: List[str] = ['dt', 'customer_nation', 'customer_region'],
        data_format: str = 'parquet',
        database: str = 'tpchdb',
        partition_keys: List[str] = ['etl_inserted'],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        super().__init__(
            conn=conn,
            upstream_table_names=upstream_table_names,
            layer='gold',
            name=name,
            primary_keys=primary_keys,
            data_format=data_format,
            database=database,
            partition_keys=partition_keys,
            run_upstream=run_upstream,
            load_data=load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        datasets = []
        for TableClass in self.upstream_table_names:
            etl = TableClass(
                conn=self.conn,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                etl.run()
            datasets.append(etl.read())
        return datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        orders = upstream_datasets[0].curr_data
        orders.create_view("wide_orders", replace=True)

        etl_date = datetime.now().isoformat()

        query = f"""
        SELECT
            CAST(order_date AS DATE) AS dt,
            customer_nation,
            customer_region,

            ROUND(SUM(net_amount), 2) AS total_revenue,
            ROUND(SUM(tax_amount), 2) AS total_tax,
            ROUND(SUM(discount_amount), 2) AS total_discounts,

            -- Créances clients
            ROUND(SUM(CASE WHEN order_status != 'F' THEN net_amount ELSE 0 END), 2) AS accounts_receivable,

            COUNT(DISTINCT order_key) AS order_count,
            ROUND(AVG(net_amount), 2) AS average_order_value,

            ROUND(SUM(net_amount) - SUM(extended_price) * 0.8, 2) AS estimated_margin,
            ROUND((SUM(net_amount) - SUM(extended_price) * 0.8) / SUM(net_amount) * 100, 2) AS margin_percentage,

            ROUND(AVG(CASE WHEN order_status != 'F' THEN DATE_DIFF('day', order_date, CURRENT_DATE) ELSE NULL END), 1) AS avg_open_order_age,

            '{etl_date}' AS etl_inserted

        FROM wide_orders
        GROUP BY dt, customer_nation, customer_region
        """

        relation = self.conn.from_query(query)
        self.curr_data = relation

        return ETLDataSet(
            name=self.name,
            curr_data=relation,
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
