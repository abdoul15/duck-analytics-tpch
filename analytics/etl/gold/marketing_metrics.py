from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_etl_base import Table
from analytics.etl.gold.wide_order_details import WideOrderDetailsGold


class MarketingMetricsGold(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = [
            WideOrderDetailsGold
        ],
        name: str = 'marketing_metrics',
        primary_keys: List[str] = ['month', 'market_segment'],
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
        WITH base AS (
            SELECT
                DATE_TRUNC('month', order_date) AS month,
                market_segment,
                net_amount,
                order_key,
                brand,
                is_late_delivery
            FROM wide_orders
        ),
        base_grouped AS (
            SELECT
                month,
                market_segment,
                COUNT(DISTINCT order_key) AS order_count,
                ROUND(SUM(net_amount), 2) AS total_revenue,
                ROUND(AVG(net_amount), 2) AS avg_order_value,
                ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate
            FROM base
            GROUP BY month, market_segment
        ),
        top_brands AS (
            SELECT
                DATE_TRUNC('month', order_date) AS month,
                market_segment,
                brand,
                COUNT(*) AS brand_orders,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE_TRUNC('month', order_date), market_segment
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM wide_orders
            GROUP BY 1, 2, 3
        )
        SELECT
            g.*,
            tb.brand AS top_brand,
            '{etl_date}' AS etl_inserted
        FROM base_grouped g
        LEFT JOIN top_brands tb
            ON g.month = tb.month
            AND g.market_segment = tb.market_segment
            AND tb.rn = 1
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
