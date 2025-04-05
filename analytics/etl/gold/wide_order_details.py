from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_etl_base import Table
from analytics.etl.silver.fct_orders import FctOrdersSilver
from analytics.etl.silver.dim_customer import DimCustomerSilver
from analytics.etl.silver.dim_part import DimPartSilver


class WideOrderDetailsGold(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = [
            FctOrdersSilver,
            DimCustomerSilver,
            DimPartSilver,
        ],
        name: str = 'wide_order_details',
        primary_keys: List[str] = ['order_key', 'line_number'],
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
        upstream_datasets = []
        for TableClass in self.upstream_table_names:
            etl_instance = TableClass(
                conn=self.conn,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                etl_instance.run()
            upstream_datasets.append(etl_instance.read())
        return upstream_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        orders = upstream_datasets[0].curr_data
        customers = upstream_datasets[1].curr_data
        parts = upstream_datasets[2].curr_data

        # Register views
        orders.create_view("orders_data", replace=True)
        customers.create_view("customers_data", replace=True)
        parts.create_view("parts_data", replace=True)


        etl_date = datetime.now().isoformat()

        query = f"""
        SELECT
            o.order_key,
            o.line_number,
            o.order_date,
            o.order_status,
            o.order_priority,
            o.extended_price,
            o.net_amount,
            o.discount_amount,
            o.tax_amount,
            c.customer_name,
            c.market_segment,
            c.nation_name AS customer_nation,
            c.region_name AS customer_region,
            p.part_name,
            p.manufacturer,
            p.brand,
            p.product_type,
            p.manufacturing_country,
            p.manufacturing_region,
            o.shipping_delay_days,
            o.delivery_delay_days,
            o.is_late_delivery,
            '{etl_date}' AS etl_inserted
        FROM orders_data o
        LEFT JOIN customers_data c ON o.customer_key = c.customer_key
        LEFT JOIN parts_data p ON o.part_key = p.part_key
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
