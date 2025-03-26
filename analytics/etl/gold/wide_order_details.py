from datetime import date
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet 
from analytics.utils.duck_etl_base import TableETL
from analytics.etl.silver.fct_orders import FctOrdersSilverETL
from analytics.etl.silver.dim_customer import DimCustomerSilverETL
from analytics.etl.silver.dim_part import DimPartSilverETL


class WideOrderDetailsGoldETL(TableETL):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            FctOrdersSilverETL,
            DimCustomerSilverETL,
            DimPartSilverETL,
        ],
        name: str = 'wide_order_details',
        primary_keys: List[str] = ['order_key', 'line_number'],
        storage_path: str = 's3://duckdb-bucket-tpch/gold/wide_order_details',
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
        upstream_datasets = []
        for TableETLClass in self.upstream_table_names:
            etl_instance = TableETLClass(
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
        orders.create_view("orders", replace=True)
        customers.create_view("customers", replace=True)
        parts.create_view("parts", replace=True)

        etl_date = date.today().isoformat()

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
        FROM orders o
        LEFT JOIN customers c ON o.customer_key = c.customer_key
        LEFT JOIN parts p ON o.part_key = p.part_key
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

        base_path = self.storage_path.rstrip('/')

        if partition_values:
            partition_path = "/".join(
                [f"{k}={v}" for k, v in partition_values.items()]
            )
            full_path = f"{base_path}/{partition_path}"
            relation = self.conn.read_parquet(f"{full_path}/*.parquet", hive_partitioning=True)
        else:
            full_path = f"{base_path}/*"
            all_data = self.conn.read_parquet(f"{full_path}/*.parquet", hive_partitioning=True)
            all_data.create_view("wide_orders_all", replace=True)
            latest = self.conn.execute("SELECT max(etl_inserted) FROM wide_orders_all").fetchone()[0]
            relation = self.conn.from_query(
                f"SELECT * FROM wide_orders_all WHERE etl_inserted = '{latest}'"
            )

        return ETLDataSet(
            name=self.name,
            curr_data=relation,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
