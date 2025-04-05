from datetime import date
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_etl_base import Table
from analytics.etl.bronze.orders import OrdersBronze
from analytics.etl.bronze.lineitem import LineItemBronze


class FctOrdersSilver(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = [
            OrdersBronze,
            LineItemBronze,
        ],
        name: str = 'fct_orders',
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
        lineitem = upstream_datasets[1].curr_data

        orders.create_view("orders", replace=True)
        lineitem.create_view("lineitem", replace=True)

        etl_date = date.today().isoformat()

        query = f"""
        SELECT
            o.o_orderkey AS order_key,
            l.l_linenumber AS line_number,
            o.o_custkey AS customer_key,
            l.l_partkey AS part_key,
            l.l_suppkey AS supplier_key,
            o.o_orderdate AS order_date,
            l.l_shipdate AS ship_date,
            l.l_commitdate AS commit_date,
            l.l_receiptdate AS receipt_date,
            o.o_totalprice AS order_total_amount,
            o.o_orderpriority AS order_priority,
            o.o_orderstatus AS order_status,
            l.l_quantity AS quantity,
            l.l_extendedprice AS extended_price,
            l.l_discount AS discount_percentage,
            l.l_tax AS tax_percentage,
            l.l_shipmode AS ship_mode,
            l.l_returnflag AS return_flag,
            l.l_linestatus AS line_status,

            -- Calculs
            ROUND(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax), 2) AS net_amount,
            ROUND(l.l_extendedprice * l.l_discount, 2) AS discount_amount,
            ROUND(l.l_extendedprice * (1 - l.l_discount) * l.l_tax, 2) AS tax_amount,

            -- Délais
            DATE_DIFF('day', o.o_orderdate, l.l_shipdate) AS shipping_delay_days,
            DATE_DIFF('day', l.l_shipdate, l.l_receiptdate) AS delivery_delay_days,

            -- Livraison en retard
            l.l_receiptdate > l.l_commitdate AS is_late_delivery,

            '{etl_date}' AS etl_inserted

        FROM lineitem l
        INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
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
