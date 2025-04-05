from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet
from analytics.utils.duck_etl_base import Table
from analytics.etl.bronze.part import PartBronze
from analytics.etl.bronze.partsupp import PartSuppBronze
from analytics.etl.bronze.supplier import SupplierBronze
from analytics.etl.bronze.nation import NationBronze
from analytics.etl.bronze.region import RegionBronze


class DimPartSilver(Table):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[Table]]] = [
            PartBronze,
            PartSuppBronze,
            SupplierBronze,
            NationBronze,
            RegionBronze,
        ],
        name: str = 'dim_part',
        primary_keys: List[str] = ['part_key'],
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
        part = upstream_datasets[0].curr_data
        partsupp = upstream_datasets[1].curr_data
        supplier = upstream_datasets[2].curr_data
        nation = upstream_datasets[3].curr_data
        region = upstream_datasets[4].curr_data

        part.create_view("part", replace=True)
        partsupp.create_view("partsupp", replace=True)
        supplier.create_view("supplier", replace=True)
        nation.create_view("nation", replace=True)
        region.create_view("region", replace=True)

        etl_date = datetime.now().isoformat()

        transformed_query = f"""
        WITH geo_data AS (
            SELECT
                n.n_nationkey,
                n.n_name AS manufacturing_country,
                r.r_name AS manufacturing_region
            FROM nation n
            LEFT JOIN region r ON n.n_regionkey = r.r_regionkey
        ),
        supplier_enriched AS (
            SELECT
                s.s_suppkey,
                s.s_name AS supplier_name,
                s.s_acctbal AS supplier_account_balance,
                g.manufacturing_country,
                g.manufacturing_region
            FROM supplier s
            LEFT JOIN geo_data g ON s.s_nationkey = g.n_nationkey
        ),
        part_transformed AS (
            SELECT
                p.p_partkey AS part_key,
                p.p_name AS part_name,
                p.p_mfgr AS manufacturer,
                p.p_brand AS brand,
                p.p_type AS product_type,
                p.p_size AS size,
                p.p_container AS container_type,
                p.p_type AS category,
                p.p_container AS packaging,
                p.p_retailprice AS retail_price,
                ROUND(p.p_retailprice * 0.8, 2) AS wholesale_price
            FROM part p
        )
        SELECT
            pt.*,
            ps.ps_supplycost AS supply_cost,
            ps.ps_availqty AS available_qty,
            se.supplier_name,
            se.supplier_account_balance,
            se.manufacturing_country,
            se.manufacturing_region,
            '{etl_date}' AS etl_inserted
        FROM part_transformed pt
        LEFT JOIN partsupp ps ON pt.part_key = ps.ps_partkey
        LEFT JOIN supplier_enriched se ON ps.ps_suppkey = se.s_suppkey
        """

        result = self.conn.from_query(transformed_query)
        self.curr_data = result

        return ETLDataSet(
            name=self.name,
            curr_data=result,
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
