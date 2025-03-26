from datetime import datetime
from typing import Dict, List, Optional, Type

import duckdb
from analytics.utils.etl_dataset import ETLDataSet 
from analytics.utils.duck_etl_base import TableETL
from analytics.etl.bronze.part import PartBronzeETL
from analytics.etl.bronze.partsupp import PartSuppBronzeETL
from analytics.etl.bronze.supplier import SupplierBronzeETL
from analytics.etl.bronze.nation import NationBronzeETL
from analytics.etl.bronze.region import RegionBronzeETL


class DimPartSilverETL(TableETL):
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            PartBronzeETL,
            PartSuppBronzeETL,
            SupplierBronzeETL,
            NationBronzeETL,
            RegionBronzeETL,
        ],
        name: str = 'dim_part',
        primary_keys: List[str] = ['part_key'],
        storage_path: str = 's3://duckdb-bucket-tpch/silver/dim_part',
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
        part = upstream_datasets[0].curr_data
        partsupp = upstream_datasets[1].curr_data
        supplier = upstream_datasets[2].curr_data
        nation = upstream_datasets[3].curr_data
        region = upstream_datasets[4].curr_data

        # Register all upstream datasets as views
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
            all_data.create_view("dim_part_all", replace=True)

            latest = self.conn.execute("SELECT max(etl_inserted) FROM dim_part_all").fetchone()[0]
            relation = self.conn.from_query(
                f"SELECT * FROM dim_part_all WHERE etl_inserted = '{latest}'"
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
