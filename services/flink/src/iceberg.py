import os 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, TimestampType, NestedField
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

dns = os.environ['DNS']

def initialize():
    warehouse_path = "s3a://warehouse/flink"

    catalog = RestCatalog(
        "default",
        **{
            "uri": f"http://{dns}:8181/",
            "warehouse": warehouse_path,
            "s3.endpoint": "http://minio:9000",
            "s3.access-key": "admin",
            "s3.secret-key": "password",
            "s3.path-style-access": True
        },
    )

    catalog.create_namespace_if_not_exists('flink', properties={}) 
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
        NestedField(field_id=3, name="ts", field_type=TimestampType(), required=True),
    )

    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
    catalog.create_table_if_not_exists(identifier="docs_example.bids",
        schema=schema,
        location="s3a://warehouse/flink/lor",
        sort_order=sort_order
    )



# # Create table environment
# env_settings = EnvironmentSettings.in_streaming_mode()
# table_env = TableEnvironment.create(env_settings)
# # Use the catalog as current catalog
# table_env.use_catalog('default')

if __name__ == '__main__':
    initialize()