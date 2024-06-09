from kedro.pipeline import Pipeline, node
from .nodes import save_data_to_s3

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=save_data_to_s3,
                inputs=["reservaciones_dashboard", "features_dashboard", "features_model", "model_data"],
                outputs="s3_upload_status",
                name="save_tables_to_s3_node"
            )
        ]
    )
