from kedro.pipeline import Pipeline, node
from .nodes import model

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=model,
                inputs=["features_model"],
                outputs="model_data",
                name="model_node"

            )
        ]
    )
