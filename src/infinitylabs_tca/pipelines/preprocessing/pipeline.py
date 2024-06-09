from kedro.pipeline import Pipeline, node
from .nodes import process_dataframes

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=process_dataframes,
                inputs=[
                    "agencias",
                    "canales",
                    "empresas",
                    "estatus_reservaciones",
                    "paises",
                    "paquetes",
                    "reservaciones",
                    "segmentos_comp",
                    "tipos_habitaciones"
                ],
                outputs=["reservaciones_dashboard", "features_dashboard", "features_model"],
                name="process_tables_node"
            )
        ]
    )
