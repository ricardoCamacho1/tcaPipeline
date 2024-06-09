from kedro.pipeline import Pipeline, node
from .nodes import save_tables

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=save_tables,
                inputs=["params:table_names"],
                outputs=['agencias', 
                         'canales', 
                         'empresas', 
                         'estatus_reservaciones', 
                         'paises', 
                         'paquetes', 
                         'reservaciones', 
                         'segmentos_comp', 
                         'tipos_habitaciones'
                         ],
                name="save_tables_node"
            )
        ]
    )
