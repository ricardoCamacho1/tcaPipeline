"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from infinitylabs_tca.pipelines.loading.pipeline import create_pipeline as create_loading_pipeline
from infinitylabs_tca.pipelines.preprocessing.pipeline import create_pipeline as create_preprocessing_pipeline
from infinitylabs_tca.pipelines.storing.pipeline import create_pipeline as create_save_to_s3_pipeline
from infinitylabs_tca.pipelines.modeling.pipeline import create_pipeline as create_modeling_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # Explicitly create the pipelines
    loading_pipeline = create_loading_pipeline()
    preprocessing_pipeline = create_preprocessing_pipeline()
    save_to_s3_pipeline = create_save_to_s3_pipeline()
    modeling_pipeline = create_modeling_pipeline()

    # Concatenate the pipelines to ensure loading runs before preprocessing
    combined_pipeline = loading_pipeline + preprocessing_pipeline + modeling_pipeline + save_to_s3_pipeline
    
    # Use the concatenated pipeline as the default
    pipelines = find_pipelines()
    pipelines["loading"] = loading_pipeline
    pipelines["preprocessing"] = preprocessing_pipeline
    pipelines["modeling"] = modeling_pipeline
    pipelines["save_to_s3"] = save_to_s3_pipeline
    pipelines["__default__"] = combined_pipeline
    
    return pipelines
