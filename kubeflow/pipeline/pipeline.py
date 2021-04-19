import kfp
import kfp.dsl as dsl

def preprocess_op():

    return dsl.ContainerOp(
        name='Preprocess Data',
        # image='kubeflow_pipeline_preprocess:latest' # !Update,
        arguments=[],
        file_outputs={}
    )

@dsl.pipeline(
  name='Stroke Data ML Pipeline',
  description='A pipeline to pre-process data and train model to predict stroke.'
)
def stroke_pipeline():
    pre_processment = preprocess_op()

client = kfp.Client()
client.creat_run_from_pipeline_func(stroke_pipeline, arguments={})
