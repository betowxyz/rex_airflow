import kfp
import kfp.dsl as dsl
from kfp import components

preprocess_op = components.load_component_from_file(
    '/home/roberto-stone/Code/rex_challenge/kubeflow/components/preprocess/preprocess.yaml'
    )

@dsl.pipeline(
  name='Stroke Data ML Pipeline',
  description='A pipeline to pre-process data and train model to predict stroke.'
)
def stroke_pipeline():
    pre_processment = preprocess_op()

if __name__ == '__main__':
    kfp.compiler.Compiler().compile(stroke_pipeline, __file__ + '.tar.gz')
