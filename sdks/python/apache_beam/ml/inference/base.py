#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# TODO: https://github.com/apache/beam/issues/21822
# mypy: ignore-errors

"""An extensible run inference transform.

Users of this module can extend the ModelHandler class for any machine learning
framework. A ModelHandler implementation is a required parameter of
RunInference.

The transform handles standard inference functionality, like metric
collection, sharing model between threads, and batching elements.
"""

import logging
import os
import pickle
import sys
import threading
import time
import uuid
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

import apache_beam as beam
from apache_beam.utils import multi_process_shared
from apache_beam.utils import shared

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  import resource
except ImportError:
  resource = None  # type: ignore[assignment]

_NANOSECOND_TO_MILLISECOND = 1_000_000
_NANOSECOND_TO_MICROSECOND = 1_000

ModelT = TypeVar('ModelT')
ExampleT = TypeVar('ExampleT')
PreProcessT = TypeVar('PreProcessT')
PredictionT = TypeVar('PredictionT')
PostProcessT = TypeVar('PostProcessT')
_INPUT_TYPE = TypeVar('_INPUT_TYPE')
_OUTPUT_TYPE = TypeVar('_OUTPUT_TYPE')
KeyT = TypeVar('KeyT')


# We use NamedTuple to define the structure of the PredictionResult,
# however, as support for generic NamedTuples is not available in Python
# versions prior to 3.11, we use the __new__ method to provide default
# values for the fields while maintaining backwards compatibility.
class PredictionResult(NamedTuple('PredictionResult',
                                  [('example', _INPUT_TYPE),
                                   ('inference', _OUTPUT_TYPE),
                                   ('model_id', Optional[str])])):
  __slots__ = ()

  def __new__(cls, example, inference, model_id=None):
    return super().__new__(cls, example, inference, model_id)


PredictionResult.__doc__ = """A NamedTuple containing both input and output
  from the inference."""
PredictionResult.example.__doc__ = """The input example."""
PredictionResult.inference.__doc__ = """Results for the inference on the model
  for the given example."""
PredictionResult.model_id.__doc__ = """Model ID used to run the prediction."""


class ModelMetadata(NamedTuple):
  model_id: str
  model_name: str


class RunInferenceDLQ(NamedTuple):
  failed_inferences: beam.PCollection
  failed_preprocessing: Sequence[beam.PCollection]
  failed_postprocessing: Sequence[beam.PCollection]


ModelMetadata.model_id.__doc__ = """Unique identifier for the model. This can be
    a file path or a URL where the model can be accessed. It is used to load
    the model for inference."""
ModelMetadata.model_name.__doc__ = """Human-readable name for the model. This
    can be used to identify the model in the metrics generated by the
    RunInference transform."""


def _to_milliseconds(time_ns: int) -> int:
  return int(time_ns / _NANOSECOND_TO_MILLISECOND)


def _to_microseconds(time_ns: int) -> int:
  return int(time_ns / _NANOSECOND_TO_MICROSECOND)


class ModelHandler(Generic[ExampleT, PredictionT, ModelT]):
  """Has the ability to load and apply an ML model."""
  def __init__(self):
    """Environment variables are set using a dict named 'env_vars' before
    loading the model. Child classes can accept this dict as a kwarg."""
    self._env_vars = {}

  def load_model(self) -> ModelT:
    """Loads and initializes a model for processing."""
    raise NotImplementedError(type(self))

  def run_inference(
      self,
      batch: Sequence[ExampleT],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None) -> Iterable[PredictionT]:
    """Runs inferences on a batch of examples.

    Args:
      batch: A sequence of examples or features.
      model: The model used to make inferences.
      inference_args: Extra arguments for models whose inference call requires
        extra parameters.

    Returns:
      An Iterable of Predictions.
    """
    raise NotImplementedError(type(self))

  def get_num_bytes(self, batch: Sequence[ExampleT]) -> int:
    """
    Returns:
       The number of bytes of data for a batch.
    """
    return len(pickle.dumps(batch))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'RunInference'

  def get_resource_hints(self) -> dict:
    """
    Returns:
       Resource hints for the transform.
    """
    return {}

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """
    Returns:
       kwargs suitable for beam.BatchElements.
    """
    return {}

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    """Validates inference_args passed in the inference call.

    Because most frameworks do not need extra arguments in their predict() call,
    the default behavior is to error out if inference_args are present.
    """
    if inference_args:
      raise ValueError(
          'inference_args were provided, but should be None because this '
          'framework does not expect extra arguments on inferences.')

  def update_model_path(self, model_path: Optional[str] = None):
    """Update the model paths produced by side inputs."""
    pass

  def get_preprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    """Gets all preprocessing functions to be run before batching/inference.
    Functions are in order that they should be applied."""
    return []

  def get_postprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    """Gets all postprocessing functions to be run after inference.
    Functions are in order that they should be applied."""
    return []

  def set_environment_vars(self):
    """Sets environment variables using a dictionary provided via kwargs.
    Keys are the env variable name, and values are the env variable value.
    Child ModelHandler classes should set _env_vars via kwargs in __init__,
    or else call super().__init__()."""
    env_vars = getattr(self, '_env_vars', {})
    for env_variable, env_value in env_vars.items():
      os.environ[env_variable] = env_value

  def with_preprocess_fn(
      self, fn: Callable[[PreProcessT], ExampleT]
  ) -> 'ModelHandler[PreProcessT, PredictionT, ModelT, PreProcessT]':
    """Returns a new ModelHandler with a preprocessing function
    associated with it. The preprocessing function will be run
    before batching/inference and should map your input PCollection
    to the base ModelHandler's input type. If you apply multiple
    preprocessing functions, they will be run on your original
    PCollection in order from last applied to first applied."""
    return _PreProcessingModelHandler(self, fn)

  def with_postprocess_fn(
      self, fn: Callable[[PredictionT], PostProcessT]
  ) -> 'ModelHandler[ExampleT, PostProcessT, ModelT, PostProcessT]':
    """Returns a new ModelHandler with a postprocessing function
    associated with it. The postprocessing function will be run
    after inference and should map the base ModelHandler's output
    type to your desired output type. If you apply multiple
    postprocessing functions, they will be run on your original
    inference result in order from first applied to last applied."""
    return _PostProcessingModelHandler(self, fn)

  def share_model_across_processes(self) -> bool:
    """Returns a boolean representing whether or not a model should
    be shared across multiple processes instead of being loaded per process.
    This is primary useful for large models that  can't fit multiple copies in
    memory. Multi-process support may vary by runner, but this will fallback to
    loading per process as necessary. See
    https://beam.apache.org/releases/pydoc/current/apache_beam.utils.multi_process_shared.html"""
    return False


class KeyedModelHandler(Generic[KeyT, ExampleT, PredictionT, ModelT],
                        ModelHandler[Tuple[KeyT, ExampleT],
                                     Tuple[KeyT, PredictionT],
                                     ModelT]):
  def __init__(self, unkeyed: ModelHandler[ExampleT, PredictionT, ModelT]):
    """A ModelHandler that takes keyed examples and returns keyed predictions.

    For example, if the original model is used with RunInference to take a
    PCollection[E] to a PCollection[P], this ModelHandler would take a
    PCollection[Tuple[K, E]] to a PCollection[Tuple[K, P]], making it possible
    to use the key to associate the outputs with the inputs.

    Args:
      unkeyed: An implementation of ModelHandler that does not require keys.
    """
    if len(unkeyed.get_preprocess_fns()) or len(unkeyed.get_postprocess_fns()):
      raise Exception(
          'Cannot make make an unkeyed model handler with pre or '
          'postprocessing functions defined into a keyed model handler. All '
          'pre/postprocessing functions must be defined on the outer model'
          'handler.')
    self._unkeyed = unkeyed
    self._env_vars = unkeyed._env_vars

  def load_model(self) -> ModelT:
    return self._unkeyed.load_model()

  def run_inference(
      self,
      batch: Sequence[Tuple[KeyT, ExampleT]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[Tuple[KeyT, PredictionT]]:
    keys, unkeyed_batch = zip(*batch)
    return zip(
        keys, self._unkeyed.run_inference(unkeyed_batch, model, inference_args))

  def get_num_bytes(self, batch: Sequence[Tuple[KeyT, ExampleT]]) -> int:
    keys, unkeyed_batch = zip(*batch)
    return len(pickle.dumps(keys)) + self._unkeyed.get_num_bytes(unkeyed_batch)

  def get_metrics_namespace(self) -> str:
    return self._unkeyed.get_metrics_namespace()

  def get_resource_hints(self):
    return self._unkeyed.get_resource_hints()

  def batch_elements_kwargs(self):
    return self._unkeyed.batch_elements_kwargs()

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    return self._unkeyed.validate_inference_args(inference_args)

  def update_model_path(self, model_path: Optional[str] = None):
    return self._unkeyed.update_model_path(model_path=model_path)

  def get_preprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._unkeyed.get_preprocess_fns()

  def get_postprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._unkeyed.get_postprocess_fns()

  def share_model_across_processes(self) -> bool:
    return self._unkeyed.share_model_across_processes()


class MaybeKeyedModelHandler(Generic[KeyT, ExampleT, PredictionT, ModelT],
                             ModelHandler[Union[ExampleT, Tuple[KeyT,
                                                                ExampleT]],
                                          Union[PredictionT,
                                                Tuple[KeyT, PredictionT]],
                                          ModelT]):
  def __init__(self, unkeyed: ModelHandler[ExampleT, PredictionT, ModelT]):
    """A ModelHandler that takes examples that might have keys and returns
    predictions that might have keys.

    For example, if the original model is used with RunInference to take a
    PCollection[E] to a PCollection[P], this ModelHandler would take either
    PCollection[E] to a PCollection[P] or PCollection[Tuple[K, E]] to a
    PCollection[Tuple[K, P]], depending on the whether the elements are
    tuples. This pattern makes it possible to associate the outputs with the
    inputs based on the key.

    Note that you cannot use this ModelHandler if E is a tuple type.
    In addition, either all examples should be keyed, or none of them.

    Args:
      unkeyed: An implementation of ModelHandler that does not require keys.
    """
    if len(unkeyed.get_preprocess_fns()) or len(unkeyed.get_postprocess_fns()):
      raise Exception(
          'Cannot make make an unkeyed model handler with pre or '
          'postprocessing functions defined into a keyed model handler. All '
          'pre/postprocessing functions must be defined on the outer model'
          'handler.')
    self._unkeyed = unkeyed
    self._env_vars = unkeyed._env_vars

  def load_model(self) -> ModelT:
    return self._unkeyed.load_model()

  def run_inference(
      self,
      batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Union[Iterable[PredictionT], Iterable[Tuple[KeyT, PredictionT]]]:
    # Really the input should be
    #    Union[Sequence[ExampleT], Sequence[Tuple[KeyT, ExampleT]]]
    # but there's not a good way to express (or check) that.
    if isinstance(batch[0], tuple):
      is_keyed = True
      keys, unkeyed_batch = zip(*batch)  # type: ignore[arg-type]
    else:
      is_keyed = False
      unkeyed_batch = batch  # type: ignore[assignment]
    unkeyed_results = self._unkeyed.run_inference(
        unkeyed_batch, model, inference_args)
    if is_keyed:
      return zip(keys, unkeyed_results)
    else:
      return unkeyed_results

  def get_num_bytes(
      self, batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]]) -> int:
    # MyPy can't follow the branching logic.
    if isinstance(batch[0], tuple):
      keys, unkeyed_batch = zip(*batch)  # type: ignore[arg-type]
      return len(
          pickle.dumps(keys)) + self._unkeyed.get_num_bytes(unkeyed_batch)
    else:
      return self._unkeyed.get_num_bytes(batch)  # type: ignore[arg-type]

  def get_metrics_namespace(self) -> str:
    return self._unkeyed.get_metrics_namespace()

  def get_resource_hints(self):
    return self._unkeyed.get_resource_hints()

  def batch_elements_kwargs(self):
    return self._unkeyed.batch_elements_kwargs()

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    return self._unkeyed.validate_inference_args(inference_args)

  def update_model_path(self, model_path: Optional[str] = None):
    return self._unkeyed.update_model_path(model_path=model_path)

  def get_preprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._unkeyed.get_preprocess_fns()

  def get_postprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._unkeyed.get_postprocess_fns()

  def share_model_across_processes(self) -> bool:
    return self._unkeyed.share_model_across_processes()


class _PreProcessingModelHandler(Generic[ExampleT,
                                         PredictionT,
                                         ModelT,
                                         PreProcessT],
                                 ModelHandler[PreProcessT, PredictionT,
                                              ModelT]):
  def __init__(
      self,
      base: ModelHandler[ExampleT, PredictionT, ModelT],
      preprocess_fn: Callable[[PreProcessT], ExampleT]):
    """A ModelHandler that has a preprocessing function associated with it.

    Args:
      base: An implementation of the underlying model handler.
      preprocess_fn: the preprocessing function to use.
    """
    self._base = base
    self._env_vars = base._env_vars
    self._preprocess_fn = preprocess_fn

  def load_model(self) -> ModelT:
    return self._base.load_model()

  def run_inference(
      self,
      batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Union[Iterable[PredictionT], Iterable[Tuple[KeyT, PredictionT]]]:
    return self._base.run_inference(batch, model, inference_args)

  def get_num_bytes(
      self, batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]]) -> int:
    return self._base.get_num_bytes(batch)

  def get_metrics_namespace(self) -> str:
    return self._base.get_metrics_namespace()

  def get_resource_hints(self):
    return self._base.get_resource_hints()

  def batch_elements_kwargs(self):
    return self._base.batch_elements_kwargs()

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    return self._base.validate_inference_args(inference_args)

  def update_model_path(self, model_path: Optional[str] = None):
    return self._base.update_model_path(model_path=model_path)

  def get_preprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return [self._preprocess_fn] + self._base.get_preprocess_fns()

  def get_postprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._base.get_postprocess_fns()


class _PostProcessingModelHandler(Generic[ExampleT,
                                          PredictionT,
                                          ModelT,
                                          PostProcessT],
                                  ModelHandler[ExampleT, PostProcessT, ModelT]):
  def __init__(
      self,
      base: ModelHandler[ExampleT, PredictionT, ModelT],
      postprocess_fn: Callable[[PredictionT], PostProcessT]):
    """A ModelHandler that has a preprocessing function associated with it.

    Args:
      base: An implementation of the underlying model handler.
      postprocess_fn: the preprocessing function to use.
    """
    self._base = base
    self._env_vars = base._env_vars
    self._postprocess_fn = postprocess_fn

  def load_model(self) -> ModelT:
    return self._base.load_model()

  def run_inference(
      self,
      batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Union[Iterable[PredictionT], Iterable[Tuple[KeyT, PredictionT]]]:
    return self._base.run_inference(batch, model, inference_args)

  def get_num_bytes(
      self, batch: Sequence[Union[ExampleT, Tuple[KeyT, ExampleT]]]) -> int:
    return self._base.get_num_bytes(batch)

  def get_metrics_namespace(self) -> str:
    return self._base.get_metrics_namespace()

  def get_resource_hints(self):
    return self._base.get_resource_hints()

  def batch_elements_kwargs(self):
    return self._base.batch_elements_kwargs()

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    return self._base.validate_inference_args(inference_args)

  def update_model_path(self, model_path: Optional[str] = None):
    return self._base.update_model_path(model_path=model_path)

  def get_preprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._base.get_preprocess_fns()

  def get_postprocess_fns(self) -> Iterable[Callable[[Any], Any]]:
    return self._base.get_postprocess_fns() + [self._postprocess_fn]


class RunInference(beam.PTransform[beam.PCollection[ExampleT],
                                   beam.PCollection[PredictionT]]):
  def __init__(
      self,
      model_handler: ModelHandler[ExampleT, PredictionT, Any],
      clock=time,
      inference_args: Optional[Dict[str, Any]] = None,
      metrics_namespace: Optional[str] = None,
      *,
      model_metadata_pcoll: beam.PCollection[ModelMetadata] = None,
      watch_model_pattern: Optional[str] = None,
      **kwargs):
    """
    A transform that takes a PCollection of examples (or features) for use
    on an ML model. The transform then outputs inferences (or predictions) for
    those examples in a PCollection of PredictionResults that contains the input
    examples and the output inferences.

    Models for supported frameworks can be loaded using a URI. Supported
    services can also be used.

    This transform attempts to batch examples using the beam.BatchElements
    transform. Batching can be configured using the ModelHandler.

    Args:
        model_handler: An implementation of ModelHandler.
        clock: A clock implementing time_ns. *Used for unit testing.*
        inference_args: Extra arguments for models whose inference call requires
          extra parameters.
        metrics_namespace: Namespace of the transform to collect metrics.
        model_metadata_pcoll: PCollection that emits Singleton ModelMetadata
          containing model path and model name, that is used as a side input
          to the _RunInferenceDoFn.
        watch_model_pattern: A glob pattern used to watch a directory
          for automatic model refresh.
    """
    self._model_handler = model_handler
    self._inference_args = inference_args
    self._clock = clock
    self._metrics_namespace = metrics_namespace
    self._model_metadata_pcoll = model_metadata_pcoll
    self._enable_side_input_loading = self._model_metadata_pcoll is not None
    self._with_exception_handling = False
    self._watch_model_pattern = watch_model_pattern
    self._kwargs = kwargs
    # Generate a random tag to use for shared.py and multi_process_shared.py to
    # allow us to effectively disambiguate in multi-model settings.
    self._model_tag = uuid.uuid4().hex

  def _get_model_metadata_pcoll(self, pipeline):
    # avoid circular imports.
    # pylint: disable=wrong-import-position
    from apache_beam.ml.inference.utils import WatchFilePattern
    extra_params = {}
    if 'interval' in self._kwargs:
      extra_params['interval'] = self._kwargs['interval']
    if 'stop_timestamp' in self._kwargs:
      extra_params['stop_timestamp'] = self._kwargs['stop_timestamp']

    return (
        pipeline | WatchFilePattern(
            file_pattern=self._watch_model_pattern, **extra_params))

  # TODO(BEAM-14046): Add and link to help documentation.
  @classmethod
  def from_callable(cls, model_handler_provider, **kwargs):
    """Multi-language friendly constructor.

    Use this constructor with fully_qualified_named_transform to
    initialize the RunInference transform from PythonCallableSource provided
    by foreign SDKs.

    Args:
      model_handler_provider: A callable object that returns ModelHandler.
      kwargs: Keyword arguments for model_handler_provider.
    """
    return cls(model_handler_provider(**kwargs))

  def _apply_fns(
      self,
      pcoll: beam.PCollection,
      fns: Iterable[Callable[[Any], Any]],
      step_prefix: str) -> Tuple[beam.PCollection, Iterable[beam.PCollection]]:
    bad_preprocessed = []
    for idx in range(len(fns)):
      fn = fns[idx]
      if self._with_exception_handling:
        pcoll, bad = (pcoll
        | f"{step_prefix}-{idx}" >> beam.Map(
          fn).with_exception_handling(
          exc_class=self._exc_class,
          use_subprocess=self._use_subprocess,
          threshold=self._threshold))
        bad_preprocessed.append(bad)
      else:
        pcoll = pcoll | f"{step_prefix}-{idx}" >> beam.Map(fn)

    return pcoll, bad_preprocessed

  # TODO(https://github.com/apache/beam/issues/21447): Add batch_size back off
  # in the case there are functional reasons large batch sizes cannot be
  # handled.
  def expand(
      self, pcoll: beam.PCollection[ExampleT]) -> beam.PCollection[PredictionT]:
    self._model_handler.validate_inference_args(self._inference_args)
    # DLQ pcollections
    bad_preprocessed = []
    bad_inference = None
    bad_postprocessed = []
    preprocess_fns = self._model_handler.get_preprocess_fns()
    postprocess_fns = self._model_handler.get_postprocess_fns()

    pcoll, bad_preprocessed = self._apply_fns(
      pcoll, preprocess_fns, 'BeamML_RunInference_Preprocess')

    resource_hints = self._model_handler.get_resource_hints()

    # check for the side input
    if self._watch_model_pattern:
      self._model_metadata_pcoll = self._get_model_metadata_pcoll(
          pcoll.pipeline)

    batched_elements_pcoll = (
        pcoll
        # TODO(https://github.com/apache/beam/issues/21440): Hook into the
        # batching DoFn APIs.
        | beam.BatchElements(**self._model_handler.batch_elements_kwargs()))

    run_inference_pardo = beam.ParDo(
        _RunInferenceDoFn(
            self._model_handler,
            self._clock,
            self._metrics_namespace,
            self._enable_side_input_loading,
            self._model_tag),
        self._inference_args,
        beam.pvalue.AsSingleton(
            self._model_metadata_pcoll,
        ) if self._enable_side_input_loading else None).with_resource_hints(
            **resource_hints)

    if self._with_exception_handling:
      results, bad_inference = (
          batched_elements_pcoll
          | 'BeamML_RunInference' >>
          run_inference_pardo.with_exception_handling(
          exc_class=self._exc_class,
          use_subprocess=self._use_subprocess,
          threshold=self._threshold))
    else:
      results = (
          batched_elements_pcoll
          | 'BeamML_RunInference' >> run_inference_pardo)

    results, bad_postprocessed = self._apply_fns(
      results, postprocess_fns, 'BeamML_RunInference_Postprocess')

    if self._with_exception_handling:
      dlq = RunInferenceDLQ(bad_inference, bad_preprocessed, bad_postprocessed)
      return results, dlq

    return results

  def with_exception_handling(
      self, *, exc_class=Exception, use_subprocess=False, threshold=1):
    """Automatically provides a dead letter output for skipping bad records.
    This can allow a pipeline to continue successfully rather than fail or
    continuously throw errors on retry when bad elements are encountered.

    This returns a tagged output with two PCollections, the first being the
    results of successfully processing the input PCollection, and the second
    being the set of bad batches of records (those which threw exceptions
    during processing) along with information about the errors raised.

    For example, one would write::

        main, other = RunInference(
          maybe_error_raising_model_handler
        ).with_exception_handling()

    and `main` will be a PCollection of PredictionResults and `other` will
    contain a `RunInferenceDLQ` object with PCollections containing failed
    records for each failed inference, preprocess operation, or postprocess
    operation. To access each collection of failed records, one would write:

        failed_inferences = other.failed_inferences
        failed_preprocessing = other.failed_preprocessing
        failed_postprocessing = other.failed_postprocessing

    failed_inferences is in the form
    PCollection[Tuple[failed batch, exception]].

    failed_preprocessing is in the form
    list[PCollection[Tuple[failed record, exception]]]], where each element of
    the list corresponds to a preprocess function. These PCollections are
    in the same order that the preprocess functions are applied.

    failed_postprocessing is in the form
    List[PCollection[Tuple[failed record, exception]]]], where each element of
    the list corresponds to a postprocess function. These PCollections are
    in the same order that the postprocess functions are applied.


    Args:
      exc_class: An exception class, or tuple of exception classes, to catch.
          Optional, defaults to 'Exception'.
      use_subprocess: Whether to execute the DoFn logic in a subprocess. This
          allows one to recover from errors that can crash the calling process
          (e.g. from an underlying library causing a segfault), but is
          slower as elements and results must cross a process boundary.  Note
          that this starts up a long-running process that is used to handle
          all the elements (until hard failure, which should be rare) rather
          than a new process per element, so the overhead should be minimal
          (and can be amortized if there's any per-process or per-bundle
          initialization that needs to be done). Optional, defaults to False.
      threshold: An upper bound on the ratio of records that can be bad before
          aborting the entire pipeline. Optional, defaults to 1.0 (meaning
          up to 100% of records can be bad and the pipeline will still succeed).
    """
    self._with_exception_handling = True
    self._exc_class = exc_class
    self._use_subprocess = use_subprocess
    self._threshold = threshold
    return self


class _ModelManager:
  """A class for efficiently managing copies of multiple models. TODO - add more detail"""
  def __init__(self, mh_map: Dict[str, ModelHandler], max_models: Optional[int]=None):
    """
    Args:
      mh_map: A map from keys to model handlers which can be used to load a model.
      max_models: The maximum number of models to load at any given time before evicting 1 from memory (using LRU logic). Leave as None to allow unlimited models.
    """
    self._max_models = max_models
    self._mh_map = mh_map
    self._loaded_keys = []
    self._proxy_map = {}
    self._tag_map = {}

  def _get_tag(self, key: str) -> str:
    """
    Args:
      key: the key associated with the model we'd like to load.
    Returns:
      the tag we can use to load the model with a multi_process_shared.py handle.
    """
    if key not in self._tag_map:
      self._tag_map[key] = uuid.uuid4().hex
    return self._tag_map[key]

  def load(self, key: str) -> str:
    """
    Loads the appropriate model for the given key into memory.
    Args:
      key: the key associated with the model we'd like to load.
    Returns:
      the tag we can use to access the model with a multi_process_shared.py handle.
    """
    tag = self._get_tag(key)
    if key in self._loaded_keys:
      if self._max_models is not None:
        # move key to the back of the list
        self._loaded_keys.append(self._loaded_keys.pop(self._loaded_keys.index(key)))
      return tag
    
    mh = self._mh_map[key]
    if self._max_models is not None and self._max_models <= len(self._loaded_keys):
      # If we're about to exceed our LRU size, remove the front model from the list from memory.
      key_to_remove = self._loaded_keys[0]
      tag_to_remove = self._get_tag(key_to_remove)
      shared_handle, model_to_remove = self._proxy_map[tag_to_remove]
      shared_handle.release(model_to_remove)
      del self._tag_map[key]
    
    # Load the new model
    shared_handle = multi_process_shared.MultiProcessShared(mh.load_model, tag=tag)
    model_reference = shared_handle.acquire()
    self._proxy_map[tag] = (shared_handle, model_reference)
    self._loaded_keys.append(key)

    return tag
  
  def increment_max_models(self, increment: int):
    """
    Increments the number of models that this instance of a _ModelManager is able to hold.
    Args:
      increment: the amount by which we are incrementing the number of models.
    """
    if self._max_models is None:
      raise ValueError("Cannot increment max_models if self._max_models is None (unlimited models mode).")
    self._max_models += increment
      


class _MetricsCollector:
  """A metrics collector that tracks ML related performance and memory usage."""
  def __init__(self, namespace: str, prefix: str = ''):
    """
    Args:
     namespace: Namespace for the metrics.
     prefix: Unique identifier for metrics, used when models
      are updated using side input.
    """
    # Metrics
    if prefix:
      prefix = f'{prefix}_'
    self._inference_counter = beam.metrics.Metrics.counter(
        namespace, prefix + 'num_inferences')
    self.failed_batches_counter = beam.metrics.Metrics.counter(
        namespace, prefix + 'failed_batches_counter')
    self._inference_request_batch_size = beam.metrics.Metrics.distribution(
        namespace, prefix + 'inference_request_batch_size')
    self._inference_request_batch_byte_size = (
        beam.metrics.Metrics.distribution(
            namespace, prefix + 'inference_request_batch_byte_size'))
    # Batch inference latency in microseconds.
    self._inference_batch_latency_micro_secs = (
        beam.metrics.Metrics.distribution(
            namespace, prefix + 'inference_batch_latency_micro_secs'))
    self._model_byte_size = beam.metrics.Metrics.distribution(
        namespace, prefix + 'model_byte_size')
    # Model load latency in milliseconds.
    self._load_model_latency_milli_secs = beam.metrics.Metrics.distribution(
        namespace, prefix + 'load_model_latency_milli_secs')

    # Metrics cache
    self._load_model_latency_milli_secs_cache = None
    self._model_byte_size_cache = None

  def update_metrics_with_cache(self):
    if self._load_model_latency_milli_secs_cache is not None:
      self._load_model_latency_milli_secs.update(
          self._load_model_latency_milli_secs_cache)
      self._load_model_latency_milli_secs_cache = None
    if self._model_byte_size_cache is not None:
      self._model_byte_size.update(self._model_byte_size_cache)
      self._model_byte_size_cache = None

  def cache_load_model_metrics(self, load_model_latency_ms, model_byte_size):
    self._load_model_latency_milli_secs_cache = load_model_latency_ms
    self._model_byte_size_cache = model_byte_size

  def update(
      self,
      examples_count: int,
      examples_byte_size: int,
      latency_micro_secs: int):
    self._inference_batch_latency_micro_secs.update(latency_micro_secs)
    self._inference_counter.inc(examples_count)
    self._inference_request_batch_size.update(examples_count)
    self._inference_request_batch_byte_size.update(examples_byte_size)


class _RunInferenceDoFn(beam.DoFn, Generic[ExampleT, PredictionT]):
  def __init__(
      self,
      model_handler: ModelHandler[ExampleT, PredictionT, Any],
      clock,
      metrics_namespace,
      enable_side_input_loading: bool = False,
      model_tag: str = "RunInference"):
    """A DoFn implementation generic to frameworks.

      Args:
        model_handler: An implementation of ModelHandler.
        clock: A clock implementing time_ns. *Used for unit testing.*
        metrics_namespace: Namespace of the transform to collect metrics.
        enable_side_input_loading: Bool to indicate if model updates
            with side inputs.
        model_tag: Tag to use to disambiguate models in multi-model settings.
    """
    self._model_handler = model_handler
    self._shared_model_handle = shared.Shared()
    self._clock = clock
    self._model = None
    self._metrics_namespace = metrics_namespace
    self._enable_side_input_loading = enable_side_input_loading
    self._side_input_path = None
    self._model_tag = model_tag

  def _load_model(self, side_input_model_path: Optional[str] = None):
    def load():
      """Function for constructing shared LoadedModel."""
      memory_before = _get_current_process_memory_in_bytes()
      start_time = _to_milliseconds(self._clock.time_ns())
      self._model_handler.update_model_path(side_input_model_path)
      model = self._model_handler.load_model()
      end_time = _to_milliseconds(self._clock.time_ns())
      memory_after = _get_current_process_memory_in_bytes()
      load_model_latency_ms = end_time - start_time
      model_byte_size = memory_after - memory_before
      self._metrics_collector.cache_load_model_metrics(
          load_model_latency_ms, model_byte_size)
      return model

    # TODO(https://github.com/apache/beam/issues/21443): Investigate releasing
    # model.
    if self._model_handler.share_model_across_processes():
      model = multi_process_shared.MultiProcessShared(
          load, tag=side_input_model_path or self._model_tag, always_proxy=True).acquire()
    else:
      model = self._shared_model_handle.acquire(
          load, tag=side_input_model_path or self._model_tag)
    # since shared_model_handle is shared across threads, the model path
    # might not get updated in the model handler
    # because we directly get cached weak ref model from shared cache, instead
    # of calling load(). For sanity check, call update_model_path again.
    self._model_handler.update_model_path(side_input_model_path)
    return model

  def get_metrics_collector(self, prefix: str = ''):
    """
    Args:
      prefix: Unique identifier for metrics, used when models
      are updated using side input.
    """
    metrics_namespace = (
        self._metrics_namespace) if self._metrics_namespace else (
            self._model_handler.get_metrics_namespace())
    return _MetricsCollector(metrics_namespace, prefix=prefix)

  def setup(self):
    self._metrics_collector = self.get_metrics_collector()
    self._model_handler.set_environment_vars()
    if not self._enable_side_input_loading:
      self._model = self._load_model()

  def update_model(self, side_input_model_path: Optional[str] = None):
    self._model = self._load_model(side_input_model_path=side_input_model_path)

  def _run_inference(self, batch, inference_args):
    start_time = _to_microseconds(self._clock.time_ns())
    try:
      result_generator = self._model_handler.run_inference(
          batch, self._model, inference_args)
    except BaseException as e:
      self._metrics_collector.failed_batches_counter.inc()
      raise e
    predictions = list(result_generator)

    end_time = _to_microseconds(self._clock.time_ns())
    inference_latency = end_time - start_time
    num_bytes = self._model_handler.get_num_bytes(batch)
    num_elements = len(batch)
    self._metrics_collector.update(num_elements, num_bytes, inference_latency)

    return predictions

  def process(
      self, batch, inference_args, si_model_metadata: Optional[ModelMetadata]):
    """
    When side input is enabled:
      The method checks if the side input model has been updated, and if so,
      updates the model and runs inference on the batch of data. If the
      side input is empty or the model has not been updated, the method
      simply runs inference on the batch of data.
    """
    if si_model_metadata:
      if isinstance(si_model_metadata, beam.pvalue.EmptySideInput):
        self.update_model(side_input_model_path=None)
        return self._run_inference(batch, inference_args)
      elif self._side_input_path != si_model_metadata.model_id:
        self._side_input_path = si_model_metadata.model_id
        self._metrics_collector = self.get_metrics_collector(
            prefix=si_model_metadata.model_name)
        with threading.Lock():
          self.update_model(si_model_metadata.model_id)
          return self._run_inference(batch, inference_args)
    return self._run_inference(batch, inference_args)

  def finish_bundle(self):
    # TODO(https://github.com/apache/beam/issues/21435): Figure out why there
    # is a cache.
    self._metrics_collector.update_metrics_with_cache()


def _is_darwin() -> bool:
  return sys.platform == 'darwin'


def _get_current_process_memory_in_bytes():
  """
  Returns:
    memory usage in bytes.
  """

  if resource is not None:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if _is_darwin():
      return usage
    return usage * 1024
  else:
    logging.warning(
        'Resource module is not available for current platform, '
        'memory usage cannot be fetched.')
  return 0
