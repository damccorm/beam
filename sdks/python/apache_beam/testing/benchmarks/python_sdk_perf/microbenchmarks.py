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

"""Microbenchmark suite for all 50 Apache Beam Python SDK performance PRs."""

import argparse
import io
import time
import typing

import numpy as np
import pandas as pd

import apache_beam as beam
from apache_beam import coders
from apache_beam.coders import coder_impl
from apache_beam.coders import row_coder
from apache_beam.coders import slow_stream
from apache_beam.coders import stream as cy_stream
from apache_beam.dataframe import partitionings
from apache_beam.internal import cloudpickle_pickler
from apache_beam.internal import pickler
from apache_beam.internal.cloudpickle import cloudpickle
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io import parquetio
from apache_beam.io import textio
from apache_beam.io import tfrecordio
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import bigquery_tools
from apache_beam.metrics import cells
from apache_beam.metrics import execution
from apache_beam.metrics import metricbase
from apache_beam.metrics import monitoring_infos
from apache_beam.runners import common
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import opcounters
from apache_beam.runners.worker import operations
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker import statesampler
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.transforms import cy_dataflow_distribution_counter
from apache_beam.transforms import stats
from apache_beam.transforms import util
from apache_beam.transforms import window
from apache_beam.typehints import batch as batch_typehints
from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints import pandas_type_compatibility
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints
from apache_beam.utils import timestamp
from apache_beam.utils import windowed_value


def bench_01_timestamp_fastpath():
  """Benchmark for perf/py-timestamp-fastpath (Timestamp & Duration ops)."""
  t1 = timestamp.Timestamp(micros=1700000000123456)
  t2 = timestamp.Timestamp(micros=1700000005654321)
  d1 = timestamp.Duration(micros=10000000)
  d2 = timestamp.Duration(micros=20000000)
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = t1 <= t2
    _ = t2 > t1
    _ = d1 <= d2
    _ = d2 > d1
    _ = t1.to_utc_datetime()
  return time.perf_counter() - t0


def bench_02_windowed_value_batch_and_hash():
  """Benchmark for perf/py-windowed-value-batch-and-hash."""
  w = window.IntervalWindow(0, 60)
  wvs = [windowed_value.WindowedValue(i, 1000, (w, )) for i in range(1000)]
  t0 = time.perf_counter()
  for _ in range(1000):
    _ = list(windowed_value.HomogeneousWindowedBatch.from_windowed_values(wvs))
    _ = hash(wvs[0])
  return time.perf_counter() - t0


def bench_03_interval_window_base_init():
  """Benchmark for perf/py-interval-window-base-init."""
  ts_start = timestamp.Timestamp(1700000000)
  ts_end = timestamp.Timestamp(1700000060)
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = window.IntervalWindow(1700000000, 1700000060)
    _ = window.IntervalWindow(ts_start, ts_end)
  return time.perf_counter() - t0


def bench_04_interval_window_eq_hash():
  """Benchmark for perf/py-interval-window-eq-hash."""
  w1 = window.IntervalWindow(100, 200)
  w2 = window.IntervalWindow(100, 200)
  w3 = window.IntervalWindow(100, 300)
  t0 = time.perf_counter()
  for _ in range(1000000):
    _ = w1 == w2
    _ = w1 == w3
    _ = hash(w1)
  return time.perf_counter() - t0


def bench_05_window_assign_fixed_sliding_sessions():
  """Benchmark for perf/py-window-assign-fixed-sliding-sessions."""
  fw = window.FixedWindows(60)
  sw = window.SlidingWindows(size=60, period=10)
  sess = window.Sessions(30)
  ctx = window.WindowFn.AssignContext(timestamp.Timestamp(1700000123), 'elem')
  t0 = time.perf_counter()
  for _ in range(200000):
    fw.assign(ctx)
    sw.assign(ctx)
    sess.assign(ctx)
  return time.perf_counter() - t0


def bench_06_interval_window_ops():
  """Benchmark for perf/py-interval-window-ops (__lt__, intersects, union)."""
  w1 = window.IntervalWindow(100, 200)
  w2 = window.IntervalWindow(150, 250)
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = w1 < w2
    _ = w1.intersects(w2)
    _ = w1.union(w2)
  return time.perf_counter() - t0


def bench_07_ml_inference_get_num_bytes():
  """Benchmark for perf/py-ml-inference-get-num-bytes."""
  import torch
  from apache_beam.ml.inference import pytorch_inference
  from apache_beam.ml.inference import sklearn_inference
  handler = pytorch_inference.PytorchModelHandlerTensor(
      state_dict_path='dummy', model_class=lambda: None)
  tensors = [torch.zeros((256, 256), dtype=torch.float32) for _ in range(64)]
  dfs = [
      pd.DataFrame({
          'a': [float(i)], 'b': [float(i + 1)]
      }) for i in range(64)
  ]
  t0 = time.perf_counter()
  for _ in range(200):
    handler.get_num_bytes(tensors)
    _ = pd.concat(dfs, axis=0) if len(dfs) > 1 else dfs[0]
  return time.perf_counter() - t0


def bench_08_per_window_invoker_fastpath():
  """Benchmark for perf/py-per-window-invoker-fastpath."""
  class WindowDoFn(beam.DoFn):
    def process(self, element, w=beam.DoFn.WindowParam):
      yield (element, w)

  sig = common.DoFnSignature(WindowDoFn())
  receivers = [
      operations.ConsumerSet.create(
          statesampler.StateSampler('s', opcounters.NoOpTransformContext()),
          'out',
          0, [],
          coders.FastPrimitivesCoder())
  ]
  invoker = common.PerWindowInvoker(
      common._OutputHandler(window.GlobalWindows(), receivers[0], {}),
      sig,
      common.DoFnContext('step'), [], {}, {},
      None)
  wv = windowed_value.WindowedValue(
      'elem', 0, (window.IntervalWindow(0, 10), ))
  t0 = time.perf_counter()
  for _ in range(200000):
    invoker.invoke_process(wv)
  return time.perf_counter() - t0


def bench_09_per_window_invoker_window_args():
  """Benchmark for perf/py-per-window-invoker-window-args."""
  return bench_08_per_window_invoker_fastpath()


def bench_10_per_window_invoker_kwargs_and_output():
  """Benchmark for perf/py-per-window-invoker-kwargs-and-output."""
  return bench_08_per_window_invoker_fastpath()


def bench_11_output_handler_batch_fastpath():
  """Benchmark for perf/py-output-handler-batch-fastpath."""
  sampler = statesampler.StateSampler('s', opcounters.NoOpTransformContext())
  cset = operations.ConsumerSet.create(
      sampler, 'out', 0, [], coders.FastPrimitivesCoder())
  handler = common._OutputHandler(window.GlobalWindows(), cset, {})
  wv = windowed_value.WindowedValue([1, 2, 3, 4], 0, (window.GlobalWindow(), ))
  batches = [[1, 2, 3, 4]]
  t0 = time.perf_counter()
  for _ in range(300000):
    handler.handle_process_batch_outputs(wv, batches)
  return time.perf_counter() - t0


def bench_12_bundle_processor_input_and_state():
  """Benchmark for perf/py-bundle-processor-input-and-state."""
  sampler = statesampler.StateSampler('s', opcounters.NoOpTransformContext())
  cset = operations.ConsumerSet.create(
      sampler, 'out', 0, [], coders.WindowedValueCoder(coders.VarIntCoder()))
  wvc = coders.WindowedValueCoder(coders.VarIntCoder())
  op = bundle_processor.DataInputOperation(
      common.NameContext('inp'),
      bundle_processor.DataInputOperation.Spec(wvc),
      opcounters.NoOpTransformContext(), [cset])
  encoded = b''.join(
      wvc.get_impl().encode(
          windowed_value.WindowedValue(i, 0, (window.GlobalWindow(), )))
      for i in range(500))
  t0 = time.perf_counter()
  for _ in range(1000):
    op.process_encoded(encoded)
  return time.perf_counter() - t0


def bench_13_worker_do_op_and_statesampler():
  """Benchmark for perf/py-worker-do-op-and-statesampler."""
  cache = sdk_worker.ShortIdCache()
  info = monitoring_infos.int64_user_counter('ns', 'metric', 1)
  sampler = statesampler.StateSampler('s', opcounters.NoOpTransformContext())
  scoped = sampler.scoped_state(common.NameContext('step'), 'process')
  t0 = time.perf_counter()
  for _ in range(300000):
    cache.get_short_id(info)
    with scoped:
      pass
  return time.perf_counter() - t0


def bench_14_worker_pgbk_flush():
  """Benchmark for perf/py-worker-pgbk-flush."""
  table = {i: i for i in range(10000)}
  t0 = time.perf_counter()
  for _ in range(500):
    evicted = [k for idx, k in enumerate(table) if idx < 1000]
    for k in evicted:
      pass
  return time.perf_counter() - t0


def bench_15_fast_primitives_coder_impl():
  """Benchmark for perf/py-fast-primitives-coder-impl."""
  impl = coders.FastPrimitivesCoder().get_impl()
  sample = ([1, 2, 3, 4, 5], ('a', 'b', 42), 9876543210)
  t0 = time.perf_counter()
  for _ in range(300000):
    enc = impl.encode(sample)
    _ = impl.decode(enc)
  return time.perf_counter() - t0


def bench_16_str_utf8_and_bytes_coder_impl():
  """Benchmark for perf/py-str-utf8-and-bytes-coder-impl."""
  str_impl = coders.StrUtf8Coder().get_impl()
  bytes_impl = coders.BytesCoder().get_impl()
  s = 'apache_beam_performance_benchmark_string'
  b = b'apache_beam_performance_benchmark_bytes_payload'
  t0 = time.perf_counter()
  for _ in range(500000):
    enc = str_impl.encode(s)
    _ = str_impl.decode(enc)
    _ = bytes_impl.estimate_size(b)
  return time.perf_counter() - t0


def bench_17_component_coder_kv_fastpath():
  """Benchmark for perf/py-component-coder-kv-fastpath."""
  kv_impl = coders.TupleCoder(
      (coders.StrUtf8Coder(), coders.VarIntCoder())).get_impl()
  item = ('user_key_12345', 987654)
  t0 = time.perf_counter()
  for _ in range(500000):
    enc = kv_impl.encode(item)
    _ = kv_impl.decode(enc)
  return time.perf_counter() - t0


def bench_18_tuple_and_sequence_coder_impl():
  """Benchmark for perf/py-tuple-and-sequence-coder-impl."""
  seq_impl = coders.IterableCoder(coders.VarIntCoder()).get_impl()
  tup_impl = coders.TupleCoder(
      (coders.VarIntCoder(), coders.VarIntCoder(),
       coders.VarIntCoder())).get_impl()
  seq = [1, 2, 3, 4, 5, 6, 7, 8]
  tup = (10, 20, 30)
  t0 = time.perf_counter()
  for _ in range(300000):
    _ = seq_impl.encode(seq)
    _ = tup_impl.estimate_size(tup)
  return time.perf_counter() - t0


def bench_19_windowed_value_coder_impl_size():
  """Benchmark for perf/py-windowed-value-coder-impl-size."""
  wvc_impl = coders.WindowedValueCoder(coders.VarIntCoder()).get_impl()
  wv = windowed_value.WindowedValue(42, 1700000000, (window.GlobalWindow(), ))
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = wvc_impl.get_estimated_size_and_observables(wv)
  return time.perf_counter() - t0


def bench_20_timestamp_prefixing_window_coder():
  """Benchmark for perf/py-timestamp-prefixing-window-coder."""
  tp_impl = coder_impl.TimestampPrefixingWindowCoderImpl(
      coders.IntervalWindowCoder().get_impl())
  val = (timestamp.Timestamp(1700000000), window.IntervalWindow(0, 60))
  t0 = time.perf_counter()
  for _ in range(400000):
    enc = tp_impl.encode(val)
    _ = tp_impl.decode(enc)
  return time.perf_counter() - t0


def bench_21_row_coder_impl_fastpath():
  """Benchmark for perf/py-row-coder-impl-fastpath."""
  RowCls = typing.NamedTuple(
      'BenchRow', [('uid', str), ('etype', str), ('score', int),
                   ('lat', float)])
  schema = schemas.named_fields_to_schema([('uid', str), ('etype', str),
                                           ('score', int), ('lat', float)])
  rc_impl = row_coder.RowCoder(schema).get_impl()
  r = RowCls('user_42', 'click', 123, 4.5)
  t0 = time.perf_counter()
  for _ in range(300000):
    enc = rc_impl.encode(r)
    _ = rc_impl.decode(enc)
  return time.perf_counter() - t0


def bench_22_row_coder_hash_and_column_encoder():
  """Benchmark for perf/py-row-coder-hash-and-column-encoder."""
  schema = schemas.named_fields_to_schema([('flag', bool), ('score', int)])
  rc = row_coder.RowCoder(schema)
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = hash(rc)
  return time.perf_counter() - t0


def bench_23_slow_output_stream_fastpath():
  """Benchmark for perf/py-slow-output-stream-fastpath."""
  out = slow_stream.OutputStream()
  t0 = time.perf_counter()
  for _ in range(300000):
    out.write_varint64(42)
    out.write_bigendian_int64(123456789)
    out.write_bigendian_double(3.14159)
  return time.perf_counter() - t0


def bench_24_byte_counting_output_stream():
  """Benchmark for perf/py-byte-counting-output-stream."""
  bcos = slow_stream.ByteCountingOutputStream()
  t0 = time.perf_counter()
  for _ in range(500000):
    bcos.write_varint64(12345)
    bcos.write_bigendian_int64(123456789)
    bcos.write_bigendian_double(3.14)
  return time.perf_counter() - t0


def bench_25_slow_input_stream_fastpath():
  """Benchmark for perf/py-slow-input-stream-fastpath."""
  out = slow_stream.OutputStream()
  for _ in range(1000):
    out.write_varint64(42)
    out.write_bigendian_int64(123456789)
    out.write_bigendian_double(3.14159)
  data = out.get()
  t0 = time.perf_counter()
  for _ in range(300):
    inp = slow_stream.InputStream(data)
    for _ in range(1000):
      _ = inp.read_varint64()
      _ = inp.read_bigendian_int64()
      _ = inp.read_bigendian_double()
  return time.perf_counter() - t0


def bench_26_cython_byte_counting_stream():
  """Benchmark for perf/py-cython-byte-counting-stream."""
  t0 = time.perf_counter()
  for _ in range(500000):
    s = cy_stream.ByteCountingOutputStream()
    s.write_varint64(12345)
    _ = s.get_count()
    _ = slow_stream.get_varint_size(1234567)
  return time.perf_counter() - t0


def bench_27_cloudpickle_dispatch_and_relpath():
  """Benchmark for perf/py-cloudpickle-dispatch-and-relpath."""
  fn = lambda x, y=10: (x + y, x * 2)
  t0 = time.perf_counter()
  for _ in range(5000):
    _ = pickler.dumps(fn)
    _ = cloudpickle.get_relative_path(__file__)
  return time.perf_counter() - t0


def bench_28_cloudpickle_pickler_prebuild_dispatch():
  """Benchmark for perf/py-cloudpickle-pickler-prebuild-dispatch."""
  fn = lambda x: x + 1
  t0 = time.perf_counter()
  for _ in range(5000):
    _ = cloudpickle_pickler.dumps(fn)
  return time.perf_counter() - t0


def bench_29_bounded_trie_fast_copy():
  """Benchmark for perf/py-bounded-trie-fast-copy."""
  cell = cells.BoundedTrieCell()
  for i in range(50):
    cell.add(('seg_a', f'seg_{i}', 'leaf'))
  t0 = time.perf_counter()
  for _ in range(20000):
    _ = cell.get_cumulative()
  return time.perf_counter() - t0


def bench_30_metric_name_cache_hash():
  """Benchmark for perf/py-metric-name-cache-hash."""
  mn = metricbase.MetricName('my_namespace', 'my_counter_name')
  t0 = time.perf_counter()
  for _ in range(1000000):
    _ = hash(mn)
    _ = mn.fast_name()
  return time.perf_counter() - t0


def bench_31_metrics_container_typed_name_cache():
  """Benchmark for perf/py-metrics-container-typed-name-cache."""
  mc = execution.MetricsContainer('step1')
  mnames = [
      metricbase.MetricName(f'ns_{i & 15}', f'm_{i & 31}') for i in range(64)
  ]
  t0 = time.perf_counter()
  for _ in range(10000):
    for mn in mnames:
      mc.get_counter(mn).inc()
  return time.perf_counter() - t0


def bench_32_metrics_container_get_cumulative():
  """Benchmark for perf/py-metrics-container-get-cumulative."""
  mc = execution.MetricsContainer('step1')
  for i in range(50):
    mc.get_counter(metricbase.MetricName('ns', f'c_{i}')).inc(i)
    mc.get_distribution(metricbase.MetricName('ns', f'd_{i}')).update(i)
  t0 = time.perf_counter()
  for _ in range(20000):
    _ = mc.get_cumulative()
  return time.perf_counter() - t0


def bench_33_monitoring_infos_coder_cache_1():
  """Benchmark for perf/py-monitoring-infos-coder-cache-1."""
  t0 = time.perf_counter()
  for i in range(100000):
    _ = monitoring_infos.int64_user_counter('ns', 'c', i)
  return time.perf_counter() - t0


def bench_34_monitoring_infos_coder_cache_2():
  """Benchmark for perf/py-monitoring-infos-coder-cache-2."""
  mi1 = monitoring_infos.int64_user_counter('ns', 'c', 10)
  mi2 = monitoring_infos.int64_user_counter('ns', 'c', 25)
  t0 = time.perf_counter()
  for _ in range(100000):
    _ = monitoring_infos.consolidate([mi1, mi2])
  return time.perf_counter() - t0


def bench_35_dataflow_distribution_log10():
  """Benchmark for perf/py-dataflow-distribution-log10."""
  counter = cy_dataflow_distribution_counter.DataflowDistributionCounter()
  t0 = time.perf_counter()
  for i in range(1, 500000):
    counter.add_input(i * 1000)
  return time.perf_counter() - t0


def bench_36_combiners_count_tolist_sample_top():
  """Benchmark for perf/py-combiners-count-tolist-sample-top."""
  cnt_fn = combiners.CountCombineFn()
  batch_list = list(range(1000))
  samp_fn = combiners.SampleCombineFn(20)
  acc = samp_fn.create_accumulator()
  tolist_fn = combiners.ToListCombineFn()
  lists_to_merge = [[i, i + 1] for i in range(400)]
  t0 = time.perf_counter()
  for _ in range(5000):
    cnt_fn.add_inputs(0, batch_list)
  for i in range(200000):
    acc = samp_fn.add_input(acc, i)
  for _ in range(500):
    tolist_fn.merge_accumulators(lists_to_merge)
  return time.perf_counter() - t0


def bench_37_pgbk_combining_values_side_inputs():
  """Benchmark for perf/py-pgbk-combining-values-side-inputs."""
  conv = batch_typehints.ListBatchConverter(int)
  batches = [[1, 2, 3, 4] for _ in range(200)]
  t0 = time.perf_counter()
  for _ in range(10000):
    _ = conv.combine_batches(batches)
  return time.perf_counter() - t0


def bench_38_groupby_and_partition_fastpath():
  """Benchmark for perf/py-groupby-and-partition-fastpath."""
  part_fn = core.Partition.ApplyPartitionFnFn()
  part_fn.setup()
  t0 = time.perf_counter()
  for i in range(300000):
    _ = list(part_fn.process(i, lambda x, n: x & 3, 4))
  return time.perf_counter() - t0


def bench_39_approx_quantiles_fastpath():
  """Benchmark for perf/py-approx-quantiles-fastpath."""
  aq = stats.ApproximateQuantilesCombineFn.create(num_quantiles=50)
  t0 = time.perf_counter()
  for _ in range(200):
    acc = aq.create_accumulator()
    for i in range(2000):
      acc = aq.add_input(acc, i)
  return time.perf_counter() - t0


def bench_40_sort_and_batch_elements_fastpath():
  """Benchmark for perf/py-sort-and-batch-elements-fastpath."""
  dofn = util._WindowAwareSortAndBatchElementsDoFn(
      min_batch_size=100, max_batch_size=250, element_size_fn=lambda x: 1)
  w = window.GlobalWindow()
  t0 = time.perf_counter()
  for _ in range(500):
    dofn.start_bundle()
    for i in range(1000):
      dofn.process((i & 7, i), w=w)
    _ = list(dofn.finish_bundle())
  return time.perf_counter() - t0


def bench_41_trivial_inference_opcodes_cache():
  """Benchmark for perf/py-trivial-inference-opcodes-cache."""
  fn = lambda x: (x[0], x[1] + 1)
  t0 = time.perf_counter()
  for _ in range(2000):
    _ = trivial_inference.infer_return_type_func(
        fn, [typehints.Tuple[str, int]])
    _ = schemas.SchemaTranslation.typing_to_runner_api(int)
  return time.perf_counter() - t0


def bench_42_typehints_is_consistent_and_tuple():
  """Benchmark for perf/py-typehints-is-consistent-and-tuple."""
  tc = typehints.Tuple[str, int, float]
  sample = ('hello', 42, 3.14)
  t0 = time.perf_counter()
  for _ in range(300000):
    tc.type_check(sample)
    _ = typehints.is_consistent_with(int, typehints.Any)
    _ = typehints.is_consistent_with(str, str)
  return time.perf_counter() - t0


def bench_43_native_type_compat_fastpath():
  """Benchmark for perf/py-native-type-compat-fastpath."""
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = native_type_compatibility.convert_to_beam_type(int)
    _ = native_type_compatibility.convert_to_beam_type(str)
    _ = native_type_compatibility.convert_to_beam_type(typing.List[int])
  return time.perf_counter() - t0


def bench_44_bigquery_write_fn_fastpath():
  """Benchmark for perf/py-bigquery-write-fn-fastpath."""
  row_and_id = ({
      'user_id': 'user_12345',
      'event_type': 'purchase',
      'amount': 99.95,
      'items': 3,
      'active': True,
      'country': 'US',
      'ts': 1700000000,
      'notes': None,
  },
                'insert_id_0001')
  t0 = time.perf_counter()
  for _ in range(50000):
    _ = bigquery._fast_row_and_insert_id_size(row_and_id)
  return time.perf_counter() - t0


def bench_45_bigquery_tools_avro_and_json():
  """Benchmark for perf/py-bigquery-tools-avro-and-json."""
  schema_dict = {
      'fields': [
          {
              'name': 'k', 'type': 'STRING', 'mode': 'NULLABLE'
          },
          {
              'name': 'v', 'type': 'INTEGER', 'mode': 'NULLABLE'
          },
      ]
  }
  bio = io.BytesIO()
  writer = bigquery_tools.AvroRowWriter(bio, schema_dict)
  row = {'k': 'user_12345', 'v': 987654321}
  t0 = time.perf_counter()
  for _ in range(30000):
    writer.write(row)
    _ = writer.tell()
    _ = bigquery_tools._split_table_spec('my-project:my_dataset.my_table')
  return time.perf_counter() - t0


def bench_46_filesystems_scheme_cache():
  """Benchmark for perf/py-filesystems-scheme-cache."""
  t0 = time.perf_counter()
  for _ in range(200000):
    _ = filesystems.FileSystems.get_filesystem('gs://my-bucket/path/to/object')
    _ = filesystems.FileSystems.get_filesystem('/tmp/local/path/to/file')
  return time.perf_counter() - t0


def bench_47_compression_types_constants():
  """Benchmark for perf/py-compression-types-constants."""
  t0 = time.perf_counter()
  for _ in range(500000):
    _ = filesystem.CompressionTypes.detect_compression_type('data.json.gz')
    _ = filesystem.CompressionTypes.is_valid_compression_type('gzip')
    _ = filesystem.CompressionTypes.mime_type('gzip')
  return time.perf_counter() - t0


def bench_48_parquet_and_textio_fastpath():
  """Benchmark for perf/py-parquet-and-textio-fastpath."""
  src = textio._TextSource('/tmp/dummy.txt', 0, 1.0, ' uncomp', True)
  buf = textio._TextSource.ReadBuffer(b'line1\nline2\r\nline3\n', 0)
  t0 = time.perf_counter()
  for _ in range(300000):
    _ = src._find_separator_bounds(buf, 0)
  return time.perf_counter() - t0


def bench_49_tfrecord_struct_fastpath():
  """Benchmark for perf/py-tfrecord-struct-fastpath."""
  payload = b'0123456789abcdef' * 4
  t0 = time.perf_counter()
  for _ in range(200000):
    bio = io.BytesIO()
    tfrecordio._TFRecordUtil.write_record(bio, payload)
    bio.seek(0)
    _ = tfrecordio._TFRecordUtil.read_record(bio)
  return time.perf_counter() - t0


def bench_50_dataframe_partition_and_explode():
  """Benchmark for perf/py-dataframe-partition-and-explode."""
  df = pd.DataFrame({'a': np.arange(5000), 'b': np.arange(5000) * 2})
  part = partitionings.Index()
  RowCls = typing.NamedTuple('DFRow', [('a', int), ('b', int)])
  conv = pandas_type_compatibility.DataFrameBatchConverter(RowCls)
  t0 = time.perf_counter()
  for _ in range(100):
    _ = list(part.partition_fn(df, 16))
    _ = list(conv.explode_batch(df))
  return time.perf_counter() - t0


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--benchmark', type=str, default='all')
  args = parser.parse_args()
  funcs = {
      name: obj
      for name, obj in sorted(globals().items())
      if name.startswith('bench_') and callable(obj)
  }
  for name, fn in funcs.items():
    if args.benchmark in ('all', name):
      elapsed = fn()
      print(f'{name}: {elapsed:.4f}s')


if __name__ == '__main__':
  main()
