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

"""Representative Cloud Dataflow benchmark suite comparing master vs optimized SDK."""

import argparse
import io
import logging
import time
import typing

import apache_beam as beam
from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.io import filesystems
from apache_beam.io import tfrecordio
from apache_beam.io.gcp import bigquery_tools
from apache_beam.metrics import Metrics
from apache_beam.metrics import execution
from apache_beam.metrics import metricbase
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import combiners
from apache_beam.transforms import stats
from apache_beam.transforms import util
from apache_beam.transforms import window
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints
from apache_beam.utils import timestamp


class WorkerSubsystemBenchDoFn(beam.DoFn):
  """Measures exact CPU timings of key subsystems inside the Dataflow worker."""
  def process(self, _: int):
    # 1. SlidingWindows.assign (200,000 calls, 6 windows each = 1.2M windows)
    sw = window.SlidingWindows(size=60, period=10)
    ctx = window.WindowFn.AssignContext(timestamp.Timestamp(1700000123), 'elem')
    t0 = time.perf_counter()
    for _ in range(200000):
      sw.assign(ctx)
    Metrics.counter('subbench_ms', '01_sliding_windows_assign_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    # 2. RowCoderImpl encode + decode (300,000 round-trips)
    local_row_cls = typing.NamedTuple(
        'LocalBenchRow',
        [
            ('user_id', str),
            ('event_type', str),
            ('score', int),
            ('latency_ms', float),
            ('payload', bytes),
        ])
    schema = schemas.named_fields_to_schema([
        ('user_id', str),
        ('event_type', str),
        ('score', int),
        ('latency_ms', float),
        ('payload', bytes),
    ])
    rc_impl = coders.RowCoder(schema).get_impl()
    sample_row = local_row_cls(
        'user_42', 'click', 123, 4.5, b'0123456789abcdef')
    t0 = time.perf_counter()
    for _ in range(300000):
      enc = rc_impl.encode(sample_row)
      _ = rc_impl.decode(enc)
    Metrics.counter('subbench_ms', '02_row_coder_roundtrip_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    # 3. MetricsContainer.get_counter + MetricName hash (500,000 lookups)
    mc = execution.MetricsContainer('step1')
    mnames = [
        metricbase.MetricName(f'ns_{i & 15}', f'm_{i & 31}') for i in range(64)
    ]
    t0 = time.perf_counter()
    for _ in range(8000):
      for mn in mnames:
        mc.get_counter(mn).inc()
    Metrics.counter('subbench_ms', '03_metrics_container_lookup_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    # 4. Combiners: CountCombineFn.add_inputs + SampleCombineFn + ToList merge
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
    Metrics.counter('subbench_ms', '04_combiners_hotpath_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    # 5. Typehints & Trivial Inference (2,000 infer_return_type_func + 200k TupleConstraint)
    tc = typehints.Tuple[str, int, float]
    sample_tup = ('hello', 42, 3.14)
    sample_fn = lambda x: (x[0], x[1] + 1)
    t0 = time.perf_counter()
    for _ in range(2000):
      trivial_inference.infer_return_type_func(
          sample_fn, [typehints.Tuple[str, int]])
    for _ in range(200000):
      tc.type_check(sample_tup)
    Metrics.counter('subbench_ms', '05_typehints_inference_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    # 6. FileSystems.get_filesystem + AvroRowWriter.write/tell
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
    avro_writer = bigquery_tools.AvroRowWriter(bio, schema_dict)
    row_dict = {'k': 'user_12345', 'v': 987654321}
    t0 = time.perf_counter()
    for _ in range(100000):
      filesystems.FileSystems.get_filesystem('gs://bucket/path/file.txt')
    for _ in range(30000):
      avro_writer.write(row_dict)
      _ = avro_writer.tell()
    Metrics.counter('subbench_ms', '06_io_filesystems_and_avro_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    # 7. Cloudpickle dumps (2,000 function serializations)
    t0 = time.perf_counter()
    for _ in range(2000):
      pickler.dumps(sample_fn)
    Metrics.counter('subbench_ms', '07_cloudpickle_dumps_ms').inc(
        int((time.perf_counter() - t0) * 1000))

    yield 1


class GenerateAndWindowDoFn(beam.DoFn):
  """Generates synthetic timestamped events and exercises Metrics + Windowing."""
  def __init__(self, elements_per_shard: int):
    self.elements_per_shard = elements_per_shard
    self.processed_counter = Metrics.counter('bench', 'elements_generated')
    self.latency_dist = Metrics.distribution('bench', 'event_latency_dist')
    self.generate_wall_ms = Metrics.counter('bench', 'stage_generate_wall_ms')
    self.opt_flag = Metrics.counter('bench', 'worker_has_optimizations')

  def start_bundle(self):
    self._t0 = time.perf_counter()

  def process(self, shard_id: int):
    if shard_id == 0:
      has_opt = 1 if hasattr(
          filesystems.FileSystems, '_systems_by_scheme') else 0
      self.opt_flag.inc(has_opt)

    base_ts = 1700000000
    n = self.elements_per_shard
    counter = self.processed_counter
    dist = self.latency_dist
    ts_val = window.TimestampedValue
    for i in range(n):
      uid = f'user_{(shard_id * 131 + i) & 1023}'
      etype = 'click' if (i & 1) == 0 else 'view'
      score = (i * 17) & 255
      lat = float((i & 63) + 1)
      counter.inc()
      if (i & 7) == 0:
        dist.update(score)
      yield ts_val((uid, etype, score, lat, b'bench_payload_bytes_0123456789'),
                   base_ts + (i % 120))

  def finish_bundle(self):
    elapsed_ms = int((time.perf_counter() - self._t0) * 1000)
    self.generate_wall_ms.inc(elapsed_ms)


class WindowInspectDoFn(beam.DoFn):
  """Exercises PerWindowInvoker (windowed DoFn args), RowCoder & IntervalWindow."""
  def __init__(self):
    self.window_ops_ms = Metrics.counter(
        'bench', 'stage_window_inspect_wall_ms')
    self.windowed_elements = Metrics.counter(
        'bench', 'windowed_elements_processed')

  def setup(self):
    self._row_cls = typing.NamedTuple(
        'LocalBenchRow',
        [
            ('user_id', str),
            ('event_type', str),
            ('score', int),
            ('latency_ms', float),
            ('payload', bytes),
        ])
    schema = schemas.named_fields_to_schema([
        ('user_id', str),
        ('event_type', str),
        ('score', int),
        ('latency_ms', float),
        ('payload', bytes),
    ])
    self._rc_impl = coders.RowCoder(schema).get_impl()

  def start_bundle(self):
    self._t0 = time.perf_counter()
    self._count = 0

  def process(
      self,
      element: tuple,
      w=beam.DoFn.WindowParam,
      ts=beam.DoFn.TimestampParam):
    self._count += 1
    _ = hash(w)
    if (self._count & 15) == 0:
      enc = self._rc_impl.encode(self._row_cls(*element))
      dec = self._rc_impl.decode(enc)
      return [(dec.user_id, dec.score + int(w.end.micros // 1000000) % 7)]
    return [(element[0], element[2] + int(w.end.micros // 1000000) % 7)]

  def finish_bundle(self):
    elapsed_ms = int((time.perf_counter() - self._t0) * 1000)
    self.window_ops_ms.inc(elapsed_ms)
    self.windowed_elements.inc(self._count)


class GenerateBytesAndPairsDoFn(beam.DoFn):
  """Generates records for I/O, coders, BatchElements, Partition, and Quantiles."""
  def __init__(self, records_per_shard: int):
    self.records_per_shard = records_per_shard
    self.io_gen_ms = Metrics.counter('bench', 'stage_io_gen_wall_ms')
    self.opt_flag = Metrics.counter('bench', 'worker_has_optimizations')

  def start_bundle(self):
    self._t0 = time.perf_counter()

  def process(self, shard_id: int):
    if shard_id == 0:
      has_opt = 1 if hasattr(
          filesystems.FileSystems, '_systems_by_scheme') else 0
      self.opt_flag.inc(has_opt)
    n = self.records_per_shard
    for i in range(n):
      k = f'key_{(shard_id * 31 + i) & 511}'
      v = (shard_id * 1000000 + i) % 100000
      payload = f'{k}:{v}:payload_record_data_{i & 255}'.encode('utf-8')
      yield (k, v, payload)

  def finish_bundle(self):
    elapsed_ms = int((time.perf_counter() - self._t0) * 1000)
    self.io_gen_ms.inc(elapsed_ms)


def run_core_window_row_combine_pipeline(
    p, num_shards: int, elements_per_shard: int):
  """Pipeline 1: Windowing + RowCoder + PerWindowInvoker + Metrics + Combiners."""
  _ = (
      p
      | 'CreateSubBenchTrigger' >> beam.Create([0])
      | 'RunWorkerSubsystemBench' >> beam.ParDo(WorkerSubsystemBenchDoFn()))

  events = (
      p
      | 'CreateShards' >> beam.Create(list(range(num_shards)))
      | 'ReshuffleShards' >> beam.Reshuffle()
      |
      'GenerateEvents' >> beam.ParDo(GenerateAndWindowDoFn(elements_per_shard))
      | 'AssignSlidingWindows' >> beam.WindowInto(
          window.SlidingWindows(size=30, period=10))
      | 'WindowedInspect' >> beam.ParDo(WindowInspectDoFn()))

  # Exercise CountCombineFn, TopCombineFn, SampleCombineFn
  _ = (
      events
      | 'CountPerUserWindow' >> combiners.Count.PerKey()
      | 'SampleCounts' >> combiners.Sample.FixedSizePerKey(5))

  _ = (
      events
      | 'ExtractScores' >> beam.Values()
      | 'GlobalWindowScores' >> beam.WindowInto(window.GlobalWindows())
      | 'TopScores' >> combiners.Top.Largest(25))


def run_io_coders_transforms_pipeline(
    p, num_shards: int, records_per_shard: int, gcs_output_prefix: str):
  """Pipeline 2: TFRecordIO + TextIO + Coders + BatchElements + Partition + Quantiles."""
  records = (
      p
      | 'CreateIOShards' >> beam.Create(list(range(num_shards)))
      | 'ReshuffleIOShards' >> beam.Reshuffle()
      | 'GenerateIORecords' >> beam.ParDo(
          GenerateBytesAndPairsDoFn(records_per_shard)))

  # Branch 1: TFRecordIO write (exercises _TFRecordUtil + FileSystems + BytesCoder)
  _ = (
      records
      | 'ExtractPayloadBytes' >> beam.Map(lambda x: x[2])
      | 'WriteTFRecords' >> tfrecordio.WriteToTFRecord(
          file_path_prefix=f'{gcs_output_prefix}/tfrecords/part', num_shards=8))

  # Branch 2: BatchElements + Partition + ApproximateQuantiles + ToList
  partitions = (
      records
      | 'ExtractValues' >> beam.Map(lambda x: x[1])
      | 'PartitionByMod4' >> beam.Partition(lambda x, n: x & 3, 4))

  _ = (
      partitions[0]
      | 'BatchP0' >> util.BatchElements(min_batch_size=100, max_batch_size=250)
      | 'CountBatches' >> combiners.Count.Globally())

  _ = (
      partitions[1]
      | 'ApproxQuantilesP1' >> stats.ApproximateQuantiles.Globally(50))


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--benchmark_suite',
      choices=['core_window_row_combine', 'io_coders_transforms'],
      required=True)
  parser.add_argument('--num_shards', type=int, default=120)
  parser.add_argument('--elements_per_shard', type=int, default=25000)
  parser.add_argument('--gcs_output_prefix', type=str, default='')
  known_args, pipeline_args = parser.parse_known_args()

  options = PipelineOptions(pipeline_args, save_main_session=True)
  p = beam.Pipeline(options=options)
  if known_args.benchmark_suite == 'core_window_row_combine':
    run_core_window_row_combine_pipeline(
        p, known_args.num_shards, known_args.elements_per_shard)
  else:
    run_io_coders_transforms_pipeline(
        p,
        known_args.num_shards,
        known_args.elements_per_shard,
        known_args.gcs_output_prefix)
  result = p.run()
  if hasattr(result, 'job_id'):
    print(f'SUBMITTED_JOB_ID={result.job_id()}')
  else:
    result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
