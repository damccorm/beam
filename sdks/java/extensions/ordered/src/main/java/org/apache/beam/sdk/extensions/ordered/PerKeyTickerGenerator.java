/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PTransform to generate per key tickers with certain frequency.
 *
 * @param <EventKeyT>
 * @param <EventT>
 */
class PerKeyTickerGenerator<EventKeyT, EventT>
    extends PTransform<
        PCollection<KV<EventKeyT, KV<Long, EventT>>>,
        PCollection<KV<EventKeyT, KV<Long, EventT>>>> {

  private static final Logger LOG = LoggerFactory.getLogger(PerKeyTickerGenerator.class);

  private final Coder<EventKeyT> eventKeyCoder;
  private final Coder<EventT> eventCoder;
  private final Duration tickerFrequency;

  PerKeyTickerGenerator(
      Coder<EventKeyT> eventKeyCoder, Coder<EventT> eventCoder, Duration tickerFrequency) {
    this.eventKeyCoder = eventKeyCoder;
    this.eventCoder = eventCoder;
    this.tickerFrequency = tickerFrequency;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized PCollection<KV<EventKeyT, KV<Long, EventT>>> expand(
      PCollection<KV<EventKeyT, KV<Long, EventT>>> input) {
    return input
        .apply(
            "Generate Tickers",
            ParDo.of(new PerKeyTickerGeneratorDoFn<>(eventKeyCoder, tickerFrequency)))
        .setCoder(
            KvCoder.of(eventKeyCoder, KvCoder.of(VarLongCoder.of(), NullableCoder.of(eventCoder))));
  }

  static class PerKeyTickerGeneratorDoFn<EventKeyT, EventT>
      extends DoFn<KV<EventKeyT, KV<Long, EventT>>, KV<EventKeyT, KV<Long, EventT>>> {

    private static final String STATE = "state";
    private static final String TIMER = "timer";

    @StateId(STATE)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<EventKeyT>> stateSpec;

    @TimerId(TIMER)
    @SuppressWarnings("unused")
    private final TimerSpec tickerTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final Duration tickerFrequency;

    PerKeyTickerGeneratorDoFn(Coder<EventKeyT> keyCoder, Duration tickerFrequency) {
      stateSpec = StateSpecs.value(keyCoder);
      this.tickerFrequency = tickerFrequency;
    }

    @ProcessElement
    public void process(
        @Element KV<EventKeyT, KV<Long, EventT>> element,
        @AlwaysFetched @StateId(STATE) ValueState<EventKeyT> state,
        @Timestamp Instant currentTimestamp,
        @TimerId(TIMER) Timer tickerTimer) {
      @Nullable EventKeyT keyValue = state.read();
      if (keyValue != null) {
        return;
      }

      // The first event received becomes the starting point for watermark tracking in this
      // transform. setTimer() method's withOutputTimestamp() determines what that watermark will
      // be.
      //
      // This can be an issue if the first event is really far back because the flattened
      // PCollection of events and tickers will have the lowest watermark of the two.
      // It might be possible to improve the tracking by inspecting other elements and progress
      // the watermark further based on the timestamp of a new event. Or use the latest contiguous
      // range side input and its timestamp.
      setTimer(tickerTimer, currentTimestamp);

      state.write(element.getKey());
    }

    @OnTimer(TIMER)
    public void onTimer(
        @StateId(STATE) ValueState<EventKeyT> state,
        @TimerId(TIMER) Timer tickerTimer,
        @Timestamp Instant currentTimestamp,
        OutputReceiver<KV<EventKeyT, KV<Long, EventT>>> outputReceiver) {

      @Nullable EventKeyT key = state.read();
      if (key == null) {
        LOG.warn(
            "Expected to get the key from the state, but got null. "
                + "It is expected during pipeline draining.");
        return;
      }

      // Null value will be an indicator to the main transform that the element is a ticker
      outputReceiver.output(KV.of(key, KV.of(0L, null)));
      setTimer(tickerTimer, currentTimestamp);
    }

    private void setTimer(Timer tickerTimer, Instant currentTime) {
      tickerTimer
          .offset(tickerFrequency)
          .withOutputTimestamp(currentTime.plus(tickerFrequency))
          .setRelative();
    }

    /**
     * This call will be received when the input is windowed or in case the input is in Global
     * Window and the pipeline is drained. In either cases we need to stop the timers. We can't
     * access the timer from this method and clear the state to indicate that the processing needs
     * to stop.
     *
     * @param state
     */
    @OnWindowExpiration
    public void onWindowExpiration(@StateId(STATE) ValueState<EventKeyT> state) {
      state.clear();
    }
  }
}
