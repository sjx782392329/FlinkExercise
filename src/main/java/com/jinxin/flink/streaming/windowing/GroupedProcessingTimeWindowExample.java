package com.jinxin.flink.streaming.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class GroupedProcessingTimeWindowExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());
        stream
            .keyBy(0)
                .timeWindow(Time.of(2500, TimeUnit.MILLISECONDS), Time.of(500, TimeUnit.MILLISECONDS))
                .reduce(new SumingReducer())
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    public void invoke(Tuple2<Long, Long> value) throws Exception {
                    }
                });
        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;
        public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
            final long startTime = System.currentTimeMillis();

            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;

            while (running && count < numElements){
                count++;
                sourceContext.collect(new Tuple2<Long, Long>(val++, 1L));

                if (val > numKeys){
                    val = 1L;
                }
            }
            final long endTime = System.currentTimeMillis();
            System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
        }

        public void cancel() {
            running = false;
        }
    }

    private static class SumingReducer implements ReduceFunction<Tuple2<Long, Long>> {
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return new Tuple2<Long, Long>(value1.f0, value1.f1 + value2.f1);
        }
    }
}
