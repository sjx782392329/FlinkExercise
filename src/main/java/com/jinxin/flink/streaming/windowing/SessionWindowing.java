package com.jinxin.flink.streaming.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/***
 *  Session window 在时间戳差值严格大于 gap 的时候才会触发新的窗口操作
 *  等于 gap 的时候还是会被计算在旧的窗口操作
  */
public class SessionWindowing {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        final boolean fileOutput = params.has("output");

        final List<Tuple3<String, Long, Integer>> input = new ArrayList<Tuple3<String, Long, Integer>>();

        input.add(new Tuple3<String, Long, Integer>("a", 1L, 1));

        input.add(new Tuple3<String, Long, Integer>("b", 1L, 1));
        input.add(new Tuple3<String, Long, Integer>("b", 3L, 1));
        input.add(new Tuple3<String, Long, Integer>("b", 5L, 1));
        input.add(new Tuple3<String, Long, Integer>("c", 6L, 1));

        input.add(new Tuple3<String, Long, Integer>("b", 7L, 1));
        input.add(new Tuple3<String, Long, Integer>("b", 9L, 1));

        input.add(new Tuple3<String, Long, Integer>("b", 12L, 1));
        input.add(new Tuple3<String, Long, Integer>("b", 16L, 1));

        input.add(new Tuple3<String, Long, Integer>("a", 10L, 1));
        input.add(new Tuple3<String, Long, Integer>("c", 11L, 1));
        input.add(new Tuple3<String, Long, Integer>("d", 11L, 1));

        DataStream<Tuple3<String, Long, Integer>> source = env
                .addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    public void run(SourceContext<Tuple3<String, Long, Integer>> sourceContext) throws Exception {
                        for (Tuple3<String, Long, Integer> value : input){
                            sourceContext.collectWithTimestamp(value, value.f1);
                            sourceContext.emitWatermark(new Watermark(value.f1 - 16));
                        }
                        sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }

                    public void cancel() {

                    }
                });
        DataStream<Tuple3<String, Long, Integer>> aggregated = source
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                .sum(2);

        if (fileOutput){
            aggregated.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout");
            aggregated.print();
        }
        env.execute();
    }
}
