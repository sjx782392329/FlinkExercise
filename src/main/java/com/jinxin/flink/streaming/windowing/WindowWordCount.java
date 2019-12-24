package com.jinxin.flink.streaming.windowing;

import com.jinxin.flink.streaming.wordcount.WordCountFlink;
import com.jinxin.flink.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text;
        text = env.fromElements(WordCountData.TEST);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);
        final int windowSize = params.getInt("window",6);
        final int slideSize = params.getInt("slide",1);

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new WordCountFlink.Tokenizer())
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1);

        counts.print();
        env.execute("Streaming Window");
    }
}
