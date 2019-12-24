package com.jinxin.flink.streaming.wordcount;

import com.jinxin.flink.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountFlink {
    public static void main(String[] args) throws Exception {
        //检查输入的参数
        final ParameterTool params = ParameterTool.fromArgs(args);

        //设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使得参数在 web 接口可用
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        //获取输入 data
        DataStream<String> text;
        if (params.has("input")){
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("执行 WordCount 使用默认的数据集");
            System.out.println("使用 --input <path> 指定具体文件的输入");
            text = env.fromElements(WordCountData.WORDS).setParallelism(1);
        }

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer()).setParallelism(1)
                .keyBy(0)
                .countWindow(10,5)
                .sum(1).setParallelism(1);

        //输出数据
        if (params.has("output")){
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("输出结果到控制台 使用 -- output <path> 指定具体路径");
            counts.print();
        }

        env.execute("Shenjinxin First Flink Demo");
    }

    public static class Tokenizer implements org.apache.flink.api.common.functions.FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            //将文本分割
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens){
                System.out.println(token);
                if (token.length() > 0){
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
