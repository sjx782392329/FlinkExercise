package com.jinxin.flink.twitter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

public class TwitterExample {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(params.getInt("paralleism", 1));

        DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);
        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .flatMap(new SelectEnglishAndTokenizeFlatMap())
                .keyBy(0)
                .sum(1);

        tweets.print();
        env.execute("Twitter Streaming Example");
    }

    private static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            if (jsonParser == null){
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(s, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
            boolean hasText = jsonNode.has("text");
            if (isEnglish && hasText){
                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());
                while (tokenizer.hasMoreElements()){
                    String result = tokenizer.nextToken().replaceAll("\\s*","").toLowerCase();
                    System.out.println("*****" + result);
                    if (!result.equals("")){
                        collector.collect(new Tuple2<String, Integer>(result, 1));
                    }
                }
            }
        }
    }
}
