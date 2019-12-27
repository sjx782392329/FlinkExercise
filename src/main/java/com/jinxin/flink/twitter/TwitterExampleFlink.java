package com.jinxin.flink.twitter;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.StringTokenizer;


public class TwitterExampleFlink {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterExample [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(params.getInt("parallelism", 1));

        // get input data
        DataStream<String> streamSource;

        streamSource = env.fromElements(TwitterExampleData.TEXTS);


        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // selecting English tweets and splitting to (word, 1)
                .flatMap(new SelectEnglishAndTokenizeFlatMap())
                // group by words and sum their occurrences
                .keyBy(0).sum(1);

        // emit result
        if (params.has("output")) {
            tweets.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            tweets.print();
        }

        // execute program
        env.execute("Twitter Streaming Example");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Deserialize JSON from twitter source
     *
     * <p>Implements a string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        /**
         * Select the language from the incoming JSON text.
         */
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
            boolean hasText = jsonNode.has("text");
            if (isEnglish && hasText) {
                // message of tweet
                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

                // split the message
                while (tokenizer.hasMoreTokens()) {
                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

                    if (!result.equals("")) {
                        out.collect(new Tuple2<String, Integer>(result, 1));
                    }
                }
            }
        }
    }

}

