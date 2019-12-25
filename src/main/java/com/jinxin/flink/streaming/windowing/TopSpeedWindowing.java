package com.jinxin.flink.streaming.windowing;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;


import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TopSpeedWindowing {
    public static void main(String[] args) throws Exception {
        //获取 参数
        ParameterTool params = ParameterTool.fromArgs(args);

        //构建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置时间特性是事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置 web 接口可以调用
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);
        //定义了 DataStream 是一个四元组集合
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
        if(params.has("input")){
            //读入数据并且进行 map 操作
            carData = env.readTextFile(params.get("input")).map(new ParseCarData());
        } else {
            System.out.println("Executing default input data set");
            //自行构造 CarSource
            carData = env.addSource(CarSource.create(1));
        }

        int evictionSec = 10;
        double triggerMeters = 50;
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
                .assignTimestampsAndWatermarks(new CarTimestamp())
                .keyBy(0)
                .window(GlobalWindows.create())
                // 窗口大小是 10s
                .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                .trigger(DeltaTrigger.of(triggerMeters,
                        new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                            private static final long serialVersionUID = 1L;
                            public double getDelta(
                                    Tuple4<Integer, Integer, Double, Long> oldDataPoint,
                                    Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                                System.out.println("****************************");
                                System.out.println("新的坐标" + newDataPoint.f2);
                                System.out.println("老的坐标" + oldDataPoint.f2);
                                System.out.println("差值是" + (newDataPoint.f2 - oldDataPoint.f2));
                                System.out.println("*****************************");
                                return newDataPoint.f2 - oldDataPoint.f2;
                            }
                        }, carData.getType().createSerializer(env.getConfig())))
                .maxBy(1);

        if (params.has("output")){
            topSpeeds.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout");
            topSpeeds.print();
        }

        env.execute("CarTopSpeedWindowingExample");
    }

    private static class ParseCarData implements org.apache.flink.api.common.functions.MapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        public Tuple4<Integer, Integer, Double, Long> map(String s)  {
            String rawData = s.substring(1,s.length() - 1);
            String[] data = rawData.split(",");
            return new Tuple4<Integer, Integer, Double, Long>(Integer.valueOf(data[0]),Integer.valueOf(data[1]),Double.valueOf(data[2]),Long.valueOf(data[3]));
        }
    }

    private static class CarSource implements SourceFunction<Tuple4<Integer,Integer,Double,Long>>{
        private static final long serialVersionUID = 1L;
        private Integer[] speeds;
        private Double[] distances;

        private Random rand = new Random();

        private volatile boolean isRunning = true;

        private CarSource(int numOfCars){
            speeds = new Integer[numOfCars];
            distances = new Double[numOfCars];
            Arrays.fill(speeds, 50);
            Arrays.fill(distances, 0d);
        }

        //该方法调用构造方法
        private static CarSource create(int cars){
            return new CarSource(cars);
        }
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> sourceContext) throws Exception {
            while (isRunning){
                //线程睡眠 100ms
                Thread.sleep(1000);
                // 遍历所有的车
                for (int carId = 0; carId < speeds.length; carId++){
                    if (rand.nextBoolean()){
                        speeds[carId] = Math.min(100, speeds[carId] + 5);
                        System.out.println("速度 +" + speeds[0]);
                    } else {
                        speeds[carId] = Math.max(0, speeds[carId] - 5);
                        System.out.println("速度 -" +speeds[0]);
                    }
                    distances[carId] += speeds[carId] / 3.6d;
                    System.out.println("=============");
                    System.out.println("距离" + distances[0]);
                    System.out.println("=============");
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<Integer, Integer, Double, Long>(carId,
                        speeds[carId], distances[carId], System.currentTimeMillis());
                    sourceContext.collect(record);
                }
            }
        }

        public void cancel() {
            isRunning = false;
        }
    }


    private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;
        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }
}
