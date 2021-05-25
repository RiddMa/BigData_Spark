package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Name: com.flink.WordCount
 * @Date: 2021/04/17
 * @Auther: weiwending
 * @Description:
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        /*获取 Flink 运行环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*配置 Kafka 连接属性*/
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",
                "master:9092");
        properties.setProperty("zookeeper.connect",
                "master:2181");
        properties.setProperty("group.id", "1");
        FlinkKafkaConsumer08<String> myconsumer = new

                FlinkKafkaConsumer08<>("test", new SimpleStringSchema(), properties);
        /*默认消费策略*/
        myconsumer.setStartFromGroupOffsets();
        DataStream<String> dataStream = env.addSource(myconsumer);
        DataStream<Tuple2<String, Integer>> result = dataStream.flatMap(new MyFlatMapper()).keyBy(0).sum(1);
        result.print().setParallelism(3);
        env.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            /*按空格分词*/
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
