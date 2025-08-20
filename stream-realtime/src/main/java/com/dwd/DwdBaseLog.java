package com.dwd;

import com.dwd.Constant;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.elasticloadbalancing.model.BackendServerDescription;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.alibaba.fastjson.JSON.parseObject;

public class DwdBaseLog extends BackendServerDescription {


    public class Constant {
        public static final String KAFKA_BROKERS = "cdh01:9092";
        public static final String ALPHA = "alpha";
    }
    public static void main(String[] args) throws Exception{
//        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.ALPHA);
    }

    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDs) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
//        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag",TypeInformation.of(String.class));
        SingleOutputStreamOperator<JSONObject> jsons = kafkaStrDs.process(
                new ProcessFunction<String, JSONObject>() {  // 修改泛型类型
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = parseObject(jsonStr);
//                            如果转换的时候，没有发生异常，说明是标准的json，将数据传递的下游
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            // 处理异常情况 属于脏数据
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        jsons.print("标准的json:");
        SideOutputDataStream<String> dirtyDS = jsons.getSideOutput(dirtyTag);
        dirtyDS.print("脏数据:");
//将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dirty_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                /设置事务Id的前缀
                .setTransactionalIdPrefix("dwd_base_log_")
                .setProperty("transaction.timeout.ms", "900000")
                .build();

        dirtyDS.sinkTo(kafkaSink);


    }
}
