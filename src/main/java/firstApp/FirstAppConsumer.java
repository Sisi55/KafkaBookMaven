package firstApp;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FirstAppConsumer {

    private static String topicName = "first-app";

    public static void main(String[] args){
        String bootstrapServers = "kafka-broker01:9092,kafka-broker02:9092,kafka-broker03:9092";
        // 1. Kafka Consumer 에 필요한 설정
        Properties conf = new Properties();
        conf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        conf.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        conf.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 2. Kafka 클러스터에서 Message 를 수신 Consume 하는 객체를 생성
        Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);

        // 3. 수신 subscribe 하는 Topic 을 등록
        consumer.subscribe(Collections.singletonList(topicName));

        for(int count=0;count<300;count++){
            // 4. Message 를 수신하여, 콘솔에 표시한다.
            ConsumerRecords<Integer, String> records = consumer.poll(1);

            for(ConsumerRecord<Integer, String> record: records){
                String msgString = String.format("key:%d, value:%s", record.key(), record.value());
                System.out.println(msgString);

                // 5. 처리 완료된 Message 의 offset 을 Commit 한다.
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata oam = new OffsetAndMetadata(record.offset()+1);
                Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                consumer.commitSync(commitInfo);
            }
            try{
                Thread.sleep(1000);
            }catch(InterruptedException ex){
                ex.printStackTrace();
            }
        }

        // 6. Kafka Consumer 를 클로즈하여 종료
        consumer.close();
    }
}
