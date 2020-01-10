package tweetProducerApp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.*;

import java.util.Properties;

public class TweetProducer {

    private void printUsageAndExit(int code) {
        System.err.println("Usage: TweetProducer <client id> <bootstrap servers> <topic>");
        System.exit(code);
    }

    private Properties makeProperties(String clientId, String bootstrapServers) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return kafkaProps;
    }

    public void run(String[] args) {
        if (args.length < 3) {
            printUsageAndExit(-1);
        }

        String clientId = args[0];
        String bootstrapServers = args[1];
        String topic = args[2];

        Properties props = makeProperties(clientId, bootstrapServers);

        TwitterStream stream = TwitterStreamFactory.getSingleton();

        StatusListener listener = new TweetProducerStatusListener(props, topic);

        stream.addListener(listener);
        stream.sample();
    }

    public static class TweetProducerStatusListener extends StatusAdapter {
        private KafkaProducer<byte[], String> producer;
        private String topic;

        public TweetProducerStatusListener(Properties props, String topic) {
            this.producer = new KafkaProducer<>(props);
            this.topic = topic;
        }

        @Override
        public void onStatus(Status status) {
            String jsonStatus = TwitterObjectFactory.getRawJSON(status);
            ProducerRecord record = new ProducerRecord<byte[], String>(this.topic, jsonStatus);
            producer.send(record);
        }
    }
}
