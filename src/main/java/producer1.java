import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producer1 {
    public static void main(String[] args){
        System.out.println("Hello World");

        String bootstrapServers = "localhost:9092";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> first_producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>("my_first", "Hye Kafka");

        first_producer.send(record);
        first_producer.flush();
        first_producer.close();
    }
}
