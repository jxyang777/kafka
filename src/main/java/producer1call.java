import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Logger;

import java.util.Properties;

public class producer1call {
    public static void main(String[] args){
        System.out.println("Hello World");

        String bootstrapServers = "localhost:9092";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> first_producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>("my_first", "Hye Kafka");

        first_producer.send(record, (recordMetadata, e) -> {
            Logger logger = (Logger) LoggerFactory.getLogger(producer1call.class);
            if(e == null){
                logger.info("Successfully received the details as: \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition:" + recordMetadata.partition() + "\n" +
                        "Offset" + recordMetadata.offset() + "\n" +
                        "Timestamp" + recordMetadata.timestamp());
            }else{
                logger.error("Can't produce,getting error",e);
            }
        });
        first_producer.flush();
        first_producer.close();
    }
}