import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumer1 {
    public static void main(String[] args){
        Logger logger = LoggerFactory.getLogger(consumer1.class.getName());
        String bootstrapServer = "localhost:9092";
        String groupID = "second_app";
        String topic = "my_first";

        //建立 consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //建立 consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //訂閱topic
        consumer.subscribe(Arrays.asList(topic));

//        int count = 0;  //訊息計數
//        int latencySum = 0;

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
//                count++;
                long datetime = System.currentTimeMillis();
                System.out.println("Current Time Stamp: " + datetime);
                System.out.println(
                        "Value: " + record.value() + "\n" +
//                        "Topic:" + record.topic() + "\n" +
//                        "Partition: " + record.partition() + "\n" +
//                        "Offset: " + record.offset() + "\n" +
                        "Timestamp" + record.timestamp() + "\n");
//                latencySum += (record.timestamp() - datetime);

//                logger.info("Key" + record.key() + "\n" +
//                        "Value: " + record.value() + "\n" +
//                        "Topic:" + record.topic() + "\n" +
//                        "Partition: " + record.partition() + "\n" +
//                        "Offset: "+record.offset() + "\n" +
//                        "Timestamp" + record.timestamp());
            }
        }
    }
}
