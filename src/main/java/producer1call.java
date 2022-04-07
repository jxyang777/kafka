import com.sun.xml.internal.ws.util.xml.CDATA;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;
import java.util.Properties;
import java.sql.Timestamp;

public class producer1call {
    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        String s = sb.toString();
        sb.setLength(0);
        return s;
    }

    public static void main(String[] args){
        System.out.println("Hello World");

        String bootstrapServers = "localhost:9092";   //producer要去發布訊息的kafka server

        //填入producer的相關屬性
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //建立一個producer
        KafkaProducer<String, String> first_producer = new KafkaProducer<>(props);

        //發送訊息
        for(int i = 0; i < 10000; i++){
            System.out.println("Message: " + i);
            String msg = getRandomString(20);
            //建立一條訊息
            ProducerRecord<String, String> record = new ProducerRecord<>("my_first", msg);

            long datetime = System.currentTimeMillis();
            //Timestamp timestamp = new Timestamp(datetime);
            System.out.println("Current Time Stamp: "+ datetime);

            first_producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    Logger logger= LoggerFactory.getLogger(producer1call.class);

                    if (e== null) {
                        System.out.println(
//                                "Topic:" + recordMetadata.topic() + "\n" +
//                                "Partition:" + recordMetadata.partition() + "\n" +
//                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());

//                        logger.info("Successfully received the details as: \n" +
//                                "Topic:" + recordMetadata.topic() + "\n" +
//                                "Partition:" + recordMetadata.partition() + "\n" +
//                                "Offset" + recordMetadata.offset() + "\n" +
//                                "Timestamp" + recordMetadata.timestamp());
                    }
                    else {
                        logger.error("Can't produce,getting error",e);
                    }
                }
            });
            first_producer.flush();
        }
        //關閉producer
        first_producer.close();
    }
}