package kafka.basic.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProductDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = 10L;
        consumer.assign(Arrays.asList(topicPartition));

        consumer.seek(topicPartition, offset);

        int numberOfMessageToRead = 5;
        boolean keepReading = true;
        int numberOfMessageReadSoFar = 0;

        while (keepReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record:records) {
                numberOfMessageReadSoFar++;
                logger.info("Key: "+record.key() + " Value" + record.value());
                logger.info("Partition: "+record.partition() + " offset" + record.offset());
                if(numberOfMessageReadSoFar >= numberOfMessageToRead){
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
