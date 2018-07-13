import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class EmulatedIncomeDataProducer {

    private KafkaProducer<Long, String> createProducer(){
        Properties props = new Properties();
        props.put("schema.registry.url", "http://sandbox-hdp.hortonworks.com:8081");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "EmulatedIncomeDataProducer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public void runProducer(String topicName, int sendMessageCount){
        KafkaProducer<Long, String> kafkaProducer = createProducer();

        long time = System.currentTimeMillis();
        try {

            for (long index = time; index < time + sendMessageCount; index++) {
                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(topicName, index, "Test Record # " + index);
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();

                long elapsedTime = System.currentTimeMillis() - time;

                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        producerRecord.key(), producerRecord.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);
            }
        } catch (Exception ex){
            System.out.println(ex.getMessage());
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
