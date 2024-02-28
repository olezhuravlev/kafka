package org.example.kafkastreams;

import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.rocksdb.Transaction;

public class KafkaStreamsMain {

//    private String transactionRequestTopicName = "transaction-request";
//    private String changeLog1TopicName = "change-log-1";
//    private String changeLog2TopicName = "change-log-2";
//    private String transactionFailedTopicName = "transaction-failed";
//    private String transactionSuccessTopicName = "transaction-success";
//    private String transactionsLoggerLabel = "transactions logger";
//
//    private String stateStorage = "latest-transactions";
//
//    public static void main(String[] args) {
//    }
//
//    private void step1() {
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, Transaction> transactionStream = builder.stream(transactionRequestTopicName, Consumed.with(stringSerde, transactionRequestAvroSerde));
//    }
//
//    private void step2(StreamsBuilder builder, KStream<String, Transaction> transactionStream) {
//
//        final KStream<String, TransactionResult> resultStream = transactionStream.transformValues(() -> new TransactionTransformer());
//        resultStream.filter(TransactionProcessor::success).to(transactionSuccessTopicName, Produced.with(Serdes.String(), transactionResultAvroSerde));
//        resultStream.filterNot(TransactionProcessor::success).to(transactionFailedTopicName, Produced.with(Serdes.String(), transactionResultAvroSerde));
//
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kaProperties);
//        kafkaStreams.start();
//        kafkaStreams.close();
//    }
//
//    // Show data in console.
//    private void tracer(StreamsBuilder builder) {
//
//        KStream<String, Transaction> transactionStream = builder.stream(transactionRequestTopicName, Consumed.with(stringSerde, transactionRequestAvroSerde));
//
//        transactionStream.print(Printed.<String, Transaction>toSysOut().withLabel(transactionsLoggerLabel));
//
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kaProperties);
//
//        // Clean up local datastore in order to guarantee that previous state will be cleared.
//        kafkaStreams.cleanUp();
//
//        kafkaStreams.start();
//    }
//
//    private void kTableDemo() {
//        StreamsBuilder builder = new StreamsBuilder();
//        KTable<String, Transaction> transactionStream = builder.stream(transactionRequestTopicName, Consumed.with(stringSerde, transactionRequestAvroSerde), Materialized.as(stateStorage));
//
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kaProperties);
//        kafkaStreams.cleanUp();
//        kafkaStreams.start();
//    }
//
//    private void kTableDemo() {
//
//        StreamsBuilder builder = new StreamsBuilder();
//
//        final KStream<String, MailingNotif> notifiers = builder.stream("kinaction_mailingNotif");
//        final GlobalKTable<String, Customer> customers = builder.globalTable("kinaction_custinfo");
//
//        // Match to client to notify by email.
//        lists.join(customers,
//                (mailingNotifID, mailing) -> mailing.getCustomerId(),
//                (mailing, customer) -> new Email(mailing, customer))
//            .peek((key, email) -> emailService.sendMessage(email));
//
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kaProperties);
//        kafkaStreams.cleanUp();
//        kafkaStreams.start();
//    }
}
