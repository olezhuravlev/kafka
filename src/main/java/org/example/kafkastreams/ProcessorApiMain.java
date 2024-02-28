package org.example.kafkastreams;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;

public class ProcessorApiMain {

//    public static void main(String[] args) {
//
//        final Serde<String> stringSerde = Serdes.String();
//
//        Deserializer<String> stringDeserializer = stringSerde.deserializer();
//        Serializer<String> stringSerializer = stringSerde.serializer();
//
//        Topology topology = new Topology();
//        topology = topology.addSource(LATEST, "kinaction_source", stringDeserializer, stringDeserializer, "kinaction_source_topic");
//
//        topology = topology.addProcessor("kinactionTestProcessor", () -> new TestProcessor(), "kinaction_source");
//
//        topology = topology.addSink("Kinaction-Destination1-Topic", "kinaction_destination1_topic", stringSerializer, stringSerializer, "kinactionTestProcessor");
//        topology = topology.addSink("Kinaction-Destination2-Topic", "kinaction_destination2_topic", stringSerializer, stringSerializer, "kinactionTestProcessor");
//    }
}
