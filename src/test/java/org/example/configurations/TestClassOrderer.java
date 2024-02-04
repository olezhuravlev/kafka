package org.example.configurations;

import java.util.Comparator;

import org.example.KafkaTemplateEmbedded;
import org.example.KafkaTemplateEmbeddedPort9092;
import org.example.KafkaTestContainer;
import org.example.ProducerConsumerEmbeddedPort9092;
import org.example.AvroZooKafkaSchemaContainers;
import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.ClassOrdererContext;

public class TestClassOrderer implements ClassOrderer {
    
    @Override
    public void orderClasses(ClassOrdererContext classOrdererContext) {
        classOrdererContext.getClassDescriptors().sort(Comparator.comparingInt(TestClassOrderer::getOrder));
    }
    
    private static int getOrder(ClassDescriptor classDescriptor) {
        
        if (classDescriptor.getTestClass().equals(KafkaTemplateEmbedded.class)) {
            return 1;
        } else if (classDescriptor.getTestClass().equals(ProducerConsumerEmbeddedPort9092.class)) {
            return 2;
        } else if (classDescriptor.getTestClass().equals(AvroZooKafkaSchemaContainers.class)) {
            return 5;
        } else if (classDescriptor.getTestClass().equals(KafkaTemplateEmbeddedPort9092.class)) {
            return 3;
        } else if (classDescriptor.getTestClass().equals(KafkaTestContainer.class)) {
            return 4;
        } else {
            return Integer.MAX_VALUE;
        }
    }
}
