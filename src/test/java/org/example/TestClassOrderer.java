package org.example;

import java.util.Comparator;

import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.ClassOrdererContext;

public class TestClassOrderer implements ClassOrderer {
    
    @Override
    public void orderClasses(ClassOrdererContext classOrdererContext) {
        classOrdererContext.getClassDescriptors().sort(Comparator.comparingInt(TestClassOrderer::getOrder));
    }
    
    private static int getOrder(ClassDescriptor classDescriptor) {
        
        if (classDescriptor.getTestClass().equals(ApplicationTest.class)) {
            return 1;
        } else if (classDescriptor.getTestClass().equals(EmbeddedKafkaIntegrationTest.class)) {
            return 2;
        } else if (classDescriptor.getTestClass().equals(HelloWorldProducerTest.class)) {
            return 3;
        } else if (classDescriptor.getTestClass().equals(KafkaTemplateTests.class)) {
            return 4;
        } else if (classDescriptor.getTestClass().equals(KafkaTestContainersLiveTest.class)) {
            return 5;
        } else {
            return Integer.MAX_VALUE;
        }
    }
}
