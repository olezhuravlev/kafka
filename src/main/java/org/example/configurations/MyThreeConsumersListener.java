package org.example.configurations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@KafkaListener
public @interface MyThreeConsumersListener {
    @AliasFor(annotation = KafkaListener.class, attribute = "id") String id();
    @AliasFor(annotation = KafkaListener.class, attribute = "topics") String[] topics();
    @AliasFor(annotation = KafkaListener.class, attribute = "concurrency") String concurrency() default "3";
}
