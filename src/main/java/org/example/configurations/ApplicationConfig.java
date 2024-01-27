package org.example.configurations;

import java.lang.reflect.Method;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;

@Configuration
public class ApplicationConfig {
    
    // Modify annotation attributes before the container created.
    @Bean
    public static KafkaListenerAnnotationBeanPostProcessor.AnnotationEnhancer groupIdEnhancer() {
        return (attrs, element) -> {
            String name = "";
            if (element instanceof Class) {
                name = ((Class<?>) element).getSimpleName();
            } else {
                name = ((Method) element).getDeclaringClass().getSimpleName() + "." + ((Method) element).getName();
            }
            attrs.put("groupId", attrs.get("id") + "." + name);
            return attrs;
        };
    }
}
