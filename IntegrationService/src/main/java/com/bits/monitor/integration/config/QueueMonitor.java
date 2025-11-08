package com.bits.monitor.integration.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class QueueMonitor {

    private final MeterRegistry registry;
    private final BlockingQueue<String> requestQueue = new LinkedBlockingQueue<>();

    public QueueMonitor(MeterRegistry registry) {
        this.registry = registry;
        System.out.println(">>> QueueMonitor constructor called");
    }

    @PostConstruct
    public void registerGauge() {
        System.out.println(">>> Registering gauge integration_queue_length...");
        Gauge.builder("integration_queue_length", requestQueue, q -> (double) q.size())
                .description("Current queue length of pending requests")
                .register(registry);
        System.out.println(">>> Gauge registered successfully!");
    }

    @Bean
    public BlockingQueue<String> requestQueue() {
        return requestQueue;
    }
}