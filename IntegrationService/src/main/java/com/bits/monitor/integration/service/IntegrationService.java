package com.bits.monitor.integration.service;

import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class IntegrationService {

    private static final List<List<Integer>> globalDataStore = new ArrayList<>();

    public Integer calculate(int number) {

        if (number<= 1) return number;

        return calculate(number -1) + calculate(number -2);
    }

    public Map<String, Object> processRandomInt(int range) {

        try {

            List<Integer> data = new ArrayList<>(range);

            for (int i = 0; i < range; i++) {
                data.add(ThreadLocalRandom.current().nextInt(0, range));
            }

            globalDataStore.add(data);

            long estimatedMemoryBytes = 0;
            for (List<Integer> list : globalDataStore) {
                estimatedMemoryBytes += (long) list.size() * Integer.BYTES; // 4 bytes each
            }
            double estimatedMemoryMB = estimatedMemoryBytes / (1024.0 * 1024.0);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("message", "Added " + range + " random numbers to memory.");
            response.put("current_global_data_store_size", globalDataStore.size());
            response.put("estimated_memory_consumed_mb", String.format("%.2f MB", estimatedMemoryMB));
            return response;
        }

        finally {
            globalDataStore.clear();
            System.gc();
        }
    }
}
