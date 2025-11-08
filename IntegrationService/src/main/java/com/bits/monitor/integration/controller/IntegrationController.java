package com.bits.monitor.integration.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bits.monitor.integration.service.IntegrationService;

import java.util.Map;


@RestController
@RequestMapping(path = "api/v1")
public class IntegrationController {

    @Autowired
    IntegrationService service;

    @GetMapping("monitor/{number}")
    public Integer  calculation(@PathVariable int number) {
        return service.calculate(number);
    }

    @GetMapping("monitor/random/{range}")
    public Map<String, Object> processRandomInt(@PathVariable int range) {
        return service.processRandomInt(range);
    }
}
