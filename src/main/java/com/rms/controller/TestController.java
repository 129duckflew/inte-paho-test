package com.rms.controller;

import com.rms.config.MqttGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController
{

    @Autowired
    MqttGateway mqttGateway;
    @PostMapping("/test")
    public String sendMsg2Mqtt(int num){
        for (int i = 0; i < num; i++) {
            mqttGateway.sendToTopic(
                " MQTT MSG " + i + " from Spring Boot!","test/",0
            );
        }
        return "OK";
    }
}
