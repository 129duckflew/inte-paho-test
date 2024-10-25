package com.rms.config;

import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.handler.annotation.Header;

@MessagingGateway(defaultRequestChannel = "mqttOutboundChannel")
public interface MqttGateway {
    void sendToMqtt(String data);

    void sendToTopic(String payload, @Header(MqttHeaders.TOPIC)String topic);

    void sendToTopic(String payload, @Header(MqttHeaders.TOPIC)String topic,@Header(MqttHeaders.QOS ) int qos);
    void sendWithResp(String payload, @Header(MqttHeaders.TOPIC)String topic,@Header(MqttHeaders.RESPONSE_TOPIC) String responseTopic,@Header(MqttHeaders.QOS ) int qos);
}
