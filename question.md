I was using the Spring-Integration-mqtt module to send and receive MQTT messages and the following exception occurred:
```log
2024-10-22 10:56:45.161 [][] ERROR o.s.i.handler.LoggingHandler:250 - org.springframework.messaging.MessageHandlingException: Failed to publish to MQTT in the [bean 'mqttOutboundHandler' for component 'mqttOutboundFlow.org.springframework.integration.config.ConsumerEndpointFactoryBean#1'; defined in: 'class path resource [com/rms/config/mqtt/MqttConfig.class]'; from source: 'com.rms.config.mqtt.MqttConfig.mqttOutboundHandler(org.eclipse.paho.mqttv5.client.MqttConnectionOptions)'], failedMessage=GenericMessage [payload={"value":[],"unitId":741,"fieldName":"landingCall-down-front-1-32","isRunningData":false,"isError":false}, headers={replyChannel=nullChannel, errorChannel=, mqtt_qos=0, id=1237ba75-e408-10ff-e322-5f692dd8970e, mqtt_topic=status/741/landingCall-down-front-1-32, timestamp=1729565799271}]
	at org.springframework.integration.mqtt.outbound.Mqttv5PahoMessageHandler.publish(Mqttv5PahoMessageHandler.java:283)
	at org.springframework.integration.mqtt.outbound.Mqttv5PahoMessageHandler.handleMessageInternal(Mqttv5PahoMessageHandler.java:222)
	at org.springframework.integration.handler.AbstractMessageHandler.doHandleMessage(AbstractMessageHandler.java:105)
	at org.springframework.integration.handler.AbstractMessageHandler.handleWithMetrics(AbstractMessageHandler.java:90)
	at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:70)
	at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:132)
	at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:148)
	at org.springframework.integration.dispatcher.UnicastingDispatcher$1.run(UnicastingDispatcher.java:129)
	at org.springframework.integration.util.ErrorHandlingTaskExecutor.lambda$execute$0(ErrorHandlingTaskExecutor.java:56)
	at java.base/java.util.concurrent.ThreadPerTaskExecutor$TaskRunner.run(ThreadPerTaskExecutor.java:314)
	at java.base/java.lang.VirtualThread.run(VirtualThread.java:311)
Caused by: Internal error, caused by no new message IDs being available (32001)
	at org.eclipse.paho.mqttv5.client.internal.ExceptionHelper.createMqttException(ExceptionHelper.java:32)
	at org.eclipse.paho.mqttv5.client.internal.ClientState.getNextMessageId(ClientState.java:1454)
	at org.eclipse.paho.mqttv5.client.internal.ClientState.send(ClientState.java:511)
	at org.eclipse.paho.mqttv5.client.internal.ClientComms.internalSend(ClientComms.java:155)
	at org.eclipse.paho.mqttv5.client.internal.ClientComms.sendNoWait(ClientComms.java:218)
	at org.eclipse.paho.mqttv5.client.MqttAsyncClient.publish(MqttAsyncClient.java:1530)
	at org.eclipse.paho.mqttv5.client.MqttAsyncClient.publish(MqttAsyncClient.java:1499)
	at org.springframework.integration.mqtt.outbound.Mqttv5PahoMessageHandler.publish(Mqttv5PahoMessageHandler.java:271)
	... 10 more
```
I use OpenJDK21,Spring6,SpringBoot3.2.5,PahoMQTT1.2.5 as dev-environment;
The following is my core logic. I configured two Mqtt clients using Spring-Integration integration flow. One is used to receive information, and the other is used to send messages. The frequency of sending messages is about 5000 messages per second.
```yaml
mqtt:
  client-id-inbound: rms-inbound
  client-id-outbound: rms-outbound
  url: tcp://127.0.0.1:1883
  username: rms
  password: 123456
```
```java
package com.rms.config.mqtt;

import com.rms.domain.entity.Prop;
import com.rms.protocol.CanProtocolLoader;
import com.rms.service.LogService;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.IMqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MqttDefaultFilePersistence;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.mqtt.core.ClientManager;
import org.springframework.integration.mqtt.core.Mqttv5ClientManager;
import org.springframework.integration.mqtt.inbound.Mqttv5PahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.Mqttv5PahoMessageHandler;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.converter.*;

import java.util.concurrent.Executors;


@Configuration
@IntegrationComponentScan("com.rms.config")
@Slf4j
@ConfigurationProperties(prefix = "mqtt")
@Data
public class MqttConfig {
    private String clientIdInbound;
    private String clientIdOutbound;
    private String url;
    private String password;
    private String username;
    @Bean
    public MqttConnectionOptions mqttConnectOptions(){
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setServerURIs(new String[] { url});
        options.setUserName(username);
        options.setPassword(password.getBytes());
        options.setAutomaticReconnect(true);
        return options;
    }
    @Bean
    public ClientManager<IMqttAsyncClient, MqttConnectionOptions> clientManager(MqttConnectionOptions options) {
        Mqttv5ClientManager clientManager = new Mqttv5ClientManager(options, clientIdInbound);
        clientManager.setPersistence(new MqttDefaultFilePersistence());
        return clientManager;
    }

    @Bean
    public SimpleMessageConverter simpleMessageConverter(){
        return new SimpleMessageConverter();
    }

    @Bean
    public MessageHandler mqttOutboundHandler(MqttConnectionOptions connectionOptions) {
        Mqttv5PahoMessageHandler messageHandler = new Mqttv5PahoMessageHandler(connectionOptions,clientIdOutbound);
        messageHandler.setAsync(true);
        messageHandler.setDefaultTopic("defaultTopic");
        messageHandler.setDefaultQos(MqttQoS.AT_MOST_ONCE.value());
        messageHandler.setConverter(simpleMessageConverter());
        return messageHandler;
    }


    @Bean
    public IntegrationFlow mqttOutboundFlow(MessageHandler mqttOutboundHandler){
        return IntegrationFlow.from("mqttOutboundChannel")
                .channel(MessageChannels.executor(Executors.newVirtualThreadPerTaskExecutor()))
                .handle(mqttOutboundHandler)
                .get();
    }

    @Bean
    public IntegrationFlow statusInboundFlow(ClientManager<IMqttAsyncClient, MqttConnectionOptions> clientManage){
        Mqttv5PahoMessageDrivenChannelAdapter messageProducer  =
                new Mqttv5PahoMessageDrivenChannelAdapter(clientManage, "status/+/#");
        return IntegrationFlow.from(messageProducer)
                .channel(MessageChannels.executor(Executors.newVirtualThreadPerTaskExecutor()))
                .transform(Transformers.objectToString())
                .transform(Transformers.fromJson(Prop.class))
                .route(Prop.class,prop->{
                    if (Boolean.TRUE.equals(prop.getIsError())){
                        return "errorDataChannel";
                    }
                    else if (Boolean.TRUE.equals(prop.getIsRunningData())){
                        return "runningDataChannel";
                    }
                    else {
                        return "discardChannel";
                    }
                })
                .get();
    }
    @Bean
    public CanProtocolLoader canProtocolLoader(){
        return new CanProtocolLoader();
    }
    @Bean
    public UnitErrorHandler errorLogHandler(LogService logService ){
        return new UnitErrorHandler(logService);
    }
    @Bean
    public UnitRunningDataHandler unitRunningDataHandler(LogService logService){
        return new UnitRunningDataHandler(logService);
    }
    @Bean
    public IntegrationFlow errorLogChannelFlow(UnitErrorHandler unitErrorHandler){
        return IntegrationFlow.from("errorDataChannel")
                .channel(MessageChannels.executor(Executors.newVirtualThreadPerTaskExecutor()))
                .handle(unitErrorHandler)
                .get();
    }
    @Bean
    public IntegrationFlow runningDataChannelFlow(UnitRunningDataHandler runningDataHandler){
        return IntegrationFlow.from("runningDataChannel")
                .channel(MessageChannels.executor(Executors.newVirtualThreadPerTaskExecutor()))
                .handle(runningDataHandler)
                .get();
    }
  
    @Bean
    public IntegrationFlow discardChannelFlow(){
        return IntegrationFlow.from("discardChannel")
                .channel(MessageChannels.executor(Executors.newVirtualThreadPerTaskExecutor()))
                .handle(message -> {
                })
                .get();
    }
}
@Slf4j
@RequiredArgsConstructor
public class UnitErrorHandler implements MessageHandler {
    private final LogService logService;

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        Prop<?> prop = (Prop<?>) message.getPayload();
        logService.saveErrorLog(prop);
    }

}
```
I use the MessagingGateway annotation to direct messages to my `mqttOutboundChannel` so I can use MqttGateway to send mqtt messages
```java
@MessagingGateway(defaultRequestChannel = "mqttOutboundChannel")
public interface MqttGateway {
    void sendToMqtt(String data);

    void sendToTopic(String payload, @Header(MqttHeaders.TOPIC)String topic);

    void sendToTopic(String payload, @Header(MqttHeaders.TOPIC)String topic,@Header(MqttHeaders.QOS ) int qos);
    void sendWithResp(String payload, @Header(MqttHeaders.TOPIC)String topic,@Header(MqttHeaders.RESPONSE_TOPIC) String responseTopic,@Header(MqttHeaders.QOS ) int qos);
}


```

When the message delivery rate is very low, generally less than 200 messages per second, the error I mentioned at the beginning will not occur, but when I increase the message production rate to 5,000.

When I removed the inbound part of mqtt messages, I found that the program worked very well without any errors. Once I introduced the `statusInboundFlow` integrated flow in the above code, the mqtt outbound client could not send messages normally and a large number of messages appeared. The exception mentioned before


I expect that according to the above logic, I can send and receive MQTT messages normally