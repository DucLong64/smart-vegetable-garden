package com.smartgarden.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgarden.model.SensorData;
import jakarta.annotation.PostConstruct;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class MqttSubscriberService {

    private MqttClient mqttClient;

    @Value("${mqtt.broker.url}")
    private String mqttBrokerUrl;

    @Value("${mqtt.client.id}")
    private String clientId;

    @Value("${mqtt.topic}")
    private String topic;

    @PostConstruct
    public void init() {
        try {
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("Client ID must not be null or empty");
            }
            mqttClient = new MqttClient(mqttBrokerUrl, clientId);
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Mất kết nối với MQTT Broker!");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    handleIncomingMessage(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });
            connect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void connect() throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        mqttClient.connect(options);
        mqttClient.subscribe(topic);
    }

    @Autowired
    private SensorDataService sensorDataService;

    private void handleIncomingMessage(String topic, MqttMessage message) {
        String payload = new String(message.getPayload());
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            SensorData sensorData = objectMapper.readValue(payload, SensorData.class);
            sensorData.setTimestamp(LocalDateTime.now()); // Cập nhật thời gian
            sensorDataService.saveSensorData(sensorData); // Lưu vào cơ sở dữ liệu
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
