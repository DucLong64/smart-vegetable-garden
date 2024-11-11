package com.smartgarden.controller;

import com.smartgarden.model.SensorData;
import com.smartgarden.service.SensorDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.time.LocalDate;

@RestController
@RequestMapping("/api/sensors")
public class SensorDataController {

    @Autowired
    private SensorDataService sensorDataService;

    @PostMapping("/data")
    public SensorData createSensorData(@RequestBody SensorData sensorData) {
        sensorData.setTimestamp(LocalDateTime.now()); // Cập nhật thời gian
        return sensorDataService.saveSensorData(sensorData);
    }
    //Api lấy toàn bộ dữ liệu
    @GetMapping("/data")
    public List<SensorData> getAllSensorData() {
        return sensorDataService.getAllSensorData();
    }
    //Api lấy dữ liệu gần nhất, realtime
    @GetMapping("/latest")
    public ResponseEntity<SensorData> getLatestSensorData() {
        SensorData latestData = sensorDataService.getLatestSensorData();
        if (latestData != null) {
            return ResponseEntity.ok(latestData);
        } else {
            return ResponseEntity.noContent().build();  // Trả về 204 nếu không có dữ liệu
        }
    }
    // API lấy dữ liệu theo ngày
    @GetMapping("/by-date")
    public ResponseEntity<List<SensorData>> getDataByDate(
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        List<SensorData> dataList = sensorDataService.getDataByDate(date);
        return dataList.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(dataList);
    }
    @GetMapping("/search")
    public ResponseEntity<List<SensorData>> searchSensorData(
            @RequestParam(value = "temperatureMin", required = false) Double temperatureMin,
            @RequestParam(value = "temperatureMax", required = false) Double temperatureMax,
            @RequestParam(value = "humidityMin", required = false) Double humidityMin,
            @RequestParam(value = "humidityMax", required = false) Double humidityMax,
            @RequestParam(value = "lightMin", required = false) Double lightMin,
            @RequestParam(value = "lightMax", required = false) Double lightMax) {

        List<SensorData> dataList = sensorDataService.searchSensorData(temperatureMin, temperatureMax,
                humidityMin, humidityMax,
                lightMin, lightMax);
        return dataList.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(dataList);
    }
}
