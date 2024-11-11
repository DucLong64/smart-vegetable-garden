package com.smartgarden.service;

import com.smartgarden.model.SensorData;
import com.smartgarden.repository.SensorDataRepository;
import com.smartgarden.specifications.SensorDataSpecifications;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

@Service
public class SensorDataService {

    @Autowired
    private SensorDataRepository sensorDataRepository;

    public SensorData saveSensorData(SensorData sensorData) {
        return sensorDataRepository.save(sensorData);
    }

    public List<SensorData> getAllSensorData() {
        return sensorDataRepository.findAll();
    }
    public SensorData getLatestSensorData() {
        return sensorDataRepository.findTopByOrderByIdDesc();
    }
    public List<SensorData> getDataByDate(LocalDate date) {
        // Đặt thời gian bắt đầu là đầu ngày (00:00:00)
        LocalDateTime startDate = date.atStartOfDay();

        // Đặt thời gian kết thúc là cuối ngày (23:59:59.999999999)
        LocalDateTime endDate = date.atTime(LocalTime.MAX);

        return sensorDataRepository.findAllByDate(startDate, endDate);
    }
    // Phương thức tìm kiếm dữ liệu theo các giá trị tùy chọn
    public List<SensorData> searchSensorData(Double temperatureMin, Double temperatureMax,
                                             Double humidityMin, Double humidityMax,
                                             Double lightMin, Double lightMax) {
        Specification<SensorData> spec = Specification.where(null);

        if (temperatureMin != null || temperatureMax != null) {
            spec = spec.and(SensorDataSpecifications.temperatureGreaterThanOrEqual(temperatureMin))
                    .and(SensorDataSpecifications.temperatureLessThanOrEqual(temperatureMax));
        }

        if (humidityMin != null || humidityMax != null) {
            spec = spec.and(SensorDataSpecifications.humidityGreaterThanOrEqual(humidityMin))
                    .and(SensorDataSpecifications.humidityLessThanOrEqual(humidityMax));
        }

        if (lightMin != null || lightMax != null) {
            spec = spec.and(SensorDataSpecifications.lightGreaterThanOrEqual(lightMin))
                    .and(SensorDataSpecifications.lightLessThanOrEqual(lightMax));
        }

        return sensorDataRepository.findAll(spec);
    }

}