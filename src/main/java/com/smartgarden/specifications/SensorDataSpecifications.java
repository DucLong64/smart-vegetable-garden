package com.smartgarden.specifications;

import com.smartgarden.model.SensorData;
import org.springframework.data.jpa.domain.Specification;

public class SensorDataSpecifications {

    public static Specification<SensorData> temperatureGreaterThanOrEqual(Double temperatureMin) {
        return (root, query, criteriaBuilder) -> {
            if (temperatureMin == null) {
                return criteriaBuilder.conjunction();
            }
            return criteriaBuilder.greaterThanOrEqualTo(root.get("temperature"), temperatureMin);
        };
    }

    public static Specification<SensorData> temperatureLessThanOrEqual(Double temperatureMax) {
        return (root, query, criteriaBuilder) -> {
            if (temperatureMax == null) {
                return criteriaBuilder.conjunction();
            }
            return criteriaBuilder.lessThanOrEqualTo(root.get("temperature"), temperatureMax);
        };
    }

    public static Specification<SensorData> humidityGreaterThanOrEqual(Double humidityMin) {
        return (root, query, criteriaBuilder) -> {
            if (humidityMin == null) {
                return criteriaBuilder.conjunction();
            }
            return criteriaBuilder.greaterThanOrEqualTo(root.get("humidity"), humidityMin);
        };
    }

    public static Specification<SensorData> humidityLessThanOrEqual(Double humidityMax) {
        return (root, query, criteriaBuilder) -> {
            if (humidityMax == null) {
                return criteriaBuilder.conjunction();
            }
            return criteriaBuilder.lessThanOrEqualTo(root.get("humidity"), humidityMax);
        };
    }

    public static Specification<SensorData> lightGreaterThanOrEqual(Double lightMin) {
        return (root, query, criteriaBuilder) -> {
            if (lightMin == null) {
                return criteriaBuilder.conjunction();
            }
            return criteriaBuilder.greaterThanOrEqualTo(root.get("light"), lightMin);
        };
    }

    public static Specification<SensorData> lightLessThanOrEqual(Double lightMax) {
        return (root, query, criteriaBuilder) -> {
            if (lightMax == null) {
                return criteriaBuilder.conjunction();
            }
            return criteriaBuilder.lessThanOrEqualTo(root.get("light"), lightMax);
        };
    }
}
