package com.smartgarden.repository;

import com.smartgarden.model.SensorData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;


import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SensorDataRepository extends JpaRepository<SensorData, Long>, JpaSpecificationExecutor<SensorData> {
    // Lấy bản ghi có ID lớn nhất
    SensorData findTopByOrderByIdDesc();
    @Query("SELECT s FROM SensorData s WHERE s.timestamp >= :startDate AND s.timestamp < :endDate")
    List<SensorData> findAllByDate(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

}