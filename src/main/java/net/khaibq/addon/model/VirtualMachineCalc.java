package net.khaibq.addon.model;

import lombok.Data;

@Data
public class VirtualMachineCalc {
    private String networkID;
    private String offerId;
    private String offerName;
    private String vmId;
    private String vmName;
    private Double occupancyTime;
    private Double runningTime;
    private Integer cpu;
    private Integer ramSize;
    private Double pauseTime;
    private Integer runningTimeDay;
    private Integer pauseTimeDay;
    private Integer calcType;
    private Integer monthlyPrice;
    private Integer dailyPrice;
    private Integer stopFee;
    private Integer totalAmount;
    private Integer flagMonth; // 1: tính theo tháng
    private Boolean isValid; // Đánh dấu dữ liệu có hợp lệ hay không - do thiếu thông tin cpu hoặc ram_size
    private String invalidReason;

}
