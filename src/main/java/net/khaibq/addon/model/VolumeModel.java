package net.khaibq.addon.model;

import lombok.Data;

@Data
public class VolumeModel {
    private String filenameDate;
    private String networkID;
    private String volumeID;
    private String diskOfferingID;
    private String diskOfferingName;
    private String templateID;
    private String templateName;
    private String zoneID;
    private String zoneName;
    private Long allocatedSize;
    private Double occupancyTime;
    private String clazz;
    private String state;
    private Integer calcType;
    private Integer monthlyPrice;
    private Integer dailyPrice;
    private Integer allocatedSizeGB;
    private Integer occupancyTimeDay;
    private Integer flagMonth;
    private Boolean isValid;
    private String invalidReason;
}
