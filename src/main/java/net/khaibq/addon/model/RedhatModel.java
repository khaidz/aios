package net.khaibq.addon.model;

import lombok.Data;

@Data
public class RedhatModel {
    private String networkID;
    private String vmId;
    private String vmName;
    private String instanceName;
    private String domain;
    private String guestOsId;
    private String templateUuid;
    private Integer vCpu;
    private Integer calcType;
    private Integer monthlyPrice;
    private Integer dailyPrice;
    private Boolean isValid;
    private String invalidReason;
}
