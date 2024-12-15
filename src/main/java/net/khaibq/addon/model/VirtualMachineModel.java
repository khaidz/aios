package net.khaibq.addon.model;

import lombok.Data;

@Data
public class VirtualMachineModel {
    private Long id;
    private String filenameDate;
    private String networkID;
    private String offerId;
    private String offerName;
    private String vmId;
    private String vmName;
    private Double occupancyTime;
    private Double runningTime;
}
