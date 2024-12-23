package net.khaibq.addon.model;

import lombok.Data;

@Data
public class WindowModel {
    private String networkID;
    private String vmId;
    private String guestOsId;
    private String name;
    private String path;
    private Integer calcType;
    private Integer price;
    private Integer count;
    private Boolean isValid;
    private String invalidReason;
}
