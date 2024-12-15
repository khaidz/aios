package net.khaibq.addon.model;

import lombok.Data;

@Data
public class ServiceOfferingModel {
    private Long id;
    private String uuid;
    private String domainPath;
    private String name;
    private Integer cpu;
    private Integer ramSize;
}
