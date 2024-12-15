package net.khaibq.addon.model;

import lombok.Data;

@Data
public class VolumeS12S34Model {
    private Long id;
    private Integer accountId;
    private Integer domainId;
    private String name;
    private String uuid;
    private String poolName;
    private String state;
}
