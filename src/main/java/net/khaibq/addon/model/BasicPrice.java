package net.khaibq.addon.model;

import cn.hutool.core.annotation.Alias;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class BasicPrice {
    private Long id;
    @Alias("契約サービスID")
    private String networkID;
    @Alias("仮想サーバ1vCPU(1core)単価")
    private Integer unitPriceVM1Cpu;
    @Alias("仮想サーバメモリ(1GB)単価")
    private Integer unitPriceVM1Gb;
    @Alias("仮想ディスク class1(10GB)単価")
    private Integer unitPriceVDClass1;
    @Alias("仮想ディスク class2(10GB)単価")
    private Integer unitPriceVDClass2;
    @Alias("Windows Server 単価")
    private Integer unitPriceWindowServer;
    @Alias("RedHat　１to8vCPUs単価")
    private Integer unitPriceRedHat1to8;
    @Alias("RedHat　９to127vCPUs単価")
    private Integer unitPriceRedHat9to127;
    @Alias("RedHat　128vCPUs～単価")
    private Integer unitPriceRedHat128;
    @Alias("Redhat　集計フラグ")
    private Integer redhatFlag;
    private Integer status;
    private LocalDateTime createdDate;
}