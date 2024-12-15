package net.khaibq.addon.model;

import cn.hutool.core.annotation.Alias;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RelativePrice {
    private Long id;
    @Alias("契約サービスID")
    private String networkID;
    @Alias("種別")
    private String type;
    @Alias("仮想サーバvCPU(core)")
    private Integer unitPriceVM1Cpu;
    @Alias("仮想サーバメモリ")
    private Integer unitPriceVM1Gb;
    @Alias("仮想ディスク class")
    private String diskType;
    @Alias("仮想ディスク サイズ")
    private Integer diskSize;
    @Alias("月額料金")
    private Integer monthPrice;
    @Alias("日料金")
    private Integer dayPrice;
    @Alias("停止料金")
    private Integer pauseFee;
    private Integer status;
    private LocalDateTime createdDate;
}