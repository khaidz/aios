package net.khaibq.addon.model;

import cn.hutool.core.annotation.Alias;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
public class NonCharge {
    private Long id;
    @Alias("契約サービスID")
    private String networkID;
    @Alias("種別")
    private String type;
    @Alias("uuID")
    private String uuid;
    @Alias("有料OS　課金フラグ")
    private Integer osFlag;
    @Alias("非課金　開始年月")
    private LocalDate startTime;
    @Alias("非課金　終了年月")
    private LocalDate stopTime;
    private Integer status;
    private LocalDateTime createdDate;
}
