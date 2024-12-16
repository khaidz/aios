create table aios.aios_master_basic_price
(
    id                          bigint auto_increment
        primary key,
    network_id                  varchar(50)                         null,
    unit_price_vm_1_cpu         int                                 null,
    unit_price_vm_1_gb          int                                 null,
    unit_price_vdclass_1        int                                 null,
    unit_price_vdclass_2        int                                 null,
    unit_price_window_server    int                                 null,
    unit_price_red_hat_1_to_8   int                                 null,
    unit_price_red_hat_9_to_127 int                                 null,
    unit_price_red_hat_128      int                                 null,
    status                      int       default 1                 not null,
    created_date                timestamp default CURRENT_TIMESTAMP null
);

create table aios.aios_master_non_charge
(
    id           bigint auto_increment
        primary key,
    network_id   varchar(50)                         null,
    type         varchar(100)                        null,
    uuid         varchar(100)                        null,
    os_flag      int                                 null,
    start_time   date                                null,
    stop_time    date                                null,
    status       int       default 1                 not null,
    created_date timestamp default CURRENT_TIMESTAMP null
);

create table aios.aios_master_relative_price
(
    id                  bigint auto_increment
        primary key,
    network_id          varchar(50)                         null,
    type                varchar(100)                        null,
    unit_price_vm_1_cpu int                                 null,
    unit_price_vm_1_gb  int                                 null,
    disk_type           varchar(50)                         null,
    disk_size           int                                 null,
    month_price         int                                 null,
    day_price           int                                 null,
    pause_fee           int                                 null,
    status              int       default 1                 not null,
    created_date        timestamp default CURRENT_TIMESTAMP null
);

