use mars_raw_data;

create table processing
(
    processing_id   int auto_increment
        primary key,
    data_name varchar(128) null,
    processing_type varchar(128) null,
    dt_result       datetime     null,
    dt_processing   datetime     null,
    state           varchar(256) null,
    version         varchar(128) null
);
