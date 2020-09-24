use mars_raw_data;

create table satellites
(
    satellite_name varchar(128) null,
    satellite_id   int auto_increment
        primary key,
    mission_name   varchar(128) null
);

create table instruments
(
    instrument_id   int auto_increment
        primary key,
    instrument_name varchar(256) null,
    satellite_id    int          not null,
    constraint instruments_satellites_satellite_id_fk
        foreign key (satellite_id) references satellites (satellite_id)
);
