create table data
(
    ID int auto_increment primary key,
    type char(3) NOT NULL,
    dt_update date NOT NULL,
    orbit_id char(4) NOT NULL,
    orbit_dt_update date NOT NULL,
    instrument_id int NOT NULL,
    sdate date NOT NULL,
    coord_system char(14) NOT NULL,
    min_lat float(7,4) NOT NULL,
    max_lat float(7,4) NOT NULL,
    west_long float(7,4) NOT NULL,
    east_long float(7,4) NOT NULL,
    file_NAME char(50) default NULL,
    file_SIZE int default NULL,
    file_VOLUME char(30) default NULL,
    file_STORAGEFILE char(30) default NULL
);
