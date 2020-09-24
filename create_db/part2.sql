create table data
(
    ID int(11) NOT NULL,
    type char(3) NOT NULL,
    orbit_dt_update date NOT NULL,
    orbit_id char(4) NOT NULL,
    dt_update date NOT NULL,
    instrument_id int NOT NULL,
    file_NAME char(50) default NULL,
    file_SIZE int default NULL,
    file_VOLUME char(30) default NULL,
    file_STORAGEFILE char(30) default NULL,
    PRIMARY KEY (ID),
    CONSTRAINT link UNIQUE (ID, type, file_NAME)
)
