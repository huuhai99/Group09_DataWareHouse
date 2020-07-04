create table log (
    data_file_id int(10) NOT NULL auto_increment primary key, 
    file_name varchar(50) NULL,
    data_file_config_id  int(10) references configuration(config_id),
    file_status varchar(25) NULL, 
    staging_load_count int(55) NULL,
    timestamp_download datetime NULL,
    timestamp_insert_staging datetime NULL,
    timestamp_insert_datawarehouse datetime NULL
);