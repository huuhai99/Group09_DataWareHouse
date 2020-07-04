create table `configuration`(
    config_id int(10) NOT NULL auto_increment primary key,
    target_table varchar(255) NULL, 
    file_type varchar(255) NULL,
    import_dir varchar(255) NULL, 
    success_dir varchar(255) NULL,
    error_dir varchar(255) NULL,
    column_list varchar(255) NULL,
	variable_list varchar(255) NULL,
	delimmeter varchar(5) NULL
    
);