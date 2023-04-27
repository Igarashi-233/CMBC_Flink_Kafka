
CREATE TABLE IF NOT EXISTS data_import_meta_conf(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `file_type` int(1) ,
   `field_map` text ,
    file_path  varchar(1000) ,
    file_path_type  int(1) ,
    process_stage int(1) ,
    project_name varchar(50),
    field_sep varchar(50) ,
    event_name varchar(50) ,
    field_size int(4) ,
    is_header int(1) ,
    out_put_path varchar(1000) ,
    process_count int(11),
    is_delete_out_put_path int(1) ,
    is_importer int(1),
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modify_time TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

 ALTER TABLE data_import_meta_conf ADD read_count INT(11);
 ALTER TABLE data_import_meta_conf ADD exception_type INT(4);


CREATE TABLE IF NOT EXISTS data_import_consumer_offset(
   id varchar(500),
   group_id varchar(100),
   partition_id int(1) ,
   topic  varchar(100) ,
   offset  int(1) ,
   create_date  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   update_date TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (id,group_id, partition_id,topic)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS data_import_type_map(
   source_type varchar(500),
   sensors_type varchar(100),
   PRIMARY KEY (source_type, sensors_type)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


insert into data_import_type_map (source_type,sensors_type) values ("Boolean","Boolean"),("Date","Date"),("Integer","Number"),("Long","Number"),("String","String");