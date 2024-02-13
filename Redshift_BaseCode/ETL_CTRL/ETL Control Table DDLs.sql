--DROP TABLE bec_etl_ctrl.batch_dw_info;
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.batch_dw_info
(
	dw_table_name VARCHAR(100)   ENCODE lzo
	,batch_priority INTEGER   ENCODE az64
	,last_refresh_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refresh_frequency VARCHAR(10)   ENCODE lzo
	,disable_flag BOOLEAN   ENCODE RAW
	,load_type VARCHAR(1)   ENCODE lzo
	,parallel_on BOOLEAN   ENCODE RAW
	,executebegints TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,batch_name VARCHAR(30)   ENCODE lzo
	,unload_flag BOOLEAN   ENCODE RAW
	,prune_days INTEGER   ENCODE az64
	,object_level VARCHAR(2000)   ENCODE lzo
	,last_refresh_date_pst TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

--DROP TABLE bec_etl_ctrl.batch_ods_info;
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.batch_ods_info
(
	ods_table_name VARCHAR(100)   ENCODE lzo
	,last_refresh_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refresh_frequency VARCHAR(10)   ENCODE lzo
	,disable_flag BOOLEAN   ENCODE RAW
	,load_type VARCHAR(1)   ENCODE lzo
	,batch_name VARCHAR(150)   ENCODE lzo
	,executebegints TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,prune_days INTEGER   ENCODE az64
	,object_priority VARCHAR(2000)   ENCODE lzo
)
DISTSTYLE AUTO
;

--DROP TABLE bec_etl_ctrl.batch_variable_info;
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.batch_variable_info
(
	seq_num INTEGER   ENCODE az64
	,variable_key VARCHAR(100)   ENCODE lzo
	,variable_value VARCHAR(100)   ENCODE lzo
)
DISTSTYLE AUTO
;

--DROP TABLE bec_etl_ctrl.etlsourceappid;
CREATE TABLE IF NOT EXISTS bec_etl_ctrl.etlsourceappid
(
	source_system VARCHAR(20)   ENCODE lzo
	,system_id INTEGER   ENCODE az64
)
DISTSTYLE AUTO
;