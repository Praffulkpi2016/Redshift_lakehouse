/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents full incremental approach for ODS.
	# File Version: KPI v1.0
*/
begin;
	drop table if exists bec_ods.BOM_OPERATION_SEQUENCES;
	
	CREATE TABLE IF NOT EXISTS bec_ods.bom_operation_sequences
	(
		OPERATION_SEQUENCE_ID NUMERIC(15,0) ENCODE az64, 
		ROUTING_SEQUENCE_ID NUMERIC(15,0) ENCODE az64, 
		OPERATION_SEQ_NUM NUMERIC(15,0) ENCODE az64, 
		LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64, 
		CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		CREATED_BY NUMERIC(15,0) ENCODE az64, 
		LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64, 
		STANDARD_OPERATION_ID NUMERIC(15,0) ENCODE az64, 
		DEPARTMENT_ID NUMERIC(15,0) ENCODE az64, 
		OPERATION_LEAD_TIME_PERCENT NUMERIC(28,10) ENCODE az64, 
		MINIMUM_TRANSFER_QUANTITY NUMERIC(28,10) ENCODE az64, 
		COUNT_POINT_TYPE NUMERIC(15,0) ENCODE az64, 
		OPERATION_DESCRIPTION VARCHAR(240) ENCODE lzo, 
		EFFECTIVITY_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		DISABLE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		BACKFLUSH_FLAG NUMERIC(15,0) ENCODE az64, 
		OPTION_DEPENDENT_FLAG NUMERIC(15,0) ENCODE az64, 
		ATTRIBUTE_CATEGORY VARCHAR(30) ENCODE lzo, 
		ATTRIBUTE1 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE2 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE3 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE4 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE5 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE6 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE7 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE8 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE9 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE10 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE11 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE12 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE13 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE14 VARCHAR(150) ENCODE lzo, 
		ATTRIBUTE15 VARCHAR(150) ENCODE lzo, 
		REQUEST_ID NUMERIC(15,0) ENCODE az64, 
		PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64, 
		PROGRAM_ID NUMERIC(15,0) ENCODE az64, 
		PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		OPERATION_TYPE NUMERIC(15,0) ENCODE az64, 
		REFERENCE_FLAG NUMERIC(15,0) ENCODE az64, 
		PROCESS_OP_SEQ_ID NUMERIC(15,0) ENCODE az64, 
		LINE_OP_SEQ_ID NUMERIC(15,0) ENCODE az64, 
		YIELD NUMERIC(28,10) ENCODE az64, 
		CUMULATIVE_YIELD NUMERIC(28,10) ENCODE az64, 
		REVERSE_CUMULATIVE_YIELD NUMERIC(28,10) ENCODE az64, 
		LABOR_TIME_CALC NUMERIC(28,10) ENCODE az64, 
		MACHINE_TIME_CALC NUMERIC(28,10) ENCODE az64, 
		TOTAL_TIME_CALC NUMERIC(28,10) ENCODE az64, 
		LABOR_TIME_USER NUMERIC(28,10) ENCODE az64, 
		MACHINE_TIME_USER NUMERIC(28,10) ENCODE az64, 
		TOTAL_TIME_USER NUMERIC(28,10) ENCODE az64, 
		NET_PLANNING_PERCENT NUMERIC(28,10) ENCODE az64, 
		X_COORDINATE NUMERIC(28,10) ENCODE az64, 
		Y_COORDINATE NUMERIC(28,10) ENCODE az64, 
		INCLUDE_IN_ROLLUP NUMERIC(28,10) ENCODE az64, 
		OPERATION_YIELD_ENABLED NUMERIC(15,0) ENCODE az64, 
		OLD_OPERATION_SEQUENCE_ID NUMERIC(15,0) ENCODE az64, 
		ACD_TYPE NUMERIC(15,0) ENCODE az64, 
		REVISED_ITEM_SEQUENCE_ID NUMERIC(15,0) ENCODE az64, 
		ORIGINAL_SYSTEM_REFERENCE VARCHAR(50) ENCODE lzo, 
		CHANGE_NOTICE VARCHAR(10) ENCODE lzo, 
		IMPLEMENTATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		ECO_FOR_PRODUCTION NUMERIC(15,0) ENCODE az64, 
		SHUTDOWN_TYPE VARCHAR(30) ENCODE lzo, 
		ACTUAL_IPK NUMERIC(28,10) ENCODE az64, 
		CRITICAL_TO_QUALITY VARCHAR(1) ENCODE lzo, 
		VALUE_ADDED VARCHAR(1) ENCODE lzo, 
		MACHINE_PROCESS_EFFICIENCY NUMERIC(28,10) ENCODE az64, 
		LABOR_PROCESS_EFFICIENCY NUMERIC(28,10) ENCODE az64, 
		TOTAL_PROCESS_EFFICIENCY NUMERIC(28,10) ENCODE az64, 
		LONG_DESCRIPTION VARCHAR(4000) ENCODE lzo, 
		CONFIG_ROUTING_ID NUMERIC(15,0) ENCODE az64, 
		MODEL_OP_SEQ_ID NUMERIC(15,0) ENCODE az64, 
		LOWEST_ACCEPTABLE_YIELD NUMERIC(28,10) ENCODE az64, 
		USE_ORG_SETTINGS NUMERIC(28,10) ENCODE az64, 
		QUEUE_MANDATORY_FLAG NUMERIC(15,0) ENCODE az64, 
		RUN_MANDATORY_FLAG NUMERIC(15,0) ENCODE az64, 
		TO_MOVE_MANDATORY_FLAG NUMERIC(15,0) ENCODE az64, 
		SHOW_NEXT_OP_BY_DEFAULT NUMERIC(15,0) ENCODE az64, 
		SHOW_SCRAP_CODE NUMERIC(15,0) ENCODE az64, 
		SHOW_LOT_ATTRIB NUMERIC(15,0) ENCODE az64, 
		TRACK_MULTIPLE_RES_USAGE_DATES NUMERIC(15,0) ENCODE az64, 
		CHECK_SKILL NUMERIC(15,0) ENCODE az64, 
		kca_operation VARCHAR(10)   ENCODE lzo,
		is_deleted_flg VARCHAR(2) ENCODE lzo,
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.bom_operation_sequences
	(
		OPERATION_SEQUENCE_ID, 
		ROUTING_SEQUENCE_ID, 
		OPERATION_SEQ_NUM, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		STANDARD_OPERATION_ID, 
		DEPARTMENT_ID, 
		OPERATION_LEAD_TIME_PERCENT, 
		MINIMUM_TRANSFER_QUANTITY, 
		COUNT_POINT_TYPE, 
		OPERATION_DESCRIPTION, 
		EFFECTIVITY_DATE, 
		DISABLE_DATE, 
		BACKFLUSH_FLAG, 
		OPTION_DEPENDENT_FLAG, 
		ATTRIBUTE_CATEGORY, 
		ATTRIBUTE1, 
		ATTRIBUTE2, 
		ATTRIBUTE3, 
		ATTRIBUTE4, 
		ATTRIBUTE5, 
		ATTRIBUTE6, 
		ATTRIBUTE7, 
		ATTRIBUTE8, 
		ATTRIBUTE9, 
		ATTRIBUTE10, 
		ATTRIBUTE11, 
		ATTRIBUTE12, 
		ATTRIBUTE13, 
		ATTRIBUTE14, 
		ATTRIBUTE15, 
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		OPERATION_TYPE, 
		REFERENCE_FLAG, 
		PROCESS_OP_SEQ_ID, 
		LINE_OP_SEQ_ID, 
		YIELD, 
		CUMULATIVE_YIELD, 
		REVERSE_CUMULATIVE_YIELD, 
		LABOR_TIME_CALC, 
		MACHINE_TIME_CALC, 
		TOTAL_TIME_CALC, 
		LABOR_TIME_USER, 
		MACHINE_TIME_USER, 
		TOTAL_TIME_USER, 
		NET_PLANNING_PERCENT, 
		X_COORDINATE, 
		Y_COORDINATE, 
		INCLUDE_IN_ROLLUP, 
		OPERATION_YIELD_ENABLED, 
		OLD_OPERATION_SEQUENCE_ID, 
		ACD_TYPE, 
		REVISED_ITEM_SEQUENCE_ID, 
		ORIGINAL_SYSTEM_REFERENCE, 
		CHANGE_NOTICE, 
		IMPLEMENTATION_DATE, 
		ECO_FOR_PRODUCTION, 
		SHUTDOWN_TYPE, 
		ACTUAL_IPK, 
		CRITICAL_TO_QUALITY, 
		VALUE_ADDED, 
		MACHINE_PROCESS_EFFICIENCY, 
		LABOR_PROCESS_EFFICIENCY, 
		TOTAL_PROCESS_EFFICIENCY, 
		LONG_DESCRIPTION, 
		CONFIG_ROUTING_ID, 
		MODEL_OP_SEQ_ID, 
		LOWEST_ACCEPTABLE_YIELD, 
		USE_ORG_SETTINGS, 
		QUEUE_MANDATORY_FLAG, 
		RUN_MANDATORY_FLAG, 
		TO_MOVE_MANDATORY_FLAG, 
		SHOW_NEXT_OP_BY_DEFAULT, 
		SHOW_SCRAP_CODE, 
		SHOW_LOT_ATTRIB, 
		TRACK_MULTIPLE_RES_USAGE_DATES, 
		CHECK_SKILL, 
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		OPERATION_SEQUENCE_ID, 
		ROUTING_SEQUENCE_ID, 
		OPERATION_SEQ_NUM, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		STANDARD_OPERATION_ID, 
		DEPARTMENT_ID, 
		OPERATION_LEAD_TIME_PERCENT, 
		MINIMUM_TRANSFER_QUANTITY, 
		COUNT_POINT_TYPE, 
		OPERATION_DESCRIPTION, 
		EFFECTIVITY_DATE, 
		DISABLE_DATE, 
		BACKFLUSH_FLAG, 
		OPTION_DEPENDENT_FLAG, 
		ATTRIBUTE_CATEGORY, 
		ATTRIBUTE1, 
		ATTRIBUTE2, 
		ATTRIBUTE3, 
		ATTRIBUTE4, 
		ATTRIBUTE5, 
		ATTRIBUTE6, 
		ATTRIBUTE7, 
		ATTRIBUTE8, 
		ATTRIBUTE9, 
		ATTRIBUTE10, 
		ATTRIBUTE11, 
		ATTRIBUTE12, 
		ATTRIBUTE13, 
		ATTRIBUTE14, 
		ATTRIBUTE15, 
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		OPERATION_TYPE, 
		REFERENCE_FLAG, 
		PROCESS_OP_SEQ_ID, 
		LINE_OP_SEQ_ID, 
		YIELD, 
		CUMULATIVE_YIELD, 
		REVERSE_CUMULATIVE_YIELD, 
		LABOR_TIME_CALC, 
		MACHINE_TIME_CALC, 
		TOTAL_TIME_CALC, 
		LABOR_TIME_USER, 
		MACHINE_TIME_USER, 
		TOTAL_TIME_USER, 
		NET_PLANNING_PERCENT, 
		X_COORDINATE, 
		Y_COORDINATE, 
		INCLUDE_IN_ROLLUP, 
		OPERATION_YIELD_ENABLED, 
		OLD_OPERATION_SEQUENCE_ID, 
		ACD_TYPE, 
		REVISED_ITEM_SEQUENCE_ID, 
		ORIGINAL_SYSTEM_REFERENCE, 
		CHANGE_NOTICE, 
		IMPLEMENTATION_DATE, 
		ECO_FOR_PRODUCTION, 
		SHUTDOWN_TYPE, 
		ACTUAL_IPK, 
		CRITICAL_TO_QUALITY, 
		VALUE_ADDED, 
		MACHINE_PROCESS_EFFICIENCY, 
		LABOR_PROCESS_EFFICIENCY, 
		TOTAL_PROCESS_EFFICIENCY, 
		LONG_DESCRIPTION, 
		CONFIG_ROUTING_ID, 
		MODEL_OP_SEQ_ID, 
		LOWEST_ACCEPTABLE_YIELD, 
		USE_ORG_SETTINGS, 
		QUEUE_MANDATORY_FLAG, 
		RUN_MANDATORY_FLAG, 
		TO_MOVE_MANDATORY_FLAG, 
		SHOW_NEXT_OP_BY_DEFAULT, 
		SHOW_SCRAP_CODE, 
		SHOW_LOT_ATTRIB, 
		TRACK_MULTIPLE_RES_USAGE_DATES, 
		CHECK_SKILL, 
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.bom_operation_sequences
	);
	
end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='bom_operation_sequences'; 

commit;