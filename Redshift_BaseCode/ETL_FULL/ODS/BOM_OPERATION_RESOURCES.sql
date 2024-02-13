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
	drop table if exists bec_ods.BOM_OPERATION_RESOURCES;
	
	CREATE TABLE IF NOT EXISTS bec_ods.bom_operation_resources
	(
		OPERATION_SEQUENCE_ID NUMERIC(15,0) ENCODE az64, 
		RESOURCE_SEQ_NUM NUMERIC(15,0) ENCODE az64, 
		RESOURCE_ID NUMERIC(15,0) ENCODE az64, 
		ACTIVITY_ID NUMERIC(15,0) ENCODE az64, 
		STANDARD_RATE_FLAG NUMERIC(15,0) ENCODE az64, 
		ASSIGNED_UNITS NUMERIC(28,10) ENCODE az64, 
		USAGE_RATE_OR_AMOUNT NUMERIC(28,10) ENCODE az64, 
		USAGE_RATE_OR_AMOUNT_INVERSE NUMERIC(28,10) ENCODE az64, 
		BASIS_TYPE NUMERIC(15,0) ENCODE az64, 
		SCHEDULE_FLAG NUMERIC(15,0) ENCODE az64, 
		LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64, 
		CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		CREATED_BY NUMERIC(15,0) ENCODE az64, 
		LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64, 
		RESOURCE_OFFSET_PERCENT NUMERIC(28,10) ENCODE az64, 
		AUTOCHARGE_TYPE NUMERIC(15,0) ENCODE az64, 
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
		SCHEDULE_SEQ_NUM NUMERIC(15,0) ENCODE az64, 
		SUBSTITUTE_GROUP_NUM NUMERIC(15,0) ENCODE az64, 
		PRINCIPLE_FLAG NUMERIC(15,0) ENCODE az64, 
		SETUP_ID NUMERIC(15,0) ENCODE az64, 
		CHANGE_NOTICE VARCHAR(10) ENCODE lzo, 
		ACD_TYPE NUMERIC(15,0) ENCODE az64, 
		ORIGINAL_SYSTEM_REFERENCE VARCHAR(50) ENCODE lzo, 
		kca_operation VARCHAR(10)   ENCODE lzo,
		is_deleted_flg VARCHAR(2) ENCODE lzo,
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.bom_operation_resources
	(
		OPERATION_SEQUENCE_ID, 
		RESOURCE_SEQ_NUM, 
		RESOURCE_ID, 
		ACTIVITY_ID, 
		STANDARD_RATE_FLAG, 
		ASSIGNED_UNITS, 
		USAGE_RATE_OR_AMOUNT, 
		USAGE_RATE_OR_AMOUNT_INVERSE, 
		BASIS_TYPE, 
		SCHEDULE_FLAG, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		RESOURCE_OFFSET_PERCENT, 
		AUTOCHARGE_TYPE, 
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
		SCHEDULE_SEQ_NUM, 
		SUBSTITUTE_GROUP_NUM, 
		PRINCIPLE_FLAG, 
		SETUP_ID, 
		CHANGE_NOTICE, 
		ACD_TYPE, 
		ORIGINAL_SYSTEM_REFERENCE,
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		OPERATION_SEQUENCE_ID, 
		RESOURCE_SEQ_NUM, 
		RESOURCE_ID, 
		ACTIVITY_ID, 
		STANDARD_RATE_FLAG, 
		ASSIGNED_UNITS, 
		USAGE_RATE_OR_AMOUNT, 
		USAGE_RATE_OR_AMOUNT_INVERSE, 
		BASIS_TYPE, 
		SCHEDULE_FLAG, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		RESOURCE_OFFSET_PERCENT, 
		AUTOCHARGE_TYPE, 
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
		SCHEDULE_SEQ_NUM, 
		SUBSTITUTE_GROUP_NUM, 
		PRINCIPLE_FLAG, 
		SETUP_ID, 
		CHANGE_NOTICE, 
		ACD_TYPE, 
		ORIGINAL_SYSTEM_REFERENCE,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.bom_operation_resources
	);
	
end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='bom_operation_resources'; 

commit;