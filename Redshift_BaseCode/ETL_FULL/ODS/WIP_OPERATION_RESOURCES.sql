/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.WIP_OPERATION_RESOURCES;

CREATE TABLE IF NOT EXISTS bec_ods.WIP_OPERATION_RESOURCES
(
	 
	 WIP_ENTITY_ID NUMERIC(15,0) ENCODE az64
	,OPERATION_SEQ_NUM NUMERIC(15,0) ENCODE az64
	,RESOURCE_SEQ_NUM NUMERIC(15,0) ENCODE az64
	,ORGANIZATION_ID NUMERIC(15,0) ENCODE az64
	,REPETITIVE_SCHEDULE_ID NUMERIC(15,0) ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,CREATED_BY NUMERIC(15,0) ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64
	,REQUEST_ID NUMERIC(15,0) ENCODE az64
	,PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,RESOURCE_ID NUMERIC(15,0) ENCODE az64
	,UOM_CODE VARCHAR(3) ENCODE lzo
	,BASIS_TYPE NUMERIC(15,0) ENCODE az64
	,USAGE_RATE_OR_AMOUNT NUMERIC(28,10) ENCODE az64
	,ACTIVITY_ID NUMERIC(15,0) ENCODE az64 
	,SCHEDULED_FLAG NUMERIC(15,0) ENCODE az64
	,ASSIGNED_UNITS NUMERIC(28,10) ENCODE az64 
	,AUTOCHARGE_TYPE NUMERIC(15,0) ENCODE az64
	,STANDARD_RATE_FLAG NUMERIC(15,0) ENCODE az64
	,APPLIED_RESOURCE_UNITS NUMERIC(28,10) ENCODE az64 
	,APPLIED_RESOURCE_VALUE NUMERIC(28,10) ENCODE az64 
	,START_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,COMPLETION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,ATTRIBUTE_CATEGORY VARCHAR(30) ENCODE lzo 
	,ATTRIBUTE1 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE2 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE3 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE4 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE5 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE6 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE7 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE8 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE9 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE10 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE11 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE12 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE13 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE14 VARCHAR(150) ENCODE lzo
	,ATTRIBUTE15 VARCHAR(150) ENCODE lzo
	,RELIEVED_RES_COMPLETION_UNITS NUMERIC(28,10) ENCODE az64 
	,RELIEVED_RES_SCRAP_UNITS NUMERIC(28,10) ENCODE az64 
	,RELIEVED_RES_COMPLETION_VALUE NUMERIC(28,10) ENCODE az64 
	,RELIEVED_RES_SCRAP_VALUE NUMERIC(28,10) ENCODE az64 
	,RELIEVED_VARIANCE_VALUE NUMERIC(28,10) ENCODE az64 
	,TEMP_RELIEVED_VALUE NUMERIC(28,10) ENCODE az64 
	,RELIEVED_RES_FINAL_COMP_UNITS NUMERIC(28,10) ENCODE az64 
	,DEPARTMENT_ID NUMERIC(15,0) ENCODE az64 
	,PHANTOM_FLAG NUMERIC(15,0) ENCODE az64 
	,PHANTOM_OP_SEQ_NUM NUMERIC(15,0) ENCODE az64
	,PHANTOM_ITEM_ID NUMERIC(15,0) ENCODE az64 
	,SCHEDULE_SEQ_NUM NUMERIC(28,10) ENCODE az64 
	,SUBSTITUTE_GROUP_NUM NUMERIC(15,0) ENCODE az64 
	,REPLACEMENT_GROUP_NUM NUMERIC(15,0) ENCODE az64 
	,PRINCIPLE_FLAG NUMERIC(15,0) ENCODE az64 
	,SETUP_ID NUMERIC(15,0) ENCODE az64 
	,PARENT_RESOURCE_SEQ NUMERIC(15,0) ENCODE az64 
	,BATCH_ID NUMERIC(15,0) ENCODE az64 
	,RELIEVED_RES_UNITS NUMERIC(28,10) ENCODE az64 
	,RELIEVED_RES_VALUE NUMERIC(28,10) ENCODE az64 
	,MAXIMUM_ASSIGNED_UNITS NUMERIC(28,10) ENCODE az64 
	,FIRM_FLAG NUMERIC(15,0) ENCODE az64 
	,GROUP_SEQUENCE_ID NUMERIC(15,0) ENCODE az64 
	,GROUP_SEQUENCE_NUMBER NUMERIC(28,10) ENCODE az64 
	,ACTUAL_START_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,ACTUAL_COMPLETION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,PROJECTED_COMPLETION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo 
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WIP_OPERATION_RESOURCES (
	
	 WIP_ENTITY_ID 
	,OPERATION_SEQ_NUM 
	,RESOURCE_SEQ_NUM 
	,ORGANIZATION_ID 
	,REPETITIVE_SCHEDULE_ID 
	,LAST_UPDATE_DATE   
	,LAST_UPDATED_BY 
	,CREATION_DATE   
	,CREATED_BY 
	,LAST_UPDATE_LOGIN 
	,REQUEST_ID 
	,PROGRAM_APPLICATION_ID  
	,PROGRAM_ID  
	,PROGRAM_UPDATE_DATE 
	,RESOURCE_ID 
	,UOM_CODE 
	,BASIS_TYPE 
	,USAGE_RATE_OR_AMOUNT 
	,ACTIVITY_ID  
	,SCHEDULED_FLAG 
	,ASSIGNED_UNITS  
	,AUTOCHARGE_TYPE 
	,STANDARD_RATE_FLAG 
	,APPLIED_RESOURCE_UNITS  
	,APPLIED_RESOURCE_VALUE  
	,START_DATE    
	,COMPLETION_DATE  
	,ATTRIBUTE_CATEGORY 
	,ATTRIBUTE1 
	,ATTRIBUTE2 
	,ATTRIBUTE3 
	,ATTRIBUTE4 
	,ATTRIBUTE5 
	,ATTRIBUTE6 
	,ATTRIBUTE7 
	,ATTRIBUTE8 
	,ATTRIBUTE9 
	,ATTRIBUTE10 
	,ATTRIBUTE11 
	,ATTRIBUTE12 
	,ATTRIBUTE13 
	,ATTRIBUTE14 
	,ATTRIBUTE15 
	,RELIEVED_RES_COMPLETION_UNITS  
	,RELIEVED_RES_SCRAP_UNITS  
	,RELIEVED_RES_COMPLETION_VALUE  
	,RELIEVED_RES_SCRAP_VALUE  
	,RELIEVED_VARIANCE_VALUE  
	,TEMP_RELIEVED_VALUE  
	,RELIEVED_RES_FINAL_COMP_UNITS  
	,DEPARTMENT_ID  
	,PHANTOM_FLAG  
	,PHANTOM_OP_SEQ_NUM  
	,PHANTOM_ITEM_ID  
	,SCHEDULE_SEQ_NUM  
	,SUBSTITUTE_GROUP_NUM  
	,REPLACEMENT_GROUP_NUM  
	,PRINCIPLE_FLAG  
	,SETUP_ID  
	,PARENT_RESOURCE_SEQ  
	,BATCH_ID  
	,RELIEVED_RES_UNITS  
	,RELIEVED_RES_VALUE  
	,MAXIMUM_ASSIGNED_UNITS  
	,FIRM_FLAG  
	,GROUP_SEQUENCE_ID  
	,GROUP_SEQUENCE_NUMBER  
	,ACTUAL_START_DATE 
	,ACTUAL_COMPLETION_DATE 
	,PROJECTED_COMPLETION_DATE 
	,KCA_OPERATION
	,IS_DELETED_FLG
	,kca_seq_id
	,kca_seq_date
)
    SELECT
     WIP_ENTITY_ID 
	,OPERATION_SEQ_NUM 
	,RESOURCE_SEQ_NUM 
	,ORGANIZATION_ID 
	,REPETITIVE_SCHEDULE_ID 
	,LAST_UPDATE_DATE   
	,LAST_UPDATED_BY 
	,CREATION_DATE   
	,CREATED_BY 
	,LAST_UPDATE_LOGIN 
	,REQUEST_ID 
	,PROGRAM_APPLICATION_ID  
	,PROGRAM_ID  
	,PROGRAM_UPDATE_DATE  
	,RESOURCE_ID 
	,UOM_CODE 
	,BASIS_TYPE 
	,USAGE_RATE_OR_AMOUNT 
	,ACTIVITY_ID  
	,SCHEDULED_FLAG 
	,ASSIGNED_UNITS  
	,AUTOCHARGE_TYPE 
	,STANDARD_RATE_FLAG 
	,APPLIED_RESOURCE_UNITS  
	,APPLIED_RESOURCE_VALUE  
	,START_DATE    
	,COMPLETION_DATE  
	,ATTRIBUTE_CATEGORY 
	,ATTRIBUTE1 
	,ATTRIBUTE2 
	,ATTRIBUTE3 
	,ATTRIBUTE4 
	,ATTRIBUTE5 
	,ATTRIBUTE6 
	,ATTRIBUTE7 
	,ATTRIBUTE8 
	,ATTRIBUTE9 
	,ATTRIBUTE10 
	,ATTRIBUTE11 
	,ATTRIBUTE12 
	,ATTRIBUTE13 
	,ATTRIBUTE14 
	,ATTRIBUTE15 
	,RELIEVED_RES_COMPLETION_UNITS  
	,RELIEVED_RES_SCRAP_UNITS  
	,RELIEVED_RES_COMPLETION_VALUE  
	,RELIEVED_RES_SCRAP_VALUE  
	,RELIEVED_VARIANCE_VALUE  
	,TEMP_RELIEVED_VALUE  
	,RELIEVED_RES_FINAL_COMP_UNITS  
	,DEPARTMENT_ID  
	,PHANTOM_FLAG  
	,PHANTOM_OP_SEQ_NUM  
	,PHANTOM_ITEM_ID  
	,SCHEDULE_SEQ_NUM  
	,SUBSTITUTE_GROUP_NUM  
	,REPLACEMENT_GROUP_NUM  
	,PRINCIPLE_FLAG  
	,SETUP_ID  
	,PARENT_RESOURCE_SEQ  
	,BATCH_ID  
	,RELIEVED_RES_UNITS  
	,RELIEVED_RES_VALUE  
	,MAXIMUM_ASSIGNED_UNITS  
	,FIRM_FLAG  
	,GROUP_SEQUENCE_ID  
	,GROUP_SEQUENCE_NUMBER  
	,ACTUAL_START_DATE 
	,ACTUAL_COMPLETION_DATE 
	,PROJECTED_COMPLETION_DATE 
		,KCA_OPERATION
		,'N' as IS_DELETED_FLG
		,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.WIP_OPERATION_RESOURCES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wip_operation_resources';
	
commit;