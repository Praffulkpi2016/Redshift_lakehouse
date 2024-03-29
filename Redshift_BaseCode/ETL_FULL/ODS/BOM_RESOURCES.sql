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

DROP TABLE if exists bec_ods.BOM_RESOURCES;

CREATE TABLE IF NOT EXISTS bec_ods.BOM_RESOURCES
(
	RESOURCE_ID NUMERIC(15,0)   ENCODE az64
	,RESOURCE_CODE VARCHAR(10)	ENCODE lzo
	,ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CREATED_BY NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64 
	,DESCRIPTION VARCHAR(240)	ENCODE lzo 
	,DISABLE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 
	,COST_ELEMENT_ID NUMERIC(15,0)   ENCODE az64
	,PURCHASE_ITEM_ID NUMERIC(15,0)   ENCODE az64 
	,COST_CODE_TYPE NUMERIC(15,0)   ENCODE az64 
	,FUNCTIONAL_CURRENCY_FLAG NUMERIC(15,0)   ENCODE az64 
	,UNIT_OF_MEASURE VARCHAR(3)	ENCODE lzo 
	,DEFAULT_ACTIVITY_ID NUMERIC(15,0)   ENCODE az64  
	,RESOURCE_TYPE NUMERIC(15,0)   ENCODE az64  
	,AUTOCHARGE_TYPE NUMERIC(15,0)   ENCODE az64  
	,STANDARD_RATE_FLAG NUMERIC(15,0)   ENCODE az64  
	,DEFAULT_BASIS_TYPE NUMERIC(15,0)   ENCODE az64  
	,ABSORPTION_ACCOUNT NUMERIC(15,0)   ENCODE az64  
	,ALLOW_COSTS_FLAG NUMERIC(15,0)   ENCODE az64 
	,RATE_VARIANCE_ACCOUNT NUMERIC(15,0)   ENCODE az64  
	,EXPENDITURE_TYPE VARCHAR(30)	ENCODE lzo 
	,ATTRIBUTE_CATEGORY VARCHAR(30)	ENCODE lzo 
	,ATTRIBUTE1 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE2 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE3 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE4 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE5 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE6 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE7 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE8 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE9 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE10 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE11 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE12 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE13 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE14 VARCHAR(150)	ENCODE lzo
	,ATTRIBUTE15 VARCHAR(150)	ENCODE lzo
	,REQUEST_ID NUMERIC(15,0)   ENCODE az64  
	,PROGRAM_APPLICATION_ID NUMERIC(15,0)   ENCODE az64  
	,PROGRAM_ID NUMERIC(15,0)   ENCODE az64  
	,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 
	,BATCHABLE NUMERIC(15,0)   ENCODE az64  
	,MAX_BATCH_CAPACITY NUMERIC(15,0)   ENCODE az64  
	,MIN_BATCH_CAPACITY NUMERIC(15,0)   ENCODE az64  
	,BATCH_CAPACITY_UOM VARCHAR(3)	ENCODE lzo 
	,BATCH_WINDOW NUMERIC(15,0)   ENCODE az64  
	,BATCH_WINDOW_UOM VARCHAR(3)	ENCODE lzo 
	,COMPETENCE_ID NUMERIC(15,0)   ENCODE az64
	,RATING_LEVEL_ID NUMERIC(15,0)   ENCODE az64
	,QUALIFICATION_TYPE_ID NUMERIC(9,0)   ENCODE az64
	,BILLABLE_ITEM_ID NUMERIC(15,0)   ENCODE az64  
	,SUPPLY_SUBINVENTORY VARCHAR(10)	ENCODE lzo
	,SUPPLY_LOCATOR_ID NUMERIC(15,0)   ENCODE az64  
	,BATCHING_PENALTY NUMERIC(15,0)   ENCODE az64  
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.BOM_RESOURCES (
    RESOURCE_ID, 
	RESOURCE_CODE, 
	ORGANIZATION_ID, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	DESCRIPTION, 
	DISABLE_DATE, 
	COST_ELEMENT_ID, 
	PURCHASE_ITEM_ID, 
	COST_CODE_TYPE, 
	FUNCTIONAL_CURRENCY_FLAG, 
	UNIT_OF_MEASURE, 
	DEFAULT_ACTIVITY_ID, 
	RESOURCE_TYPE, 
	AUTOCHARGE_TYPE, 
	STANDARD_RATE_FLAG, 
	DEFAULT_BASIS_TYPE, 
	ABSORPTION_ACCOUNT, 
	ALLOW_COSTS_FLAG, 
	RATE_VARIANCE_ACCOUNT, 
	EXPENDITURE_TYPE, 
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
	BATCHABLE, 
	MAX_BATCH_CAPACITY, 
	MIN_BATCH_CAPACITY, 
	BATCH_CAPACITY_UOM, 
	BATCH_WINDOW, 
	BATCH_WINDOW_UOM, 
	COMPETENCE_ID, 
	RATING_LEVEL_ID, 
	QUALIFICATION_TYPE_ID, 
	BILLABLE_ITEM_ID, 
	SUPPLY_SUBINVENTORY, 
	SUPPLY_LOCATOR_ID, 
	BATCHING_PENALTY, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        RESOURCE_ID, 
		RESOURCE_CODE, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DESCRIPTION, 
		DISABLE_DATE, 
		COST_ELEMENT_ID, 
		PURCHASE_ITEM_ID, 
		COST_CODE_TYPE, 
		FUNCTIONAL_CURRENCY_FLAG, 
		UNIT_OF_MEASURE, 
		DEFAULT_ACTIVITY_ID, 
		RESOURCE_TYPE, 
		AUTOCHARGE_TYPE, 
		STANDARD_RATE_FLAG, 
		DEFAULT_BASIS_TYPE, 
		ABSORPTION_ACCOUNT, 
		ALLOW_COSTS_FLAG, 
		RATE_VARIANCE_ACCOUNT, 
		EXPENDITURE_TYPE, 
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
		BATCHABLE, 
		MAX_BATCH_CAPACITY, 
		MIN_BATCH_CAPACITY, 
		BATCH_CAPACITY_UOM, 
		BATCH_WINDOW, 
		BATCH_WINDOW_UOM, 
		COMPETENCE_ID, 
		RATING_LEVEL_ID, 
		QUALIFICATION_TYPE_ID, 
		BILLABLE_ITEM_ID, 
		SUPPLY_SUBINVENTORY, 
		SUPPLY_LOCATOR_ID, 
		BATCHING_PENALTY, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.BOM_RESOURCES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bom_resources';
	
commit;