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

DROP TABLE if exists bec_ods.MRP_FORECAST_DATES_V;

CREATE TABLE IF NOT EXISTS bec_ods.MRP_FORECAST_DATES_V
(
	 transaction_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,forecast_designator VARCHAR(16383)   ENCODE lzo
	,forecast_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_forecast_quantity NUMERIC(28,10)   ENCODE az64
	,current_forecast_quantity NUMERIC(28,10)   ENCODE az64
	,confidence_percentage NUMERIC(28,10)   ENCODE az64
	,bucket_type NUMERIC(15,0)   ENCODE az64
	,rate_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,origination_type NUMERIC(15,0)   ENCODE az64
	,origination_type_desc VARCHAR(80)   ENCODE lzo
	,customer_id NUMERIC(15,0)   ENCODE az64
	,ship_id NUMERIC(15,0)   ENCODE az64
	,bill_id NUMERIC(15,0)   ENCODE az64
	,comments VARCHAR(240)   ENCODE lzo
	,source_organization_id NUMERIC(15,0)   ENCODE az64
	,source_organization_code VARCHAR(3)   ENCODE lzo
	,source_forecast_designator VARCHAR(10)   ENCODE lzo
	,source_code VARCHAR(10)   ENCODE lzo
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,end_item_id NUMERIC(15,0)   ENCODE az64
	,end_planning_bom_percent NUMERIC(28,10)   ENCODE az64
	,forecast_rule_id NUMERIC(15,0)   ENCODE az64
	,demand_usage_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,forecast_trend NUMERIC(28,10)   ENCODE az64
	,focus_type NUMERIC(15,0)   ENCODE az64
	,forecast_mad NUMERIC(28,10)   ENCODE az64
	,demand_class VARCHAR(30)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ddf_context VARCHAR(30)   ENCODE lzo
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,project_id NUMERIC(15,0)   ENCODE az64
	,project_number VARCHAR(4000)   ENCODE lzo
	,task_id NUMERIC(15,0)   ENCODE az64
	,task_number VARCHAR(4000)   ENCODE lzo
	,line_id NUMERIC(15,0)   ENCODE az64
	,line_code VARCHAR(10)   ENCODE lzo
	,concatenated_segments VARCHAR(40)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MRP_FORECAST_DATES_V (
        TRANSACTION_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      INVENTORY_ITEM_ID
,      ORGANIZATION_ID
,      FORECAST_DESIGNATOR
,      FORECAST_DATE
,      ORIGINAL_FORECAST_QUANTITY
,      CURRENT_FORECAST_QUANTITY
,      CONFIDENCE_PERCENTAGE
,      BUCKET_TYPE
,      RATE_END_DATE
,      ORIGINATION_TYPE
,      ORIGINATION_TYPE_DESC
,      CUSTOMER_ID
,      SHIP_ID
,      BILL_ID
,      COMMENTS
,      SOURCE_ORGANIZATION_ID
,      SOURCE_ORGANIZATION_CODE
,      SOURCE_FORECAST_DESIGNATOR
,      SOURCE_CODE
,      SOURCE_LINE_ID
,      END_ITEM_ID
,      END_PLANNING_BOM_PERCENT
,      FORECAST_RULE_ID
,      DEMAND_USAGE_START_DATE
,      FORECAST_TREND
,      FOCUS_TYPE
,      FORECAST_MAD
,      DEMAND_CLASS
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      DDF_CONTEXT
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      PROJECT_ID
,      PROJECT_NUMBER
,      TASK_ID
,      TASK_NUMBER
,      LINE_ID
,      LINE_CODE
,      CONCATENATED_SEGMENTS
	,KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
       TRANSACTION_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      INVENTORY_ITEM_ID
,      ORGANIZATION_ID
,      FORECAST_DESIGNATOR
,      FORECAST_DATE
,      ORIGINAL_FORECAST_QUANTITY
,      CURRENT_FORECAST_QUANTITY
,      CONFIDENCE_PERCENTAGE
,      BUCKET_TYPE
,      RATE_END_DATE
,      ORIGINATION_TYPE
,      ORIGINATION_TYPE_DESC
,      CUSTOMER_ID
,      SHIP_ID
,      BILL_ID
,      COMMENTS
,      SOURCE_ORGANIZATION_ID
,      SOURCE_ORGANIZATION_CODE
,      SOURCE_FORECAST_DESIGNATOR
,      SOURCE_CODE
,      SOURCE_LINE_ID
,      END_ITEM_ID
,      END_PLANNING_BOM_PERCENT
,      FORECAST_RULE_ID
,      DEMAND_USAGE_START_DATE
,      FORECAST_TREND
,      FOCUS_TYPE
,      FORECAST_MAD
,      DEMAND_CLASS
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      DDF_CONTEXT
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      PROJECT_ID
,      PROJECT_NUMBER
,      TASK_ID
,      TASK_NUMBER
,      LINE_ID
,      LINE_CODE
,      CONCATENATED_SEGMENTS
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.MRP_FORECAST_DATES_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mrp_forecast_dates_v';
	
commit;