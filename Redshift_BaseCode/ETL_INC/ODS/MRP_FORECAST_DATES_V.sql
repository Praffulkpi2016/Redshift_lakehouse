/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;

TRUNCATE TABLE bec_ods.MRP_FORECAST_DATES_V;

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