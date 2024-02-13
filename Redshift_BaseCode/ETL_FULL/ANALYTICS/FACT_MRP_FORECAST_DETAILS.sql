 /*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_MRP_FORECAST_DETAILS;

create table bec_dwh.FACT_MRP_FORECAST_DETAILS diststyle all sortkey (TRANSACTION_ID)
as 
(SELECT
	TRANSACTION_ID ,
	LAST_UPDATE_DATE ,
	LAST_UPDATED_BY ,
	CREATION_DATE ,
	CREATED_BY ,
	LAST_UPDATE_LOGIN ,
	INVENTORY_ITEM_ID ,
	ORGANIZATION_ID ,
	FORECAST_DESIGNATOR ,
	FORECAST_DATE ,
	ORIGINAL_FORECAST_QUANTITY ,
	CURRENT_FORECAST_QUANTITY ,
	CONFIDENCE_PERCENTAGE ,
	BUCKET_TYPE ,
	RATE_END_DATE ,
	ORIGINATION_TYPE ,
	ORIGINATION_TYPE_DESC as MEANING ,
	CUSTOMER_ID ,
	SHIP_ID ,
	BILL_ID ,
	COMMENTS ,
	SOURCE_ORGANIZATION_ID ,
	SOURCE_ORGANIZATION_CODE as ORGANIZATION_CODE ,
	SOURCE_FORECAST_DESIGNATOR ,
	SOURCE_CODE ,
	SOURCE_LINE_ID ,
	END_ITEM_ID ,
	END_PLANNING_BOM_PERCENT ,
	FORECAST_RULE_ID ,
	DEMAND_USAGE_START_DATE ,
	FORECAST_TREND ,
	FOCUS_TYPE ,
	FORECAST_MAD ,
	DEMAND_CLASS ,
	REQUEST_ID ,
	PROGRAM_APPLICATION_ID ,
	PROGRAM_ID ,
	PROGRAM_UPDATE_DATE ,
	DDF_CONTEXT ,
	ATTRIBUTE_CATEGORY ,
	ATTRIBUTE1 ,
	ATTRIBUTE2 ,
	ATTRIBUTE3 ,
	ATTRIBUTE4 ,
	ATTRIBUTE5 ,
	ATTRIBUTE6 ,
	ATTRIBUTE7 ,
	ATTRIBUTE8 ,
	ATTRIBUTE9 ,
	ATTRIBUTE10 ,
	ATTRIBUTE11 ,
	ATTRIBUTE12 ,
	ATTRIBUTE13 ,
	ATTRIBUTE14 ,
	ATTRIBUTE15 ,
	PROJECT_ID ,
	--MRP_GET_PROJECT.PROJECT(PROJECT_ID) ,
	TASK_ID ,
	--MRP_GET_PROJECT.TASK(TASK_ID) ,
	LINE_ID ,
	LINE_CODE,
		(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
       || nvl(transaction_id, 0) || '-' || nvl(line_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
BEC_ODS.MRP_FORECAST_DATES_V
);
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_mrp_forecast_details'
	and batch_name = 'ascp';

commit;
