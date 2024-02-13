/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;

TRUNCATE TABLE bec_ods_stg.MRP_FORECAST_DATES;

insert into	bec_ods_stg.MRP_FORECAST_DATES
    (
	transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_item_id,
	organization_id,
	forecast_designator,
	forecast_date,
	original_forecast_quantity,
	current_forecast_quantity,
	confidence_percentage,
	bucket_type,
	rate_end_date,
	origination_type,
	customer_id,
	ship_id,
	bill_id,
	comments,
	source_organization_id,
	source_forecast_designator,
	source_code,
	source_line_id,
	end_item_id,
	end_planning_bom_percent,
	forecast_rule_id,
	demand_usage_start_date,
	forecast_trend,
	focus_type,
	forecast_mad,
	demand_class,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	old_transaction_id,
	to_update,
	ddf_context,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	project_id,
	task_id,
	line_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(select
	transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_item_id,
	organization_id,
	forecast_designator,
	forecast_date,
	original_forecast_quantity,
	current_forecast_quantity,
	confidence_percentage,
	bucket_type,
	rate_end_date,
	origination_type,
	customer_id,
	ship_id,
	bill_id,
	comments,
	source_organization_id,
	source_forecast_designator,
	source_code,
	source_line_id,
	end_item_id,
	end_planning_bom_percent,
	forecast_rule_id,
	demand_usage_start_date,
	forecast_trend,
	focus_type,
	forecast_mad,
	demand_class,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	old_transaction_id,
	to_update,
	ddf_context,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	project_id,
	task_id,
	line_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.MRP_FORECAST_DATES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(TRANSACTION_ID, 0)  ,KCA_SEQ_ID) in 
	(select nvl(TRANSACTION_ID, 0) as TRANSACTION_ID  ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MRP_FORECAST_DATES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
	group by nvl(TRANSACTION_ID, 0)  )
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mrp_forecast_dates')	
);
END;