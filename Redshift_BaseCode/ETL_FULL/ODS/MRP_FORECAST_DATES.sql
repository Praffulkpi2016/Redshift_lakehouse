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
	
	DROP TABLE if exists bec_ods.MRP_FORECAST_DATES;
	
	CREATE TABLE IF NOT EXISTS bec_ods.MRP_FORECAST_DATES
	(
		
		transaction_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,forecast_designator VARCHAR(10)   ENCODE lzo
	,forecast_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_forecast_quantity NUMERIC(28,10)   ENCODE az64
	,current_forecast_quantity NUMERIC(28,10)   ENCODE az64
	,confidence_percentage NUMERIC(28,10)   ENCODE az64
	,bucket_type NUMERIC(15,0)   ENCODE az64
	,rate_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,origination_type NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,ship_id NUMERIC(15,0)   ENCODE az64
	,bill_id NUMERIC(15,0)   ENCODE az64
	,comments VARCHAR(240)   ENCODE lzo
	,source_organization_id NUMERIC(15,0)   ENCODE az64
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
	,program_application_id NUMERIC(38,10)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,old_transaction_id NUMERIC(15,0)   ENCODE az64
	,to_update NUMERIC(15,0)   ENCODE az64
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
	,task_id NUMERIC(15,0)   ENCODE az64
	,line_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo,
		IS_DELETED_FLG VARCHAR(2) ENCODE lzo, 
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE
	auto;
	
	INSERT INTO bec_ods.MRP_FORECAST_DATES (
		
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
		IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
	)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
	bec_ods_stg.MRP_FORECAST_DATES;
	
	end;		
	
	UPDATE bec_etl_ctrl.batch_ods_info
	SET
	load_type = 'I',
	last_refresh_date = getdate()
	WHERE
	ods_table_name = 'mrp_forecast_dates';
	
	commit;		