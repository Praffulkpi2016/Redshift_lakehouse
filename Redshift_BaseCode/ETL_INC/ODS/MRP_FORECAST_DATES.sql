/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents Incremental load approach for ODS.
	# File Version: KPI v1.0
*/

begin;
	-- Delete Records
	
	delete
	from
	bec_ods.MRP_FORECAST_DATES
	where
	(
		nvl(TRANSACTION_ID, 0)  
		
	) in 
	(
		select
		NVL(stg.TRANSACTION_ID,0) AS TRANSACTION_ID 
		from
		bec_ods.MRP_FORECAST_DATES ods,
		bec_ods_stg.MRP_FORECAST_DATES stg
		where
		NVL(ods.TRANSACTION_ID,0) = NVL(stg.TRANSACTION_ID,0)   
		and stg.kca_operation in ('INSERT', 'UPDATE')
	);
	
	commit;
	-- Insert records
	
	insert
	into bec_ods.MRP_FORECAST_DATES  (
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
		kca_operation,
		IS_DELETED_FLG,
		kca_seq_id,
	kca_seq_date)
	(
		select
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
		from
		bec_ods_stg.MRP_FORECAST_DATES
		where
		kca_operation IN ('INSERT','UPDATE')
		and (
			nvl(TRANSACTION_ID, 0) ,
			KCA_SEQ_ID
		) in 
		(
			select
			nvl(TRANSACTION_ID, 0) as TRANSACTION_ID , 
			max(KCA_SEQ_ID)
			from
			bec_ods_stg.MRP_FORECAST_DATES
			where
			kca_operation IN ('INSERT','UPDATE')
			group by
			nvl(TRANSACTION_ID, 0)   
		)	
	);
	
	commit;
	
	-- Soft delete
	
update bec_ods.MRP_FORECAST_DATES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MRP_FORECAST_DATES set IS_DELETED_FLG = 'Y'
where (TRANSACTION_ID)  in
(
select TRANSACTION_ID from bec_raw_dl_ext.MRP_FORECAST_DATES
where (TRANSACTION_ID,KCA_SEQ_ID)
in 
(
select TRANSACTION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MRP_FORECAST_DATES
group by TRANSACTION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;


update
bec_etl_ctrl.batch_ods_info
set
last_refresh_date = getdate()
where
ods_table_name = 'mrp_forecast_dates';

commit;	