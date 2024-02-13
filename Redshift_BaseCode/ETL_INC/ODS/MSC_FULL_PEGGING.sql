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

truncate table bec_ods.MSC_FULL_PEGGING;

INSERT INTO bec_ods.MSC_FULL_PEGGING (
		pegging_id,
	demand_quantity,
	supply_quantity,
	allocated_quantity,
	end_item_usage,
	demand_date,
	supply_date,
	supply_type,
	end_origination_type,
	inventory_item_id,
	organization_id,
	plan_id,
	prev_pegging_id,
	end_pegging_id,
	transaction_id,
	disposition_id,
	demand_id,
	project_id,
	task_id,
	sr_instance_id,
	demand_class,
	updated,
	status,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	unit_number,
	dest_transaction_id,
	reserved_qty,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
pegging_id,
	demand_quantity,
	supply_quantity,
	allocated_quantity,
	end_item_usage,
	demand_date,
	supply_date,
	supply_type,
	end_origination_type,
	inventory_item_id,
	organization_id,
	plan_id,
	prev_pegging_id,
	end_pegging_id,
	transaction_id,
	disposition_id,
	demand_id,
	project_id,
	task_id,
	sr_instance_id,
	demand_class,
	updated,
	status,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	unit_number,
	dest_transaction_id,
	reserved_qty,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.MSC_FULL_PEGGING;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_full_pegging';
	
Commit;