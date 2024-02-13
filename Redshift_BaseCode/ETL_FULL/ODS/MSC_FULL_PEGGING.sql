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

DROP TABLE if exists bec_ods.MSC_FULL_PEGGING;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_FULL_PEGGING
(
	pegging_id NUMERIC(15,0)   ENCODE az64
	,demand_quantity NUMERIC(28,10)   ENCODE az64
	,supply_quantity NUMERIC(28,10)   ENCODE az64
	,allocated_quantity NUMERIC(28,10)   ENCODE az64
	,end_item_usage NUMERIC(28,10)   ENCODE az64
	,demand_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,supply_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,supply_type NUMERIC(15,0)   ENCODE az64
	,end_origination_type NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,plan_id NUMERIC(15,0)   ENCODE az64
	,prev_pegging_id NUMERIC(15,0)   ENCODE az64
	,end_pegging_id NUMERIC(15,0)   ENCODE az64
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,disposition_id NUMERIC(15,0)   ENCODE az64
	,demand_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,demand_class VARCHAR(34)   ENCODE lzo
	,updated NUMERIC(15,0)   ENCODE az64
	,status NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,unit_number VARCHAR(30)   ENCODE lzo
	,dest_transaction_id NUMERIC(15,0)   ENCODE az64
	,reserved_qty NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

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