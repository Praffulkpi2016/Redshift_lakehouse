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

DROP TABLE if exists bec_ods.WIP_OPERATIONS;

CREATE TABLE IF NOT EXISTS bec_ods.WIP_OPERATIONS
(

     wip_entity_id NUMERIC(15,0)   ENCODE az64
	,operation_seq_num NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,repetitive_schedule_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,operation_sequence_id NUMERIC(15,0)   ENCODE az64
	,standard_operation_id NUMERIC(15,0)   ENCODE az64
	,department_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,scheduled_quantity NUMERIC(28,10)   ENCODE az64
	,quantity_in_queue NUMERIC(28,10)   ENCODE az64
	,quantity_running NUMERIC(28,10)   ENCODE az64
	,quantity_waiting_to_move NUMERIC(28,10)   ENCODE az64
	,quantity_rejected NUMERIC(28,10)   ENCODE az64
	,quantity_scrapped NUMERIC(28,10)   ENCODE az64
	,quantity_completed NUMERIC(28,10)   ENCODE az64
	,first_unit_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,first_unit_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_unit_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_unit_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,previous_operation_seq_num NUMERIC(15,0)   ENCODE az64
	,next_operation_seq_num NUMERIC(15,0)   ENCODE az64
	,count_point_type NUMERIC(15,0)   ENCODE az64
	,backflush_flag NUMERIC(15,0)   ENCODE az64
	,minimum_transfer_quantity NUMERIC(28,10)   ENCODE az64
	,date_last_moved TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,wf_itemtype VARCHAR(150)   ENCODE lzo
	,wf_itemkey VARCHAR(150)   ENCODE lzo
	,operation_yield NUMERIC(28,10)   ENCODE az64
	,operation_yield_enabled NUMERIC(28,10)   ENCODE az64
	,pre_split_quantity NUMERIC(28,10)   ENCODE az64
	,operation_completed VARCHAR(1)   ENCODE lzo
	,shutdown_type VARCHAR(30)   ENCODE lzo
	,x_pos NUMERIC(28,10)   ENCODE az64
	,y_pos NUMERIC(28,10)   ENCODE az64
	,previous_operation_seq_id NUMERIC(15,0)   ENCODE az64
	,skip_flag NUMERIC(15,0)   ENCODE az64
	,long_description VARCHAR(4000)   ENCODE lzo
	,cumulative_scrap_quantity NUMERIC(28,10)   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,recommended VARCHAR(1)   ENCODE lzo
	,progress_percentage NUMERIC(28,10)   ENCODE az64
	,wsm_op_seq_num NUMERIC(28,10)   ENCODE az64
	,wsm_bonus_quantity NUMERIC(28,10)   ENCODE az64
	,employee_id NUMERIC(15,0)   ENCODE az64
	,actual_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,projected_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wsm_update_quantity_txn_id NUMERIC(15,0)   ENCODE az64
	,wsm_costed_quantity_completed NUMERIC(28,10)   ENCODE az64
	,lowest_acceptable_yield NUMERIC(28,10)   ENCODE az64
	,check_skill NUMERIC(15,0)   ENCODE az64  
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.WIP_OPERATIONS (
   wip_entity_id,
	operation_seq_num,
	organization_id,
	repetitive_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	operation_sequence_id,
	standard_operation_id,
	department_id,
	description,
	scheduled_quantity,
	quantity_in_queue,
	quantity_running,
	quantity_waiting_to_move,
	quantity_rejected,
	quantity_scrapped,
	quantity_completed,
	first_unit_start_date,
	first_unit_completion_date,
	last_unit_start_date,
	last_unit_completion_date,
	previous_operation_seq_num,
	next_operation_seq_num,
	count_point_type,
	backflush_flag,
	minimum_transfer_quantity,
	date_last_moved,
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
	wf_itemtype,
	wf_itemkey,
	operation_yield,
	operation_yield_enabled,
	pre_split_quantity,
	operation_completed,
	shutdown_type,
	x_pos,
	y_pos,
	previous_operation_seq_id,
	skip_flag,
	long_description,
	cumulative_scrap_quantity,
	disable_date,
	recommended,
	progress_percentage,
	wsm_op_seq_num,
	wsm_bonus_quantity,
	employee_id,
	actual_start_date,
	actual_completion_date,
	projected_completion_date,
	wsm_update_quantity_txn_id,
	wsm_costed_quantity_completed,
	lowest_acceptable_yield,
	check_skill,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    select
	wip_entity_id,
	operation_seq_num,
	organization_id,
	repetitive_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	operation_sequence_id,
	standard_operation_id,
	department_id,
	description,
	scheduled_quantity,
	quantity_in_queue,
	quantity_running,
	quantity_waiting_to_move,
	quantity_rejected,
	quantity_scrapped,
	quantity_completed,
	first_unit_start_date,
	first_unit_completion_date,
	last_unit_start_date,
	last_unit_completion_date,
	previous_operation_seq_num,
	next_operation_seq_num,
	count_point_type,
	backflush_flag,
	minimum_transfer_quantity,
	date_last_moved,
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
	wf_itemtype,
	wf_itemkey,
	operation_yield,
	operation_yield_enabled,
	pre_split_quantity,
	operation_completed,
	shutdown_type,
	x_pos,
	y_pos,
	previous_operation_seq_id,
	skip_flag,
	long_description,
	cumulative_scrap_quantity,
	disable_date,
	recommended,
	progress_percentage,
	wsm_op_seq_num,
	wsm_bonus_quantity,
	employee_id,
	actual_start_date,
	actual_completion_date,
	projected_completion_date,
	wsm_update_quantity_txn_id,
	wsm_costed_quantity_completed,
	lowest_acceptable_yield,
	check_skill,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.WIP_OPERATIONS;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'wip_operations';

commit;