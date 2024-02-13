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

DROP TABLE if exists bec_ods.MTL_CYCLE_COUNT_HEADERS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_CYCLE_COUNT_HEADERS
(
	cycle_count_header_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,cycle_count_header_name VARCHAR(30)   ENCODE lzo
	,inventory_adjustment_account NUMERIC(15,0)   ENCODE az64
	,orientation_code NUMERIC(15,0)   ENCODE az64
	,abc_assignment_group_id NUMERIC(15,0)   ENCODE az64
	,onhand_visible_flag NUMERIC(15,0)   ENCODE az64
	,days_until_late NUMERIC(15,0)   ENCODE az64
	,autoschedule_enabled_flag NUMERIC(15,0)   ENCODE az64
	,schedule_interval_time NUMERIC(15,0)   ENCODE az64
	,zero_count_flag NUMERIC(15,0)   ENCODE az64
	,header_last_schedule_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,header_next_schedule_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,approval_option_code NUMERIC(15,0)   ENCODE az64
	,automatic_recount_flag NUMERIC(15,0)   ENCODE az64
	,next_user_count_sequence NUMERIC(15,0)   ENCODE az64
	,unscheduled_count_entry NUMERIC(15,0)   ENCODE az64
	,cycle_count_calendar VARCHAR(10)   ENCODE lzo
	,calendar_exception_set NUMERIC(15,0)   ENCODE az64
	,approval_tolerance_positive NUMERIC(15,0)   ENCODE az64
	,approval_tolerance_negative NUMERIC(15,0)   ENCODE az64
	,cost_tolerance_positive NUMERIC(15,0)   ENCODE az64
	,cost_tolerance_negative NUMERIC(15,0)   ENCODE az64
	,hit_miss_tolerance_positive NUMERIC(15,0)   ENCODE az64
	,hit_miss_tolerance_negative NUMERIC(15,0)   ENCODE az64
	,abc_initialization_status NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
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
	,maximum_auto_recounts NUMERIC(15,0)   ENCODE az64
	,serial_count_option NUMERIC(15,0)   ENCODE az64
	,serial_detail_option NUMERIC(15,0)   ENCODE az64
	,serial_adjustment_option NUMERIC(15,0)   ENCODE az64
	,serial_discrepancy_option NUMERIC(15,0)   ENCODE az64
	,container_adjustment_option NUMERIC(15,0)   ENCODE az64
	,container_discrepancy_option NUMERIC(15,0)   ENCODE az64
	,container_enabled_flag NUMERIC(15,0)   ENCODE az64
	,cycle_count_type NUMERIC(15,0)   ENCODE az64
	,schedule_empty_locations NUMERIC(15,0)   ENCODE az64
	,default_num_counts_per_year NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_CYCLE_COUNT_HEADERS (
	cycle_count_header_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	cycle_count_header_name,
	inventory_adjustment_account,
	orientation_code,
	abc_assignment_group_id,
	onhand_visible_flag,
	days_until_late,
	autoschedule_enabled_flag,
	schedule_interval_time,
	zero_count_flag,
	header_last_schedule_date,
	header_next_schedule_date,
	disable_date,
	approval_option_code,
	automatic_recount_flag,
	next_user_count_sequence,
	unscheduled_count_entry,
	cycle_count_calendar,
	calendar_exception_set,
	approval_tolerance_positive,
	approval_tolerance_negative,
	cost_tolerance_positive,
	cost_tolerance_negative,
	hit_miss_tolerance_positive,
	hit_miss_tolerance_negative,
	abc_initialization_status,
	description,
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
	maximum_auto_recounts,
	serial_count_option,
	serial_detail_option,
	serial_adjustment_option,
	serial_discrepancy_option,
	container_adjustment_option,
	container_discrepancy_option,
	container_enabled_flag,
	cycle_count_type,
	schedule_empty_locations,
	default_num_counts_per_year,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
    SELECT
	cycle_count_header_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	cycle_count_header_name,
	inventory_adjustment_account,
	orientation_code,
	abc_assignment_group_id,
	onhand_visible_flag,
	days_until_late,
	autoschedule_enabled_flag,
	schedule_interval_time,
	zero_count_flag,
	header_last_schedule_date,
	header_next_schedule_date,
	disable_date,
	approval_option_code,
	automatic_recount_flag,
	next_user_count_sequence,
	unscheduled_count_entry,
	cycle_count_calendar,
	calendar_exception_set,
	approval_tolerance_positive,
	approval_tolerance_negative,
	cost_tolerance_positive,
	cost_tolerance_negative,
	hit_miss_tolerance_positive,
	hit_miss_tolerance_negative,
	abc_initialization_status,
	description,
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
	maximum_auto_recounts,
	serial_count_option,
	serial_detail_option,
	serial_adjustment_option,
	serial_discrepancy_option,
	container_adjustment_option,
	container_discrepancy_option,
	container_enabled_flag,
	cycle_count_type,
	schedule_empty_locations,
	default_num_counts_per_year,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_CYCLE_COUNT_HEADERS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_cycle_count_headers';
	
commit;