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

DROP TABLE if exists bec_ods.MTL_PHYSICAL_ADJUSTMENTS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_PHYSICAL_ADJUSTMENTS
(
    adjustment_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,physical_inventory_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,subinventory_name VARCHAR(10)   ENCODE lzo
	,system_quantity NUMERIC(28,10)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,count_quantity NUMERIC(28,10)   ENCODE az64
	,adjustment_quantity NUMERIC(28,10)   ENCODE az64
	,revision VARCHAR(3)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,lot_number VARCHAR(80)   ENCODE lzo
	,lot_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,serial_number VARCHAR(30)   ENCODE lzo
	,actual_cost NUMERIC(28,10)   ENCODE az64
	,approval_status NUMERIC(22,0)   ENCODE az64
	,approved_by_employee_id NUMERIC(9,0)   ENCODE az64
	,automatic_approval_code NUMERIC(15,0)   ENCODE az64
	,gl_adjust_account NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,lot_serial_controls VARCHAR(1)   ENCODE lzo
	,temp_approver NUMERIC(1,0)   ENCODE az64
	,parent_lpn_id NUMERIC(15,0)   ENCODE az64
	,outermost_lpn_id NUMERIC(15,0)   ENCODE az64
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,secondary_system_qty NUMERIC(28,10)   ENCODE az64
	,secondary_count_qty NUMERIC(28,10)   ENCODE az64
	,secondary_adjustment_qty NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_PHYSICAL_ADJUSTMENTS (
    adjustment_id,
	organization_id,
	physical_inventory_id,
	inventory_item_id,
	subinventory_name,
	system_quantity,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	count_quantity,
	adjustment_quantity,
	revision,
	locator_id,
	lot_number,
	lot_expiration_date,
	serial_number,
	actual_cost,
	approval_status,
	approved_by_employee_id,
	automatic_approval_code,
	gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	lot_serial_controls,
	temp_approver,
	parent_lpn_id,
	outermost_lpn_id,
	cost_group_id,
	secondary_system_qty,
	secondary_count_qty,
	secondary_adjustment_qty,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
    adjustment_id,
	organization_id,
	physical_inventory_id,
	inventory_item_id,
	subinventory_name,
	system_quantity,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	count_quantity,
	adjustment_quantity,
	revision,
	locator_id,
	lot_number,
	lot_expiration_date,
	serial_number,
	actual_cost,
	approval_status,
	approved_by_employee_id,
	automatic_approval_code,
	gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	lot_serial_controls,
	temp_approver,
	parent_lpn_id,
	outermost_lpn_id,
	cost_group_id,
	secondary_system_qty,
	secondary_count_qty,
	secondary_adjustment_qty,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_physical_adjustments';
	
commit;