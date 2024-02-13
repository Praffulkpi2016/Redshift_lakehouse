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

TRUNCATE TABLE bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS;

insert into	bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS
    (adjustment_id,
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
	KCA_SEQ_ID,
	kca_seq_date)
(select
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
	KCA_SEQ_ID,
	kca_seq_date
from
	bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (NVL(ADJUSTMENT_ID,0),KCA_SEQ_ID) in 
	(select NVL(ADJUSTMENT_ID,0) AS ADJUSTMENT_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by NVL(ADJUSTMENT_ID,0))
     and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_physical_adjustments')
	 
            )
	 );
END;