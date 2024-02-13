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

delete from bec_ods.MTL_PHYSICAL_ADJUSTMENTS
where (NVL(ADJUSTMENT_ID,0)) in (
select NVL(stg.ADJUSTMENT_ID,0) as ADJUSTMENT_ID
 
from bec_ods.MTL_PHYSICAL_ADJUSTMENTS ods, bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS stg
where NVL(ods.ADJUSTMENT_ID,0) = NVL(stg.ADJUSTMENT_ID,0) AND  
	   
 stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_PHYSICAL_ADJUSTMENTS
    (
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
	IS_DELETED_FLG,
	kca_seq_id,
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
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS
where kca_operation IN ('INSERT','UPDATE') 
	and (NVL(ADJUSTMENT_ID,0),KCA_SEQ_ID) in 
	(select NVL(ADJUSTMENT_ID,0) as ADJUSTMENT_ID,max(KCA_SEQ_ID) from bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by NVL(ADJUSTMENT_ID,0))	
	);

commit;

 

-- Soft delete
update bec_ods.MTL_PHYSICAL_ADJUSTMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_PHYSICAL_ADJUSTMENTS set IS_DELETED_FLG = 'Y'
where (ADJUSTMENT_ID )  in
(
select ADJUSTMENT_ID  from bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
where (ADJUSTMENT_ID ,KCA_SEQ_ID)
in 
(
select ADJUSTMENT_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
group by ADJUSTMENT_ID 
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
	ods_table_name = 'mtl_physical_adjustments';

commit;