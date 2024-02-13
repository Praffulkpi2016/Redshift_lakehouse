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
	bec_ods.MTL_ONHAND_QUANTITIES
where
	(nvl(ONHAND_QUANTITIES_ID, 0)  
	) in 
	(
	select		 
		nvl(stg.ONHAND_QUANTITIES_ID, 0) as ONHAND_QUANTITIES_ID
	from
		bec_ods.MTL_ONHAND_QUANTITIES ods,
		bec_ods_stg.MTL_ONHAND_QUANTITIES stg
	where
		NVL(ods.ONHAND_QUANTITIES_ID, 0) = NVL(stg.ONHAND_QUANTITIES_ID, 0)
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_ONHAND_QUANTITIES
    (inventory_item_id,
	organization_id,
	date_received,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	primary_transaction_quantity,
	subinventory_code,
	revision,
	locator_id,
	create_transaction_id,
	update_transaction_id,
	lot_number,
	orig_date_received,
	cost_group_id,
	containerized_flag,
	project_id,
	task_id,
	onhand_quantities_id,
	organization_type,
	owning_organization_id,
	owning_tp_type,
	planning_organization_id,
	planning_tp_type,
	transaction_uom_code,
	transaction_quantity,
	secondary_uom_code,
	secondary_transaction_quantity,
	is_consigned,
	lpn_id,
	status_id,
	mcc_code,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
		inventory_item_id,
		organization_id,
		date_received,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		primary_transaction_quantity,
		subinventory_code,
		revision,
		locator_id,
		create_transaction_id,
		update_transaction_id,
		lot_number,
		orig_date_received,
		cost_group_id,
		containerized_flag,
		project_id,
		task_id,
		onhand_quantities_id,
		organization_type,
		owning_organization_id,
		owning_tp_type,
		planning_organization_id,
		planning_tp_type,
		transaction_uom_code,
		transaction_quantity,
		secondary_uom_code,
		secondary_transaction_quantity,
		is_consigned,
		lpn_id,
		status_id,
		mcc_code,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_ONHAND_QUANTITIES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(ONHAND_QUANTITIES_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(ONHAND_QUANTITIES_ID, 0) as ONHAND_QUANTITIES_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MTL_ONHAND_QUANTITIES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(ONHAND_QUANTITIES_ID, 0)  
			)	
	);

commit;
 
-- Soft delete
update bec_ods.MTL_ONHAND_QUANTITIES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_ONHAND_QUANTITIES set IS_DELETED_FLG = 'Y'
where (ONHAND_QUANTITIES_ID )  in
(
select ONHAND_QUANTITIES_ID  from bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
where (ONHAND_QUANTITIES_ID ,KCA_SEQ_ID)
in 
(
select ONHAND_QUANTITIES_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
group by ONHAND_QUANTITIES_ID 
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
	ods_table_name = 'mtl_onhand_quantities';

commit;