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
begin;

truncate
	table bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL;

insert
	into
	bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL
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
	KCA_OPERATION,
	KCA_SEQ_ID,
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
		KCA_OPERATION,
		KCA_SEQ_ID,
	kca_seq_date
	from
		bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
		and (
 nvl(ONHAND_QUANTITIES_ID, 0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(ONHAND_QUANTITIES_ID, 0) as ONHAND_QUANTITIES_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
		where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
		group by
			(
nvl(ONHAND_QUANTITIES_ID, 0 )))
		and ( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_onhand_quantities_detail')
			 
            )
			);
end;
