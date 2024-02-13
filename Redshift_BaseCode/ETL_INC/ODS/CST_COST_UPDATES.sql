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
	bec_ods.CST_COST_UPDATES
where
	(
	NVL(COST_UPDATE_ID,0) 

	) in 
	(
	select
		NVL(stg.COST_UPDATE_ID,0) AS COST_UPDATE_ID 
	from
		bec_ods.CST_COST_UPDATES ods,
		bec_ods_stg.CST_COST_UPDATES stg
	where
	NVL(ods.COST_UPDATE_ID,0) = NVL(stg.COST_UPDATE_ID,0) 
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.CST_COST_UPDATES
    (cost_update_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	status,
	organization_id,
	cost_type_id,
	update_date,
	description,
	range_option,
	update_resource_ovhd_flag,
	update_activity_flag,
	snapshot_saved_flag,
	inv_adjustment_account,
	single_item,
	item_range_low,
	item_range_high,
	category_id,
	category_set_id,
	inventory_adjustment_value,
	intransit_adjustment_value,
	wip_adjustment_value,
	scrap_adjustment_value,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	cost_update_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	status,
	organization_id,
	cost_type_id,
	update_date,
	description,
	range_option,
	update_resource_ovhd_flag,
	update_activity_flag,
	snapshot_saved_flag,
	inv_adjustment_account,
	single_item,
	item_range_low,
	item_range_high,
	category_id,
	category_set_id,
	inventory_adjustment_value,
	intransit_adjustment_value,
	wip_adjustment_value,
	scrap_adjustment_value,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.CST_COST_UPDATES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(COST_UPDATE_ID,0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(COST_UPDATE_ID,0) AS COST_UPDATE_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.CST_COST_UPDATES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(COST_UPDATE_ID,0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.CST_COST_UPDATES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_COST_UPDATES set IS_DELETED_FLG = 'Y'
where (COST_UPDATE_ID)  in
(
select COST_UPDATE_ID from bec_raw_dl_ext.CST_COST_UPDATES
where (COST_UPDATE_ID,KCA_SEQ_ID)
in 
(
select COST_UPDATE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_COST_UPDATES
group by COST_UPDATE_ID
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
	ods_table_name = 'cst_cost_updates';

commit;