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
    bec_ods.MTL_PHYSICAL_INVENTORIES
where
    (
    NVL(ORGANIZATION_ID, 0),
    NVL(PHYSICAL_INVENTORY_ID, 0)

    ) in 
    (
    select
        NVL(stg.ORGANIZATION_ID, 0) as ORGANIZATION_ID,
        NVL(stg.PHYSICAL_INVENTORY_ID, 0) as PHYSICAL_INVENTORY_ID
    from
        bec_ods.MTL_PHYSICAL_INVENTORIES ods,
        bec_ods_stg.MTL_PHYSICAL_INVENTORIES stg
    where
        NVL(ods.PHYSICAL_INVENTORY_ID, 0) = NVL(stg.PHYSICAL_INVENTORY_ID, 0)
            and NVL(ods.ORGANIZATION_ID, 0) = NVL(stg.ORGANIZATION_ID, 0)
                and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records
insert
	into
	bec_ods.MTL_PHYSICAL_INVENTORIES
    (physical_inventory_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	physical_inventory_date,
	last_adjustment_date,
	total_adjustment_value,
	description,
	freeze_date,
	physical_inventory_name,
	approval_required,
	all_subinventories_flag,
	next_tag_number,
	tag_number_increments,
	default_gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	approval_tolerance_pos,
	approval_tolerance_neg,
	cost_variance_pos,
	cost_variance_neg,
	number_of_skus,
	dynamic_tag_entry_flag,
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
	attribute_category,
	exclude_zero_balance,
	exclude_negative_balance,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
		physical_inventory_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	physical_inventory_date,
	last_adjustment_date,
	total_adjustment_value,
	description,
	freeze_date,
	physical_inventory_name,
	approval_required,
	all_subinventories_flag,
	next_tag_number,
	tag_number_increments,
	default_gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	approval_tolerance_pos,
	approval_tolerance_neg,
	cost_variance_pos,
	cost_variance_neg,
	number_of_skus,
	dynamic_tag_entry_flag,
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
	attribute_category,
	exclude_zero_balance,
	exclude_negative_balance,
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_PHYSICAL_INVENTORIES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(PHYSICAL_INVENTORY_ID,0),
		NVL(ORGANIZATION_ID,0),
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(PHYSICAL_INVENTORY_ID,0) AS PHYSICAL_INVENTORY_ID,
	        NVL(ORGANIZATION_ID,0) AS ORGANIZATION_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MTL_PHYSICAL_INVENTORIES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(PHYSICAL_INVENTORY_ID,0),
			NVL(ORGANIZATION_ID,0)
			)	
	);
commit;
 
-- Soft delete
update bec_ods.MTL_PHYSICAL_INVENTORIES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_PHYSICAL_INVENTORIES set IS_DELETED_FLG = 'Y'
where (NVL(PHYSICAL_INVENTORY_ID, 0),NVL(ORGANIZATION_ID, 0) )  in
(
select NVL(PHYSICAL_INVENTORY_ID, 0),NVL(ORGANIZATION_ID, 0)  from bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES
where (NVL(PHYSICAL_INVENTORY_ID, 0),NVL(ORGANIZATION_ID, 0) ,KCA_SEQ_ID)
in 
(
select NVL(PHYSICAL_INVENTORY_ID, 0),NVL(ORGANIZATION_ID, 0) ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES
group by NVL(PHYSICAL_INVENTORY_ID, 0),NVL(ORGANIZATION_ID, 0) 
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
	ods_table_name = 'mtl_physical_inventories';

commit;