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
	bec_ods.MRP_SR_ASSIGNMENTS
where
	NVL(ASSIGNMENT_ID, 0) in 
	(
	select
		NVL(stg.ASSIGNMENT_ID, 0) as ASSIGNMENT_ID
	from
		bec_ods.MRP_SR_ASSIGNMENTS ods,
		bec_ods_stg.MRP_SR_ASSIGNMENTS stg
	where
		NVL(ods.ASSIGNMENT_ID, 0) = NVL(stg.ASSIGNMENT_ID, 0)
			and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MRP_SR_ASSIGNMENTS (
	assignment_id
    ,assignment_type
    ,sourcing_rule_id
    ,sourcing_rule_type
    ,assignment_set_id
    ,last_update_date
    ,last_updated_by
    ,creation_date
    ,created_by
    ,last_update_login
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,organization_id
    ,customer_id
    ,ship_to_site_id
    ,category_id
    ,category_set_id
    ,inventory_item_id
    ,secondary_inventory
    ,attribute_category
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	assignment_id
    ,assignment_type
    ,sourcing_rule_id
    ,sourcing_rule_type
    ,assignment_set_id
    ,last_update_date
    ,last_updated_by
    ,creation_date
    ,created_by
    ,last_update_login
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,organization_id
    ,customer_id
    ,ship_to_site_id
    ,category_id
    ,category_set_id
    ,inventory_item_id
    ,secondary_inventory
    ,attribute_category
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.MRP_SR_ASSIGNMENTS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(ASSIGNMENT_ID, 0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(ASSIGNMENT_ID, 0) as ASSIGNMENT_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MRP_SR_ASSIGNMENTS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(ASSIGNMENT_ID, 0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.MRP_SR_ASSIGNMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MRP_SR_ASSIGNMENTS set IS_DELETED_FLG = 'Y'
where (ASSIGNMENT_ID)  in
(
select ASSIGNMENT_ID from bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
where (ASSIGNMENT_ID,KCA_SEQ_ID)
in 
(
select ASSIGNMENT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
group by ASSIGNMENT_ID
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
	ods_table_name = 'mrp_sr_assignments';

commit;