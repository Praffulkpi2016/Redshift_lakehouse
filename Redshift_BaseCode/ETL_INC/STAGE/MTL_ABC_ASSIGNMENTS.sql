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

truncate table bec_ods_stg.MTL_ABC_ASSIGNMENTS;

insert into	bec_ods_stg.MTL_ABC_ASSIGNMENTS
   (
inventory_item_id,
	assignment_group_id,
	abc_class_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
	inventory_item_id,
	assignment_group_id,
	abc_class_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
     KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0),kca_seq_id) in 
	(select NVL(INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,NVL(ASSIGNMENT_GROUP_ID,0) AS ASSIGNMENT_GROUP_ID ,NVL(ABC_CLASS_ID,0) AS ABC_CLASS_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_abc_assignments')
);
end;