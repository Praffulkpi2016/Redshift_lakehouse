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

delete from bec_ods.MTL_ABC_ASSIGNMENTS
where (NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0)) in (
select NVL(stg.INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,NVL(stg.ASSIGNMENT_GROUP_ID,0) AS ASSIGNMENT_GROUP_ID ,NVL(stg.ABC_CLASS_ID,0) AS ABC_CLASS_ID  from bec_ods.MTL_ABC_ASSIGNMENTS ods, bec_ods_stg.MTL_ABC_ASSIGNMENTS stg
where NVL(ods.INVENTORY_ITEM_ID,0)= NVL(stg.INVENTORY_ITEM_ID,0) and 
	NVL(ods.ASSIGNMENT_GROUP_ID,0)= NVL(stg.ASSIGNMENT_GROUP_ID,0) and 
	NVL(ods.ABC_CLASS_ID,0)= NVL(stg.ABC_CLASS_ID,0) and 
	stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_ABC_ASSIGNMENTS
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MTL_ABC_ASSIGNMENTS
	where kca_operation IN ('INSERT','UPDATE') 
	and (NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0),kca_seq_id) in 
	(select NVL(INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,NVL(ASSIGNMENT_GROUP_ID,0) AS ASSIGNMENT_GROUP_ID ,NVL(ABC_CLASS_ID,0) AS ABC_CLASS_ID ,max(kca_seq_id) from bec_ods_stg.MTL_ABC_ASSIGNMENTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0))
);

commit;

-- Soft delete
update bec_ods.MTL_ABC_ASSIGNMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_ABC_ASSIGNMENTS set IS_DELETED_FLG = 'Y'
where (NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0))  in
(
select NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0) from bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
where (NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0),KCA_SEQ_ID)
in 
(
select NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
group by NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_abc_assignments'; 

commit;