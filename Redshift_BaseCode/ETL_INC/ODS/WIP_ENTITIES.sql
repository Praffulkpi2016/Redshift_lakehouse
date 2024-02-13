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

delete from bec_ods.WIP_ENTITIES
where (WIP_ENTITY_ID) in (
select stg.WIP_ENTITY_ID
from bec_ods.WIP_ENTITIES ods, bec_ods_stg.WIP_ENTITIES stg
where ods.WIP_ENTITY_ID = stg.WIP_ENTITY_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.WIP_ENTITIES
       (  wip_entity_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wip_entity_name,
	entity_type,
	description,
	primary_item_id,
	gen_object_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
		  wip_entity_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wip_entity_name,
	entity_type,
	description,
	primary_item_id,
	gen_object_id,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.WIP_ENTITIES
	where kca_operation in ('INSERT','UPDATE') 
	and (WIP_ENTITY_ID,kca_seq_id) in 
	(select WIP_ENTITY_ID,max(kca_seq_id) from bec_ods_stg.WIP_ENTITIES 
     where kca_operation in ('INSERT','UPDATE')
     group by WIP_ENTITY_ID)
);

commit;



-- Soft delete
update bec_ods.WIP_ENTITIES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WIP_ENTITIES set IS_DELETED_FLG = 'Y'
where (WIP_ENTITY_ID)  in
(
select WIP_ENTITY_ID from bec_raw_dl_ext.WIP_ENTITIES
where (WIP_ENTITY_ID,KCA_SEQ_ID)
in 
(
select WIP_ENTITY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WIP_ENTITIES
group by WIP_ENTITY_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wip_entities';

commit;