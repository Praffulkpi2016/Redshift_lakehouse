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

truncate table bec_ods_stg.WIP_ENTITIES;

insert into	bec_ods_stg.WIP_ENTITIES
   (wip_entity_id,
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
	kca_seq_id
	,KCA_SEQ_DATE)
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
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.WIP_ENTITIES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (WIP_ENTITY_ID,kca_seq_id) in 
	(select WIP_ENTITY_ID,max(kca_seq_id) from bec_raw_dl_ext.WIP_ENTITIES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by WIP_ENTITY_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wip_entities')

            )
);
end;