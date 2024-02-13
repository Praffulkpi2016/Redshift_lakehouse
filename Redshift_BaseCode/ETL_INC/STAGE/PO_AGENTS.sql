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

truncate table bec_ods_stg.PO_AGENTS;

insert into	bec_ods_stg.PO_AGENTS
   (
        agent_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	location_id,
	category_id,
	authorization_limit,
	start_date_active,
	end_date_active,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	IS_CONTRACT_OFFICER,
	WARRANT_ID,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
    agent_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	location_id,
	category_id,
	authorization_limit,
	start_date_active,
	end_date_active,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	IS_CONTRACT_OFFICER,
	WARRANT_ID,
	kca_operation,
	kca_seq_id
,KCA_SEQ_DATE	from bec_raw_dl_ext.PO_AGENTS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (AGENT_ID,kca_seq_id) in 
	(select AGENT_ID,max(kca_seq_id) from bec_raw_dl_ext.PO_AGENTS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by AGENT_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_agents')

            )	
);
end;