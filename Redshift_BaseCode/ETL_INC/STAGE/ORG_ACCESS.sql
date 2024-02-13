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
	
	truncate table bec_ods_stg.ORG_ACCESS;
	
	insert into	bec_ods_stg.ORG_ACCESS
	(organization_id,
		resp_application_id,
		responsibility_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		disable_date,
		comments,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		KCA_OPERATION,
		kca_seq_id,
	kca_seq_date)
	(
		select
		organization_id,
		resp_application_id,
		responsibility_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		disable_date,
		comments,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
		from bec_raw_dl_ext.ORG_ACCESS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
		and (nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ),kca_seq_id) in 
		(select nvl(RESP_APPLICATION_ID,0) as RESP_APPLICATION_ID,nvl(RESPONSIBILITY_ID, 0 ) as RESPONSIBILITY_ID,nvl(ORGANIZATION_ID, 0 ) AS ORGANIZATION_ID,max(kca_seq_id) from bec_raw_dl_ext.ORG_ACCESS 
			where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ))
		and	kca_seq_date > (
			select
			(executebegints-prune_days)
			from
			bec_etl_ctrl.batch_ods_info
			where
		ods_table_name = 'org_access')
		
	);
	end;	