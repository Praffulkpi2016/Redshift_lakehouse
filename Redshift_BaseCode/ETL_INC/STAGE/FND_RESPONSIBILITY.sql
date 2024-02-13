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

truncate table bec_ods_stg.FND_RESPONSIBILITY;

insert into	bec_ods_stg.FND_RESPONSIBILITY
   (	
	APPLICATION_ID, 
	RESPONSIBILITY_ID, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	DATA_GROUP_APPLICATION_ID, 
	DATA_GROUP_ID, 
	MENU_ID, 
	TERM_SECURITY_ENABLED_FLAG, 
	START_DATE, 
	END_DATE, 
	GROUP_APPLICATION_ID, 
	REQUEST_GROUP_ID, 
	VERSION, 
	WEB_HOST_NAME, 
	WEB_AGENT_NAME, 
	RESPONSIBILITY_KEY, 
	ZD_EDITION_NAME, 
	ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date) 
(
	select
		APPLICATION_ID, 
		RESPONSIBILITY_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DATA_GROUP_APPLICATION_ID, 
		DATA_GROUP_ID, 
		MENU_ID, 
		TERM_SECURITY_ENABLED_FLAG, 
		START_DATE, 
		END_DATE, 
		GROUP_APPLICATION_ID, 
		REQUEST_GROUP_ID, 
		VERSION, 
		WEB_HOST_NAME, 
		WEB_AGENT_NAME, 
		RESPONSIBILITY_KEY, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id
		,kca_seq_date
	from bec_raw_dl_ext.FND_RESPONSIBILITY
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
	and (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),kca_seq_id) in 
	(select nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),max(kca_seq_id) from bec_raw_dl_ext.FND_RESPONSIBILITY 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0))
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_responsibility')
		 
            )
);
end;