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

truncate table bec_ods_stg.fnd_new_messages;

insert into	bec_ods_stg.fnd_new_messages
   (APPLICATION_ID,
	LANGUAGE_CODE,
	MESSAGE_NUMBER,
	MESSAGE_NAME,
	MESSAGE_TEXT,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	DESCRIPTION,
	TYPE,
	MAX_LENGTH,
	CATEGORY,
	SEVERITY,
	FND_LOG_SEVERITY,
	ZD_EDITION_NAME,
	ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id
	,kca_seq_date)
(
	select
		APPLICATION_ID,
		LANGUAGE_CODE,
		MESSAGE_NUMBER,
		MESSAGE_NAME,
		MESSAGE_TEXT,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		DESCRIPTION,
		TYPE,
		MAX_LENGTH,
		CATEGORY,
		SEVERITY,
		FND_LOG_SEVERITY,
		ZD_EDITION_NAME,
		ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id
		,kca_seq_date
	from bec_raw_dl_ext.fnd_new_messages
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
	and (nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'),kca_seq_id,last_update_date) in 
	(select nvl(APPLICATION_ID,0) as APPLICATION_ID,nvl(MESSAGE_NAME,'NA') as MESSAGE_NAME,nvl(LANGUAGE_CODE,'NA') as LANGUAGE_CODE,nvl(MESSAGE_NUMBER,0) as MESSAGE_NUMBER,nvl(ZD_EDITION_NAME,'NA') as ZD_EDITION_NAME,max(kca_seq_id) 
	,max(last_update_date)
	from bec_raw_dl_ext.fnd_new_messages 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'))
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_new_messages')
		 
            )
);
end;