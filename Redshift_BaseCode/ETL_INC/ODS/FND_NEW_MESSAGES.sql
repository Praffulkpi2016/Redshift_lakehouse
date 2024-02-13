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

delete from bec_ods.fnd_new_messages
where (nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'))  in (
select nvl(stg.APPLICATION_ID,0) as APPLICATION_ID,nvl(stg.MESSAGE_NAME,'NA') as MESSAGE_NAME,nvl(stg.LANGUAGE_CODE,'NA') as LANGUAGE_CODE,nvl(stg.MESSAGE_NUMBER,0) as MESSAGE_NUMBER ,nvl(stg.ZD_EDITION_NAME,'NA') as ZD_EDITION_NAME from bec_ods.fnd_new_messages ods, bec_ods_stg.fnd_new_messages stg
where nvl(ods.APPLICATION_ID,0) = nvl(stg.APPLICATION_ID,0) 
and nvl(ods.MESSAGE_NAME,'NA') = nvl(stg.MESSAGE_NAME,'NA') 
and nvl(ods.LANGUAGE_CODE,'NA') = nvl(stg.LANGUAGE_CODE,'NA') 
and nvl(ods.MESSAGE_NUMBER,0) = nvl(stg.MESSAGE_NUMBER,0)  
and nvl(ods.ZD_EDITION_NAME,'NA') = nvl(stg.ZD_EDITION_NAME,'NA')  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.fnd_new_messages
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
        IS_DELETED_FLG,
		kca_seq_id, 
	    kca_seq_date)	
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
	from bec_ods_stg.fnd_new_messages
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'),kca_seq_id) in 
	(select nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA') as ZD_EDITION_NAME,max(kca_seq_id) from bec_ods_stg.fnd_new_messages 
     where kca_operation IN ('INSERT','UPDATE') 
     group by nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'))
);

commit;

 
-- Soft delete
update bec_ods.fnd_new_messages set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fnd_new_messages set IS_DELETED_FLG = 'Y'
where (nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'))  in
(
select nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA') from bec_raw_dl_ext.fnd_new_messages
where (nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'),KCA_SEQ_ID)
in 
(
select nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fnd_new_messages
group by nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fnd_new_messages';

commit;