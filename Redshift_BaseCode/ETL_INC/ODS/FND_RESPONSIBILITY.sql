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

delete from bec_ods.FND_RESPONSIBILITY
where (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0)) in (
select nvl(stg.APPLICATION_ID,0) as APPLICATION_ID,nvl(stg.RESPONSIBILITY_ID,0) as RESPONSIBILITY_ID from bec_ods.FND_RESPONSIBILITY ods, bec_ods_stg.FND_RESPONSIBILITY stg
where nvl(ods.APPLICATION_ID,0) = nvl(stg.APPLICATION_ID,0) and nvl(ods.RESPONSIBILITY_ID,0) = nvl(stg.RESPONSIBILITY_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FND_RESPONSIBILITY
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.FND_RESPONSIBILITY
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),kca_seq_id) in 
	(select nvl(APPLICATION_ID,0) as APPLICATION_ID,nvl(RESPONSIBILITY_ID,0) as RESPONSIBILITY_ID,max(kca_seq_id) from bec_ods_stg.FND_RESPONSIBILITY 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0))
);

commit;
 

-- Soft delete
update bec_ods.FND_RESPONSIBILITY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FND_RESPONSIBILITY set IS_DELETED_FLG = 'Y'
where (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0))  in
(
select nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0) from bec_raw_dl_ext.FND_RESPONSIBILITY
where (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),KCA_SEQ_ID)
in 
(
select nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FND_RESPONSIBILITY
group by nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fnd_responsibility';

commit;