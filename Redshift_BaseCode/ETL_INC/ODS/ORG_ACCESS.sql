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

delete from bec_ods.ORG_ACCESS
where (nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ))  in (
select nvl(stg.RESP_APPLICATION_ID,0) as RESP_APPLICATION_ID,nvl(stg.RESPONSIBILITY_ID, 0 ) as RESPONSIBILITY_ID,nvl(stg.ORGANIZATION_ID, 0 ) AS ORGANIZATION_ID from bec_ods.ORG_ACCESS ods, bec_ods_stg.ORG_ACCESS stg
where nvl(ods.RESP_APPLICATION_ID,0) = nvl(stg.RESP_APPLICATION_ID,0)
and nvl(ods.RESPONSIBILITY_ID,0) = nvl(stg.RESPONSIBILITY_ID,0)
and nvl(ods.ORGANIZATION_ID,0) = nvl(stg.ORGANIZATION_ID,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.ORG_ACCESS
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.ORG_ACCESS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ),kca_seq_id) in 
	(select nvl(RESP_APPLICATION_ID,0) as RESP_APPLICATION_ID,nvl(RESPONSIBILITY_ID, 0 ) as RESPONSIBILITY_ID,nvl(ORGANIZATION_ID, 0 ) AS ORGANIZATION_ID,max(kca_seq_id) from bec_ods_stg.ORG_ACCESS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID, 0 ),nvl(ORGANIZATION_ID, 0 ))
);

commit;

-- Soft delete
update bec_ods.ORG_ACCESS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ORG_ACCESS set IS_DELETED_FLG = 'Y'
where (nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(ORGANIZATION_ID, 0 ) )  in
(
select nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(ORGANIZATION_ID, 0 )  from bec_raw_dl_ext.ORG_ACCESS
where (nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(ORGANIZATION_ID, 0 ) ,KCA_SEQ_ID)
in 
(
select nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(ORGANIZATION_ID, 0 ) ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ORG_ACCESS
group by nvl(RESP_APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(ORGANIZATION_ID, 0 ) 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'org_access';

commit;