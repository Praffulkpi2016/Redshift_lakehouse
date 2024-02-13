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

delete from bec_ods.CS_INCIDENT_SEVERITIES_TL
where (nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA')) in (
select nvl(stg.INCIDENT_SEVERITY_ID,0) as INCIDENT_SEVERITY_ID,
nvl(stg.LANGUAGE, 'NA') as LANGUAGE
from bec_ods.CS_INCIDENT_SEVERITIES_TL ods, bec_ods_stg.CS_INCIDENT_SEVERITIES_TL stg
where nvl(ods.INCIDENT_SEVERITY_ID,0) = nvl(stg.INCIDENT_SEVERITY_ID,0) 
and nvl(ods.LANGUAGE, 'NA') = nvl(stg.LANGUAGE, 'NA')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CS_INCIDENT_SEVERITIES_TL
       (	
		INCIDENT_SEVERITY_ID, 
		LANGUAGE, 
		SOURCE_LANG, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DESCRIPTION, 
		NAME, 
		SECURITY_GROUP_ID,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		INCIDENT_SEVERITY_ID, 
		LANGUAGE, 
		SOURCE_LANG, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DESCRIPTION, 
		NAME, 
		SECURITY_GROUP_ID,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CS_INCIDENT_SEVERITIES_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA'),kca_seq_ID) in 
	(select nvl(INCIDENT_SEVERITY_ID,0) as INCIDENT_SEVERITY_ID,nvl(LANGUAGE, 'NA') as LANGUAGE,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.CS_INCIDENT_SEVERITIES_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA'))
);

commit;

-- Soft delete
update bec_ods.CS_INCIDENT_SEVERITIES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CS_INCIDENT_SEVERITIES_TL set IS_DELETED_FLG = 'Y'
where (nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA'))  in
(
select nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA') from bec_raw_dl_ext.CS_INCIDENT_SEVERITIES_TL
where (nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA'),KCA_SEQ_ID)
in 
(
select nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CS_INCIDENT_SEVERITIES_TL
group by nvl(INCIDENT_SEVERITY_ID,0),nvl(LANGUAGE, 'NA')
) 
and kca_operation= 'DELETE'
);
commit;


end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'cs_incident_severities_tl';

commit;