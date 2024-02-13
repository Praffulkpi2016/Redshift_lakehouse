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

delete from bec_ods.MTL_PLANNERS
where (nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA')) in (
select nvl(stg.ORGANIZATION_ID,0) as ORGANIZATION_ID,
nvl(stg.PLANNER_CODE, 'NA') as PLANNER_CODE
from bec_ods.MTL_PLANNERS ods, bec_ods_stg.MTL_PLANNERS stg
where nvl(ods.ORGANIZATION_ID,0) = nvl(stg.ORGANIZATION_ID,0) 
and nvl(ods.PLANNER_CODE, 'NA') = nvl(stg.PLANNER_CODE, 'NA')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_PLANNERS
       (	
		PLANNER_CODE, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DESCRIPTION, 
		DISABLE_DATE, 
		ATTRIBUTE_CATEGORY, 
		ATTRIBUTE1, 
		ATTRIBUTE2, 
		ATTRIBUTE3, 
		ATTRIBUTE4, 
		ATTRIBUTE5, 
		ATTRIBUTE6, 
		ATTRIBUTE7, 
		ATTRIBUTE8, 
		ATTRIBUTE9, 
		ATTRIBUTE10, 
		ATTRIBUTE11, 
		ATTRIBUTE12, 
		ATTRIBUTE13, 
		ATTRIBUTE14, 
		ATTRIBUTE15, 
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		ELECTRONIC_MAIL_ADDRESS, 
		EMPLOYEE_ID, 
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		PLANNER_CODE, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DESCRIPTION, 
		DISABLE_DATE, 
		ATTRIBUTE_CATEGORY, 
		ATTRIBUTE1, 
		ATTRIBUTE2, 
		ATTRIBUTE3, 
		ATTRIBUTE4, 
		ATTRIBUTE5, 
		ATTRIBUTE6, 
		ATTRIBUTE7, 
		ATTRIBUTE8, 
		ATTRIBUTE9, 
		ATTRIBUTE10, 
		ATTRIBUTE11, 
		ATTRIBUTE12, 
		ATTRIBUTE13, 
		ATTRIBUTE14, 
		ATTRIBUTE15, 
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		ELECTRONIC_MAIL_ADDRESS, 
		EMPLOYEE_ID, 
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MTL_PLANNERS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA'),kca_seq_ID) in 
	(select nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID,nvl(PLANNER_CODE, 'NA') as PLANNER_CODE,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.MTL_PLANNERS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA'))
);

commit;

-- Soft delete
update bec_ods.MTL_PLANNERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_PLANNERS set IS_DELETED_FLG = 'Y'
where (nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA') )  in
(
select nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA')  from bec_raw_dl_ext.MTL_PLANNERS
where (nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA') ,KCA_SEQ_ID)
in 
(
select nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA') ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_PLANNERS
group by nvl(ORGANIZATION_ID,0),nvl(PLANNER_CODE, 'NA') 
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_planners';

commit;