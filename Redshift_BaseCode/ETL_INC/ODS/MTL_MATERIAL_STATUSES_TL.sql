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

delete from bec_ods.MTL_MATERIAL_STATUSES_TL
where (nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA')) in (
select nvl(stg.STATUS_ID,0) as STATUS_ID,
nvl(stg.LANGUAGE, 'NA') as LANGUAGE
from bec_ods.MTL_MATERIAL_STATUSES_TL ods, bec_ods_stg.MTL_MATERIAL_STATUSES_TL stg
where nvl(ods.STATUS_ID,0) = nvl(stg.STATUS_ID,0) 
and nvl(ods.LANGUAGE, 'NA') = nvl(stg.LANGUAGE, 'NA')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_MATERIAL_STATUSES_TL
       (	
		STATUS_ID, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		STATUS_CODE, 
		DESCRIPTION, 
		LANGUAGE, 
		SOURCE_LANG, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		STATUS_ID, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		STATUS_CODE, 
		DESCRIPTION, 
		LANGUAGE, 
		SOURCE_LANG, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MTL_MATERIAL_STATUSES_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA'),kca_seq_ID) in 
	(select nvl(STATUS_ID,0) as STATUS_ID,nvl(LANGUAGE, 'NA') as LANGUAGE,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.MTL_MATERIAL_STATUSES_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA'))
);

commit;

-- Soft delete
update bec_ods.MTL_MATERIAL_STATUSES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_MATERIAL_STATUSES_TL set IS_DELETED_FLG = 'Y'
where (nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA') )  in
(
select nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA')  from bec_raw_dl_ext.MTL_MATERIAL_STATUSES_TL
where (nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA') ,KCA_SEQ_ID)
in 
(
select nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA') ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_MATERIAL_STATUSES_TL
group by nvl(STATUS_ID,0),nvl(LANGUAGE, 'NA') 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_material_statuses_tl';

commit;