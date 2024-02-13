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

delete from bec_ods.OKC_K_GRPINGS
where (nvl(ID,'NA')) in (
select nvl(stg.ID,'NA') as ID from bec_ods.OKC_K_GRPINGS ods, bec_ods_stg.OKC_K_GRPINGS stg
where nvl(ods.ID,'NA') = nvl(stg.ID,'NA')  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OKC_K_GRPINGS
       (	
		ID,
		CGP_PARENT_ID,
		INCLUDED_CGP_ID,
		INCLUDED_CHR_ID,
		OBJECT_VERSION_NUMBER,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		SECURITY_GROUP_ID,
		SCS_CODE,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		ID,
		CGP_PARENT_ID,
		INCLUDED_CGP_ID,
		INCLUDED_CHR_ID,
		OBJECT_VERSION_NUMBER,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		SECURITY_GROUP_ID,
		SCS_CODE,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.OKC_K_GRPINGS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(ID,'NA'),kca_seq_id) in 
	(select nvl(ID,'NA') as ID,max(kca_seq_id) as kca_seq_id from bec_ods_stg.OKC_K_GRPINGS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(ID,'NA')
	 )
);

commit;

-- Soft delete
update bec_ods.OKC_K_GRPINGS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKC_K_GRPINGS set IS_DELETED_FLG = 'Y'
where (nvl(ID,'NA'))  in
(
select nvl(ID,'NA') from bec_raw_dl_ext.OKC_K_GRPINGS
where (nvl(ID,'NA'),KCA_SEQ_ID)
in 
(
select nvl(ID,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKC_K_GRPINGS
group by nvl(ID,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'okc_k_grpings';

commit;