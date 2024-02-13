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

delete from bec_ods.FA_ADDITIONS_TL
where (nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0)) in (
select nvl(stg.LANGUAGE,'NA') as LANGUAGE,nvl(stg.ASSET_ID,0) as ASSET_ID from bec_ods.FA_ADDITIONS_TL ods, bec_ods_stg.FA_ADDITIONS_TL stg
where nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA') and nvl(ods.ASSET_ID,0) = nvl(stg.ASSET_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FA_ADDITIONS_TL
       (	
		ASSET_ID, 
		LANGUAGE, 
		SOURCE_LANG, 
		DESCRIPTION, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		ASSET_ID, 
		LANGUAGE, 
		SOURCE_LANG, 
		DESCRIPTION, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.FA_ADDITIONS_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0),kca_seq_id) in 
	(select nvl(LANGUAGE,'NA') as LANGUAGE,nvl(ASSET_ID,0) as ASSET_ID,max(kca_seq_id) from bec_ods_stg.FA_ADDITIONS_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0))
);

commit;

-- Soft delete
update bec_ods.FA_ADDITIONS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_ADDITIONS_TL set IS_DELETED_FLG = 'Y'
where (nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0))  in
(
select nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0) from bec_raw_dl_ext.FA_ADDITIONS_TL
where (nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0),KCA_SEQ_ID)
in 
(
select nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_ADDITIONS_TL
group by nvl(LANGUAGE,'NA'),nvl(ASSET_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_additions_tl';

commit;