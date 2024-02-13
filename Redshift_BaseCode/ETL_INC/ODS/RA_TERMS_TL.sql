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

delete from bec_ods.RA_TERMS_TL
where (TERM_ID, LANGUAGE) in (
select stg.TERM_ID, stg.LANGUAGE from bec_ods.RA_TERMS_TL ods,  bec_ods_stg.RA_TERMS_TL stg
where ods.TERM_ID = stg.TERM_ID and ods.LANGUAGE = stg.LANGUAGE
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records
INSERT INTO bec_ods.RA_TERMS_TL
(
TERM_ID
,DESCRIPTION
,NAME
,LANGUAGE
,SOURCE_LANG
,LAST_UPDATE_DATE
,CREATION_DATE
,CREATED_BY
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(
SELECT
TERM_ID
,DESCRIPTION
,NAME
,LANGUAGE
,SOURCE_LANG
,LAST_UPDATE_DATE
,CREATION_DATE
,CREATED_BY
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
       ,'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RA_TERMS_TL
	where kca_operation in ('INSERT','UPDATE') 
	and (TERM_ID, LANGUAGE,kca_seq_id) in 
	(select TERM_ID, LANGUAGE,max(kca_seq_id) from bec_ods_stg.RA_TERMS_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by TERM_ID, LANGUAGE)
);

commit;



-- Soft delete
update bec_ods.RA_TERMS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_TERMS_TL set IS_DELETED_FLG = 'Y'
where (TERM_ID, LANGUAGE)  in
(
select TERM_ID, LANGUAGE from bec_raw_dl_ext.RA_TERMS_TL
where (TERM_ID, LANGUAGE,KCA_SEQ_ID)
in 
(
select TERM_ID, LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_TERMS_TL
group by TERM_ID, LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate(),load_type = 'I'
where ods_table_name = 'ra_terms_tl';

commit;