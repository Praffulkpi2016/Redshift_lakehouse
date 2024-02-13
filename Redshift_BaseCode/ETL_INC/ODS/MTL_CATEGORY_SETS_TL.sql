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

delete from bec_ods.MTL_CATEGORY_SETS_TL
where (CATEGORY_SET_ID,language) in (
select stg.CATEGORY_SET_ID,stg.language from bec_ods.MTL_CATEGORY_SETS_TL ods, bec_ods_stg.MTL_CATEGORY_SETS_TL stg
where ods.CATEGORY_SET_ID = stg.CATEGORY_SET_ID and ods.language = stg.language and stg.kca_operation in ('INSERT','UPDATE'));

commit;

-- Insert records

INSERT
	INTO
	bec_ods.MTL_CATEGORY_SETS_TL
(   CATEGORY_SET_ID,
	LANGUAGE,
	SOURCE_LANG,
	CATEGORY_SET_NAME,
	DESCRIPTION,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	ZD_EDITION_NAME,
    ZD_SYNC,
	KCA_OPERATION,
    IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
	
( 	SELECT
	CATEGORY_SET_ID,
	LANGUAGE,
	SOURCE_LANG,
	CATEGORY_SET_NAME,
	DESCRIPTION,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	ZD_EDITION_NAME,
    ZD_SYNC,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
FROM
	bec_ods_stg.MTL_CATEGORY_SETS_TL
where kca_operation IN ('INSERT','UPDATE') and (CATEGORY_SET_ID,language,kca_seq_id) in (select CATEGORY_SET_ID,language,max(kca_seq_id) from bec_ods_stg.MTL_CATEGORY_SETS_TL 
where kca_operation IN ('INSERT','UPDATE')
group by CATEGORY_SET_ID,language)


);
commit; 
 
-- Soft delete
update bec_ods.MTL_CATEGORY_SETS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_CATEGORY_SETS_TL set IS_DELETED_FLG = 'Y'
where (CATEGORY_SET_ID,language)  in
(
select CATEGORY_SET_ID,language from bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
where (CATEGORY_SET_ID,language,KCA_SEQ_ID)
in 
(
select CATEGORY_SET_ID,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
group by CATEGORY_SET_ID,language
) 
and kca_operation= 'DELETE'
);
commit;


end;


update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'mtl_category_sets_tl';

commit;

