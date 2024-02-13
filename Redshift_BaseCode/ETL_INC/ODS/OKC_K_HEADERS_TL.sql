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

delete from bec_ods.OKC_K_HEADERS_TL
where (ID,language) in (
select stg.ID,stg.language
from bec_ods.OKC_K_HEADERS_TL ods, bec_ods_stg.OKC_K_HEADERS_TL stg
where ods.ID = stg.ID and ods.language = stg.language 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OKC_K_HEADERS_TL
       (id,
	"language",
	source_lang,
	sfwt_flag,
	short_description,
	comments,
	description,
	cognomen,
	non_response_reason,
	non_response_explain,
	set_aside_reason,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	security_group_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
	id,
	"language",
	source_lang,
	sfwt_flag,
	short_description,
	comments,
	description,
	cognomen,
	non_response_reason,
	non_response_explain,
	set_aside_reason,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	security_group_id,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.OKC_K_HEADERS_TL
	where kca_operation IN ('INSERT','UPDATE')
	and (ID,language,kca_seq_id) in 
	(select ID,language, max(kca_seq_id) from bec_ods_stg.OKC_K_HEADERS_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by ID,language)
);

commit;




-- Soft delete
update bec_ods.OKC_K_HEADERS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKC_K_HEADERS_TL set IS_DELETED_FLG = 'Y'
where (nvl(ID,0), nvl(LANGUAGE,'NA'))  in
(
select nvl(ID,0), nvl(LANGUAGE,'NA') from bec_raw_dl_ext.OKC_K_HEADERS_TL
where (nvl(ID,0), nvl(LANGUAGE,'NA'),KCA_SEQ_ID)
in 
(
select nvl(ID,0), nvl(LANGUAGE,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKC_K_HEADERS_TL
group by nvl(ID,0), nvl(LANGUAGE,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'okc_k_headers_tl';

commit;