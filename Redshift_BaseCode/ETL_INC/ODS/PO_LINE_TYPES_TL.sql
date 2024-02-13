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

delete from bec_ods.PO_LINE_TYPES_TL
where (LINE_TYPE_ID,language) in (
select stg.LINE_TYPE_ID,stg.language
from bec_ods.PO_LINE_TYPES_TL ods, bec_ods_stg.PO_LINE_TYPES_TL stg
where ods.LINE_TYPE_ID = stg.LINE_TYPE_ID and ods.language = stg.language 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.PO_LINE_TYPES_TL
       (line_type_id,
	"language",
	source_lang,
	description,
	line_type,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	ZD_EDITION_NAME ,
	ZD_SYNC,	
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
		line_type_id,
	"language",
	source_lang,
	description,
	line_type,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	ZD_EDITION_NAME ,
	ZD_SYNC,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.PO_LINE_TYPES_TL
	where kca_operation in ('INSERT','UPDATE') 
	and (LINE_TYPE_ID,language,kca_seq_id) in 
	(select LINE_TYPE_ID,language,max(kca_seq_id) from bec_ods_stg.PO_LINE_TYPES_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by LINE_TYPE_ID,language)
);

commit;


-- Soft delete
update bec_ods.PO_LINE_TYPES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.PO_LINE_TYPES_TL set IS_DELETED_FLG = 'Y'
where (LINE_TYPE_ID,language)  in
(
select LINE_TYPE_ID,language from bec_raw_dl_ext.PO_LINE_TYPES_TL
where (LINE_TYPE_ID,language,KCA_SEQ_ID)
in 
(
select LINE_TYPE_ID,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.PO_LINE_TYPES_TL
group by LINE_TYPE_ID,language
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'po_line_types_tl';

commit;