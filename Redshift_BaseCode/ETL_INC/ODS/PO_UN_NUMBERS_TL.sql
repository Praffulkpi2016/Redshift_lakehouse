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

delete from bec_ods.PO_UN_NUMBERS_TL
where (un_number_id,language) in (
select stg.un_number_id,stg.language 
from bec_ods.PO_UN_NUMBERS_TL ods, bec_ods_stg.PO_UN_NUMBERS_TL stg
where 
ods.un_number_id = stg.un_number_id and 
ods.language = stg.language 


and stg.kca_operation IN ('INSERT','UPDATE')
);


-- Insert records

insert into	bec_ods.PO_UN_NUMBERS_TL
       (
	un_number_id,
	"language",
	source_lang,
	un_number,
	description,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)
	
	(
	select
		
	
     un_number_id,
	"language",
	source_lang,
	un_number,
	description,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.PO_UN_NUMBERS_TL
	where kca_operation in ('INSERT','UPDATE') 
	and (un_number_id,language ,kca_seq_id) in 
	(select un_number_id,language ,max(kca_seq_id) from bec_ods_stg.PO_UN_NUMBERS_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by un_number_id,language )
);
commit;


-- Soft delete
update bec_ods.PO_UN_NUMBERS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.PO_UN_NUMBERS_TL set IS_DELETED_FLG = 'Y'
where (un_number_id,language)  in
(
select un_number_id,language from bec_raw_dl_ext.PO_UN_NUMBERS_TL
where (un_number_id,language,KCA_SEQ_ID)
in 
(
select un_number_id,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.PO_UN_NUMBERS_TL
group by un_number_id,language
) 
and kca_operation= 'DELETE'
);
commit;

end;


update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'po_un_numbers_tl';

commit;