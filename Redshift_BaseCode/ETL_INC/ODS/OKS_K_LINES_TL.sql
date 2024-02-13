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

delete from bec_ods.OKS_K_LINES_TL
where (NVL(ID,'NA'),NVL(LANGUAGE,'NA')) in (
select NVL(stg.ID,'NA'),NVL(stg.LANGUAGE,'NA') from bec_ods.OKS_K_LINES_TL ods, bec_ods_stg.OKS_K_LINES_TL stg
where ods.ID = stg.ID AND ods.LANGUAGE = stg.LANGUAGE and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OKS_K_LINES_TL
       (	
	id,
	"language",
	source_lang,
	sfwt_flag,
	invoice_text,
	ib_trx_details,
	status_text,
	react_time_name,
	security_group_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
	id,
	"language",
	source_lang,
	sfwt_flag,
	invoice_text,
	ib_trx_details,
	status_text,
	react_time_name,
	security_group_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.OKS_K_LINES_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (ID,LANGUAGE,kca_seq_id) in 
	(select ID,LANGUAGE,max(kca_seq_id) from bec_ods_stg.OKS_K_LINES_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by ID,LANGUAGE)
);

commit;

-- Soft delete
update bec_ods.OKS_K_LINES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKS_K_LINES_TL set IS_DELETED_FLG = 'Y'
where (NVL(ID,'NA'),NVL(LANGUAGE,'NA'))  in
(
select NVL(ID,'NA'),NVL(LANGUAGE,'NA') from bec_raw_dl_ext.OKS_K_LINES_TL
where (NVL(ID,'NA'),NVL(LANGUAGE,'NA'),KCA_SEQ_ID)
in 
(
select NVL(ID,'NA'),NVL(LANGUAGE,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKS_K_LINES_TL
group by NVL(ID,'NA'),NVL(LANGUAGE,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oks_k_lines_tl';

commit;