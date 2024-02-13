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

delete from bec_ods.QP_LIST_HEADERS_TL
where ( NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0)) in (
select NVL(stg.LANGUAGE,'NA') as LANGUAGE,NVL(stg.LIST_HEADER_ID,0)  as LIST_HEADER_ID
from bec_ods.QP_LIST_HEADERS_TL ods, bec_ods_stg.QP_LIST_HEADERS_TL stg
where nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA') 
AND nvl(ods.LIST_HEADER_ID,0) = nvl(stg.LIST_HEADER_ID,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.QP_LIST_HEADERS_TL
(		list_header_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	"language",
	source_lang,
	"name",
	description,
	version_no
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
	list_header_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	"language",
	source_lang,
	"name",
	description,
	version_no
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from bec_ods_stg.QP_LIST_HEADERS_TL
where kca_operation in ('INSERT','UPDATE') 
	and (NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0),kca_seq_id) in 
	(select NVL(LANGUAGE,'NA')as LANGUAGE,NVL(LIST_HEADER_ID,0)as LIST_HEADER_ID,max(kca_seq_id) from bec_ods_stg.QP_LIST_HEADERS_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0))
);

commit;


-- Soft delete
update bec_ods.QP_LIST_HEADERS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.QP_LIST_HEADERS_TL set IS_DELETED_FLG = 'Y'
where (NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0))  in
(
select NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0) from bec_raw_dl_ext.QP_LIST_HEADERS_TL
where (NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0),KCA_SEQ_ID)
in 
(
select NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.QP_LIST_HEADERS_TL
group by NVL(LANGUAGE,'NA'),NVL(LIST_HEADER_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate() 
where ods_table_name='qp_list_headers_tl';
commit;