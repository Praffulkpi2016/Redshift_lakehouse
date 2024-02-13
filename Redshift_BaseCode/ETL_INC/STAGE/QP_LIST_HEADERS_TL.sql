/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;
TRUNCATE TABLE bec_ods_stg.QP_LIST_HEADERS_TL;


insert into bec_ods_stg.QP_LIST_HEADERS_TL
(
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
,KCA_SEQ_ID
,KCA_SEQ_DATE
) (SELECT 
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
,KCA_SEQ_ID
,KCA_SEQ_DATE
 from bec_raw_dl_ext.QP_LIST_HEADERS_TL 
 where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (LANGUAGE,LIST_HEADER_ID,KCA_SEQ_ID) in 
	(select LANGUAGE,LIST_HEADER_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.QP_LIST_HEADERS_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by LANGUAGE,LIST_HEADER_ID)
     and  (KCA_SEQ_DATE > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='qp_list_headers_tl')
)
);
END;