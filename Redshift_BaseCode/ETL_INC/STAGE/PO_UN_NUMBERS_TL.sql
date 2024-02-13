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

begin;

truncate table bec_ods_stg.PO_UN_NUMBERS_TL;
INSERT INTO bec_ods_stg.PO_UN_NUMBERS_TL (
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
	kca_seq_id
	,KCA_SEQ_DATE
)

(
	 SELECT
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
    kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.PO_UN_NUMBERS_TL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (un_number_id,language ,kca_seq_id) in 
	(select un_number_id,language ,max(kca_seq_id) from bec_raw_dl_ext.PO_UN_NUMBERS_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by un_number_id,language )
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_un_numbers_tl')
            )	
);
end;