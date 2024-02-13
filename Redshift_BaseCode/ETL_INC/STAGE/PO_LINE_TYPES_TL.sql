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

truncate table bec_ods_stg.PO_LINE_TYPES_TL;

insert into	bec_ods_stg.PO_LINE_TYPES_TL
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
	kca_seq_id
	,KCA_SEQ_DATE)
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
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE	from bec_raw_dl_ext.PO_LINE_TYPES_TL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (LINE_TYPE_ID,LANGUAGE,kca_seq_id) in 
	(select LINE_TYPE_ID,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.PO_LINE_TYPES_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by LINE_TYPE_ID,LANGUAGE)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_line_types_tl')
            )	
);
end;