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

truncate table bec_ods_stg.GL_IMPORT_REFERENCES;

insert into	bec_ods_stg.GL_IMPORT_REFERENCES
   (je_batch_id,
	je_header_id,
	je_line_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	reference_1,
	reference_2,
	reference_3,
	reference_4,
	reference_5,
	reference_6,
	reference_7,
	reference_8,
	reference_9,
	reference_10,
	subledger_doc_sequence_id,
	subledger_doc_sequence_value,
	gl_sl_link_id,
	gl_sl_link_table,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
	je_batch_id,
	je_header_id,
	je_line_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	reference_1,
	reference_2,
	reference_3,
	reference_4,
	reference_5,
	reference_6,
	reference_7,
	reference_8,
	reference_9,
	reference_10,
	subledger_doc_sequence_id,
	subledger_doc_sequence_value,
	gl_sl_link_id,
	gl_sl_link_table,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.GL_IMPORT_REFERENCES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(JE_HEADER_ID,0),nvl(JE_LINE_NUM,0),nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
nvl(GL_SL_LINK_ID,0),kca_seq_id) in 
	(select nvl(JE_HEADER_ID,0) as JE_HEADER_ID ,
	nvl(JE_LINE_NUM,0) as JE_LINE_NUM,
	nvl(SUBLEDGER_DOC_SEQUENCE_ID,0) as SUBLEDGER_DOC_SEQUENCE_ID,
	nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) as SUBLEDGER_DOC_SEQUENCE_VALUE ,
	nvl(GL_SL_LINK_ID,0) as GL_SL_LINK_ID,
	max(kca_seq_id) from bec_raw_dl_ext.GL_IMPORT_REFERENCES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(JE_HEADER_ID,0),nvl(JE_LINE_NUM,0),nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
nvl(GL_SL_LINK_ID,0) )
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_import_references')

            )
);
end;