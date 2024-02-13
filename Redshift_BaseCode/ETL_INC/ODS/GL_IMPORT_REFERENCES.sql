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

delete
from
	bec_ods.GL_IMPORT_REFERENCES
where
	(
	nvl(JE_HEADER_ID,0),
	nvl(JE_LINE_NUM,0),
	nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),
	nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
	nvl(GL_SL_LINK_ID,0)
	) in 
	(
	select
		nvl(stg.JE_HEADER_ID,0) as JE_HEADER_ID ,
		nvl(stg.JE_LINE_NUM,0) as JE_LINE_NUM,
		nvl(stg.SUBLEDGER_DOC_SEQUENCE_ID,0) as SUBLEDGER_DOC_SEQUENCE_ID,
		nvl(stg.SUBLEDGER_DOC_SEQUENCE_VALUE,0) as SUBLEDGER_DOC_SEQUENCE_VALUE ,
		nvl(stg.GL_SL_LINK_ID,0) as GL_SL_LINK_ID 
	from
		bec_ods.GL_IMPORT_REFERENCES ods,
		bec_ods_stg.GL_IMPORT_REFERENCES stg
	where
	 
	  nvl(ods.JE_HEADER_ID,0) = nvl(stg.JE_HEADER_ID,0)
	and nvl(ods.JE_LINE_NUM,0) = nvl(stg.JE_LINE_NUM,0)
	and nvl(ods.SUBLEDGER_DOC_SEQUENCE_ID,0) = nvl(stg.SUBLEDGER_DOC_SEQUENCE_ID,0)
	and nvl(ods.SUBLEDGER_DOC_SEQUENCE_VALUE,0) = nvl(stg.SUBLEDGER_DOC_SEQUENCE_VALUE,0)
	and nvl(ods.GL_SL_LINK_ID,0) = nvl(stg.GL_SL_LINK_ID,0)	 
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.GL_IMPORT_REFERENCES
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
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
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
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.GL_IMPORT_REFERENCES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(JE_HEADER_ID,0),
		nvl(JE_LINE_NUM,0),
		nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),
		nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
		nvl(GL_SL_LINK_ID,0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(JE_HEADER_ID,0) as JE_HEADER_ID ,
			nvl(JE_LINE_NUM,0) as JE_LINE_NUM,
			nvl(SUBLEDGER_DOC_SEQUENCE_ID,0) as SUBLEDGER_DOC_SEQUENCE_ID,
			nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) as SUBLEDGER_DOC_SEQUENCE_VALUE ,
			nvl(GL_SL_LINK_ID,0) as GL_SL_LINK_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.GL_IMPORT_REFERENCES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(JE_HEADER_ID,0),
		nvl(JE_LINE_NUM,0),
		nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),
		nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
		nvl(GL_SL_LINK_ID,0)
			)	
	);

commit;

 
-- Soft delete

update
	bec_ods.GL_IMPORT_REFERENCES
set
	IS_DELETED_FLG = 'Y'
where
	(nvl(JE_HEADER_ID,0),
		nvl(JE_LINE_NUM,0),
		nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),
		nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
		nvl(GL_SL_LINK_ID,0)) in 
		(
	select
		nvl(ext.JE_HEADER_ID,0) as JE_HEADER_ID ,
		nvl(ext.JE_LINE_NUM,0) as JE_LINE_NUM,
		nvl(ext.SUBLEDGER_DOC_SEQUENCE_ID,0) as SUBLEDGER_DOC_SEQUENCE_ID,
		nvl(ext.SUBLEDGER_DOC_SEQUENCE_VALUE,0) as SUBLEDGER_DOC_SEQUENCE_VALUE ,
		nvl(ext.GL_SL_LINK_ID,0) as GL_SL_LINK_ID 
	from
		bec_ods.GL_IMPORT_REFERENCES ods,
		bec_raw_dl_ext.GL_IMPORT_REFERENCES ext
	where
		  nvl(ods.JE_HEADER_ID,0) = nvl(ext.JE_HEADER_ID,0)
		and nvl(ods.JE_LINE_NUM,0) = nvl(ext.JE_LINE_NUM,0)
		and nvl(ods.SUBLEDGER_DOC_SEQUENCE_ID,0) = nvl(ext.SUBLEDGER_DOC_SEQUENCE_ID,0)
		and nvl(ods.SUBLEDGER_DOC_SEQUENCE_VALUE,0) = nvl(ext.SUBLEDGER_DOC_SEQUENCE_VALUE,0)
		and nvl(ods.GL_SL_LINK_ID,0) = nvl(ext.GL_SL_LINK_ID,0)	 
		and ext.kca_operation = 'DELETE');

commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_import_references';

commit;