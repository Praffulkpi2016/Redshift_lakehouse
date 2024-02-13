/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
BEGIN; 
DROP TABLE IF EXISTS bec_ods_stg.GL_IMPORT_REFERENCES;

CREATE TABLE bec_ods_stg.GL_IMPORT_REFERENCES 
DISTKEY (JE_HEADER_ID)
SORTKEY (
  JE_HEADER_ID,
  JE_LINE_NUM,
  SUBLEDGER_DOC_SEQUENCE_ID,
  SUBLEDGER_DOC_SEQUENCE_VALUE,
  GL_SL_LINK_ID,
  last_update_date
)
AS SELECT * FROM bec_raw_dl_ext.GL_IMPORT_REFERENCES
where kca_operation != 'DELETE' and (
nvl(JE_HEADER_ID,0),
nvl(JE_LINE_NUM,0),
nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),
nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,
nvl(GL_SL_LINK_ID,0),
 last_update_date) in (
select   nvl(JE_HEADER_ID,0) as JE_HEADER_ID ,
	nvl(JE_LINE_NUM,0) as JE_LINE_NUM,
	nvl(SUBLEDGER_DOC_SEQUENCE_ID,0) as SUBLEDGER_DOC_SEQUENCE_ID,
	nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) as SUBLEDGER_DOC_SEQUENCE_VALUE ,
	nvl(GL_SL_LINK_ID,0) as GL_SL_LINK_ID,
	max(last_update_date) from bec_raw_dl_ext.GL_IMPORT_REFERENCES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(JE_HEADER_ID,0),nvl(JE_LINE_NUM,0),nvl(SUBLEDGER_DOC_SEQUENCE_ID,0),nvl(SUBLEDGER_DOC_SEQUENCE_VALUE,0) ,nvl(GL_SL_LINK_ID,0));

END;