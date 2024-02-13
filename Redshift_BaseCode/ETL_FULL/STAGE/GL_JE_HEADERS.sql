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
DROP TABLE IF EXISTS bec_ods_stg.GL_JE_HEADERS;

CREATE TABLE bec_ods_stg.GL_JE_HEADERS 
DISTKEY (je_header_id)
SORTKEY (je_header_id, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.GL_JE_HEADERS
WHERE kca_operation != 'DELETE' and (je_header_id,last_update_date)
in  (select je_header_id,max(last_update_date) from bec_raw_dl_ext.GL_JE_HEADERS 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by je_header_id);
END;