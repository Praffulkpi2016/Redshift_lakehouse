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
DROP TABLE IF EXISTS bec_ods_stg.GL_JE_SOURCES_TL;

CREATE TABLE bec_ods_stg.GL_JE_SOURCES_TL 
DISTKEY (JE_SOURCE_NAME)
SORTKEY (JE_SOURCE_NAME, LANGUAGE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.GL_JE_SOURCES_TL
WHERE kca_operation != 'DELETE' and (JE_SOURCE_NAME,LANGUAGE,last_update_date)
in  (select JE_SOURCE_NAME,LANGUAGE,max(last_update_date) from bec_raw_dl_ext.GL_JE_SOURCES_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by JE_SOURCE_NAME,LANGUAGE);
END;