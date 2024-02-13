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
DROP TABLE IF EXISTS bec_ods_stg.AP_TERMS_TL;

CREATE TABLE bec_ods_stg.AP_TERMS_TL 
DISTKEY (TERM_ID)
SORTKEY (TERM_ID, LANGUAGE, LAST_UPDATE_DATE)
AS SELECT * FROM bec_raw_dl_ext.AP_TERMS_TL
WHERE kca_operation != 'DELETE' and (term_id,language,last_update_date) in (select term_id,language,max(last_update_date) from bec_raw_dl_ext.AP_TERMS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by term_id,language);
END;
