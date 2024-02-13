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
DROP TABLE IF EXISTS bec_ods_stg.AP_TERMS_LINES;

CREATE TABLE bec_ods_stg.AP_TERMS_LINES
DISTKEY (TERM_ID)
SORTKEY (TERM_ID, SEQUENCE_NUM, LAST_UPDATE_DATE)
AS 
select * from bec_raw_dl_ext.AP_TERMS_LINES
where kca_operation != 'DELETE' and (term_id,SEQUENCE_NUM,last_update_date) in (select term_id,SEQUENCE_NUM,max(last_update_date) from bec_raw_dl_ext.AP_TERMS_LINES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by term_id,SEQUENCE_NUM);
END;
