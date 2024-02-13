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
DROP TABLE IF EXISTS bec_ods_stg.AP_NOTES;

CREATE TABLE bec_ods_stg.AP_NOTES 
DISTKEY (NOTE_ID)
SORTKEY (NOTE_ID, LAST_UPDATE_DATE)
AS SELECT * FROM bec_raw_dl_ext.AP_NOTES
where kca_operation != 'DELETE' and (NOTE_ID,last_update_date) in (select NOTE_ID,max(last_update_date) from bec_raw_dl_ext.AP_NOTES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by NOTE_ID);

END;
