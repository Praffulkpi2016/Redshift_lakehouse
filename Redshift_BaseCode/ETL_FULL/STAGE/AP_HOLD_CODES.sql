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
DROP TABLE IF EXISTS bec_ods_stg.AP_HOLD_CODES;

CREATE TABLE bec_ods_stg.AP_HOLD_CODES 
DISTKEY (HOLD_LOOKUP_CODE)
SORTKEY (HOLD_LOOKUP_CODE, LAST_UPDATE_DATE)
AS SELECT * FROM bec_raw_dl_ext.AP_HOLD_CODES
where kca_operation != 'DELETE' 
and (hold_lookup_code,last_update_date) in 
(select hold_lookup_code,max(last_update_date) from bec_raw_dl_ext.AP_HOLD_CODES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by hold_lookup_code);
END;