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
DROP TABLE IF EXISTS bec_ods_stg.AP_BANK_BRANCHES;

CREATE TABLE bec_ods_stg.AP_BANK_BRANCHES 
DISTKEY (BANK_BRANCH_ID)
SORTKEY (BANK_BRANCH_ID, LAST_UPDATE_DATE)
AS SELECT * FROM bec_raw_dl_ext.AP_BANK_BRANCHES
where kca_operation != 'DELETE' 
and (bank_branch_id,last_update_date) in 
(select bank_branch_id,max(last_update_date) from bec_raw_dl_ext.AP_BANK_BRANCHES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by bank_branch_id);
END;