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
DROP TABLE IF EXISTS bec_ods_stg.GL_CODE_COMBINATIONS;

CREATE TABLE bec_ods_stg.GL_CODE_COMBINATIONS 
DISTKEY (code_combination_id)
SORTKEY (code_combination_id, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.GL_CODE_COMBINATIONS
WHERE kca_operation != 'DELETE' and (code_combination_id,last_update_date) in  (select code_combination_id,max(last_update_date) from bec_raw_dl_ext.GL_CODE_COMBINATIONS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by code_combination_id);
END;