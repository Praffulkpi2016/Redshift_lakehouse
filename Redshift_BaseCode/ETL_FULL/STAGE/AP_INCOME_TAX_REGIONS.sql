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
DROP TABLE IF EXISTS bec_ods_stg.AP_INCOME_TAX_REGIONS;

CREATE TABLE bec_ods_stg.AP_INCOME_TAX_REGIONS 
DISTKEY (REGION_SHORT_NAME)
SORTKEY (REGION_SHORT_NAME, LAST_UPDATE_DATE)
AS SELECT * FROM bec_raw_dl_ext.AP_INCOME_TAX_REGIONS
where kca_operation != 'DELETE' 
and (REGION_SHORT_NAME,last_update_date) in 
(select REGION_SHORT_NAME,max(last_update_date) from bec_raw_dl_ext.AP_INCOME_TAX_REGIONS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by REGION_SHORT_NAME);
END;