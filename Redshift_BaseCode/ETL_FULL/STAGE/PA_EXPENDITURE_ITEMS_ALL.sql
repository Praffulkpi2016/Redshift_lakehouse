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

DROP TABLE IF EXISTS bec_ods_stg.pa_expenditure_items_all;

CREATE TABLE bec_ods_stg.pa_expenditure_items_all 
DISTKEY (EXPENDITURE_ITEM_ID)
SORTKEY (EXPENDITURE_ITEM_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.pa_expenditure_items_all
where kca_operation != 'DELETE' 
and (nvl(EXPENDITURE_ITEM_ID,0),last_update_date) in 
(select nvl(EXPENDITURE_ITEM_ID,0) as EXPENDITURE_ITEM_ID,max(last_update_date) from bec_raw_dl_ext.pa_expenditure_items_all 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(EXPENDITURE_ITEM_ID,0));

END;