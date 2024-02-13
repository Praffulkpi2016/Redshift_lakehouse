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
begin;
DROP TABLE IF EXISTS bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL ; 
CREATE TABLE bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL 
DISTKEY (ONHAND_QUANTITIES_ID)
SORTKEY (  ONHAND_QUANTITIES_ID, last_update_date)
AS 
(SELECT * FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
WHERE kca_operation != 'DELETE' and  
(
 nvl(ONHAND_QUANTITIES_ID,0) ,
  last_update_date) 
in 
(select   
nvl(ONHAND_QUANTITIES_ID,0) AS ONHAND_QUANTITIES_ID ,
max(last_update_date) from bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by  
nvl(ONHAND_QUANTITIES_ID,0) 
)
);
end;