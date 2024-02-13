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

DROP TABLE IF EXISTS bec_ods_stg.MTL_ABC_ASSIGNMENTS;

CREATE TABLE bec_ods_stg.MTL_ABC_ASSIGNMENTS 
DISTKEY (INVENTORY_ITEM_ID) 
SORTKEY (  INVENTORY_ITEM_ID, ASSIGNMENT_GROUP_ID, ABC_CLASS_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
where kca_operation != 'DELETE' 
and (NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0),last_update_date) in 
(select NVL(INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,NVL(ASSIGNMENT_GROUP_ID,0) AS ASSIGNMENT_GROUP_ID ,NVL(ABC_CLASS_ID,0) AS ABC_CLASS_ID ,max(last_update_date) from bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by NVL(INVENTORY_ITEM_ID,0),NVL(ASSIGNMENT_GROUP_ID,0),NVL(ABC_CLASS_ID,0));

END;