/*CATEGORY_SET_ID,
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
DROP TABLE IF EXISTS bec_ods_stg.MSC_ITEM_CATEGORIES;
CREATE TABLE bec_ods_stg.MSC_ITEM_CATEGORIES 
DISTKEY (ORGANIZATION_ID)
SORTKEY (  ORGANIZATION_ID, SR_INSTANCE_ID, INVENTORY_ITEM_ID, CATEGORY_SET_ID, SR_CATEGORY_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.MSC_ITEM_CATEGORIES
where kca_operation != 'DELETE' 
and (nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0),last_update_date) in 
(select nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID,nvl(SR_INSTANCE_ID,0) as SR_INSTANCE_ID,nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,nvl(CATEGORY_SET_ID,0) as CATEGORY_SET_ID,nvl(SR_CATEGORY_ID,0) as SR_CATEGORY_ID ,max(last_update_date) from bec_raw_dl_ext.MSC_ITEM_CATEGORIES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0));
END;