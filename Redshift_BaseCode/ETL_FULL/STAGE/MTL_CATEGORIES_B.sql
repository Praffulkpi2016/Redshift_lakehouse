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
DROP TABLE IF EXISTS bec_ods_stg.MTL_CATEGORIES_B;

CREATE TABLE bec_ods_stg.MTL_CATEGORIES_B 
DISTKEY (category_id)
SORTKEY (  category_id, last_update_date)
AS 
select * from bec_raw_dl_ext.MTL_CATEGORIES_B
where kca_operation != 'DELETE' and (category_id,last_update_date) in (select category_id,max(last_update_date) from bec_raw_dl_ext.MTL_CATEGORIES_B 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by category_id);

END;