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

DROP TABLE IF EXISTS bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY;

CREATE TABLE bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY 
DISTKEY (flex_value_set_id)
SORTKEY (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
where kca_operation != 'DELETE' 
and (flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,last_update_date) in 
(select flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,max(last_update_date) from bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE);

END;