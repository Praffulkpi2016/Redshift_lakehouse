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

DROP TABLE IF EXISTS bec_ods_stg.BOM_COMPONENTS_B;

CREATE TABLE bec_ods_stg.BOM_COMPONENTS_B 
DISTKEY (COMPONENT_SEQUENCE_ID)
SORTKEY (COMPONENT_SEQUENCE_ID, LAST_UPDATE_DATE)
AS 
SELECT * FROM bec_raw_dl_ext.BOM_COMPONENTS_B
where kca_operation != 'DELETE' 
and (COMPONENT_SEQUENCE_ID,last_update_date) in 
(select COMPONENT_SEQUENCE_ID,max(last_update_date) from bec_raw_dl_ext.BOM_COMPONENTS_B 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by COMPONENT_SEQUENCE_ID);

END;