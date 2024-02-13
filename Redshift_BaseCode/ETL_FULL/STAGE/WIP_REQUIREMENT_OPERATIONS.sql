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

DROP TABLE IF EXISTS bec_ods_stg.wip_requirement_operations;

CREATE TABLE bec_ods_stg.wip_requirement_operations  DISTKEY (INVENTORY_ITEM_ID)
SORTKEY (INVENTORY_ITEM_ID, WIP_ENTITY_ID, OPERATION_SEQ_NUM, REPETITIVE_SCHEDULE_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.wip_requirement_operations
where kca_operation != 'DELETE' 
and (nvl(INVENTORY_ITEM_ID,0),nvl(WIP_ENTITY_ID,0),nvl(OPERATION_SEQ_NUM,0),nvl(REPETITIVE_SCHEDULE_ID,0),last_update_date) in 
(select nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,
nvl(WIP_ENTITY_ID,0) as WIP_ENTITY_ID,
nvl(OPERATION_SEQ_NUM,0) as OPERATION_SEQ_NUM,
nvl(REPETITIVE_SCHEDULE_ID,0) as REPETITIVE_SCHEDULE_ID,
max(last_update_date) from bec_raw_dl_ext.wip_requirement_operations 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(INVENTORY_ITEM_ID,0),nvl(WIP_ENTITY_ID,0),
nvl(OPERATION_SEQ_NUM,0),nvl(REPETITIVE_SCHEDULE_ID,0));

END;
