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

DROP TABLE IF EXISTS bec_ods_stg.RCV_LOTS_SUPPLY;

CREATE TABLE bec_ods_stg.RCV_LOTS_SUPPLY 
DISTKEY (SUPPLY_TYPE_CODE)
SORTKEY (SHIPMENT_LINE_ID, LOT_NUM, quantity, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
where kca_operation != 'DELETE' 
and (NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity,last_update_date) in 
(select NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity,max(last_update_date) from bec_raw_dl_ext.RCV_LOTS_SUPPLY
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity);

END;