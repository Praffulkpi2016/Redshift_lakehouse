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

DROP TABLE IF EXISTS bec_ods_stg.mtl_supply;

CREATE TABLE bec_ods_stg.mtl_supply 
DISTKEY (SUPPLY_TYPE_CODE)
SORTKEY (SUPPLY_SOURCE_ID, PO_DISTRIBUTION_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.mtl_supply
where kca_operation != 'DELETE' 
and (SUPPLY_TYPE_CODE,SUPPLY_SOURCE_ID,nvl(PO_DISTRIBUTION_ID,0),last_update_date) in 
(select SUPPLY_TYPE_CODE,SUPPLY_SOURCE_ID,nvl(PO_DISTRIBUTION_ID,0),max(last_update_date) from bec_raw_dl_ext.mtl_supply 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by SUPPLY_TYPE_CODE,SUPPLY_SOURCE_ID,nvl(PO_DISTRIBUTION_ID,0));

END;