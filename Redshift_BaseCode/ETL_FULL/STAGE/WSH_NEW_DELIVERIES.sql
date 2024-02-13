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

DROP TABLE IF EXISTS bec_ods_stg.WSH_NEW_DELIVERIES ;

CREATE TABLE bec_ods_stg.WSH_NEW_DELIVERIES 
DISTKEY (DELIVERY_ID)
SORTKEY (DELIVERY_ID, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.WSH_NEW_DELIVERIES 
where kca_operation != 'DELETE' 
and (DELIVERY_ID,last_update_date) in 
(select DELIVERY_ID,max(last_update_date) from bec_raw_dl_ext.WSH_NEW_DELIVERIES  
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by DELIVERY_ID);

END;




