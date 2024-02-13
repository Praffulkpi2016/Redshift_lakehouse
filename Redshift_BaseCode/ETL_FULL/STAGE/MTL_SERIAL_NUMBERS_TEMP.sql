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

DROP TABLE IF EXISTS bec_ods_stg.MTL_SERIAL_NUMBERS_TEMP;

CREATE TABLE bec_ods_stg.MTL_SERIAL_NUMBERS_TEMP 
DISTKEY (TRANSACTION_TEMP_ID)
SORTKEY (TRANSACTION_TEMP_ID, FM_SERIAL_NUMBER, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.MTL_SERIAL_NUMBERS_TEMP
where kca_operation != 'DELETE' 
and (TRANSACTION_TEMP_ID,FM_SERIAL_NUMBER,last_update_date) in 
(select NVL(TRANSACTION_TEMP_ID,0) AS TRANSACTION_TEMP_ID ,NVL(FM_SERIAL_NUMBER,'NA') AS FM_SERIAL_NUMBER,max(last_update_date) from bec_raw_dl_ext.MTL_SERIAL_NUMBERS_TEMP 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by NVL(TRANSACTION_TEMP_ID,0),NVL(FM_SERIAL_NUMBER,'NA'));

END;