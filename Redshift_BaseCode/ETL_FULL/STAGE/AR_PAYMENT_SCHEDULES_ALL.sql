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

DROP TABLE IF EXISTS bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL;

CREATE TABLE bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL 
DISTKEY (PAYMENT_SCHEDULE_ID)
SORTKEY (PAYMENT_SCHEDULE_ID, LAST_UPDATE_DATE)
AS 
SELECT * FROM bec_raw_dl_ext.AR_PAYMENT_SCHEDULES_ALL
where kca_operation != 'DELETE' 
and (PAYMENT_SCHEDULE_ID,last_update_date) in 
(select PAYMENT_SCHEDULE_ID,max(last_update_date) from bec_raw_dl_ext.AR_PAYMENT_SCHEDULES_ALL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by PAYMENT_SCHEDULE_ID);

END;