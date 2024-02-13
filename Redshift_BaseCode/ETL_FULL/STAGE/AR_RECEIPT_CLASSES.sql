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

DROP TABLE IF EXISTS bec_ods_stg.AR_RECEIPT_CLASSES;

CREATE TABLE bec_ods_stg.AR_RECEIPT_CLASSES 
DISTKEY (RECEIPT_CLASS_ID)
SORTKEY (RECEIPT_CLASS_ID, LAST_UPDATE_DATE)
AS 
SELECT * FROM bec_raw_dl_ext.AR_RECEIPT_CLASSES
where kca_operation != 'DELETE' 
and (RECEIPT_CLASS_ID,last_update_date) in 
(select RECEIPT_CLASS_ID,max(last_update_date) from bec_raw_dl_ext.AR_RECEIPT_CLASSES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by RECEIPT_CLASS_ID);

END;