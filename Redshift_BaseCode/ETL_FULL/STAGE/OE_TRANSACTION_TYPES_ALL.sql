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

DROP TABLE IF EXISTS bec_ods_stg.oe_transaction_types_all;

CREATE TABLE bec_ods_stg.oe_transaction_types_all 
DISTKEY (TRANSACTION_TYPE_ID)
SORTKEY (TRANSACTION_TYPE_ID, last_update_date) AS 
SELECT * FROM bec_raw_dl_ext.oe_transaction_types_all
where kca_operation != 'DELETE' 
and (nvl(TRANSACTION_TYPE_ID,0),last_update_date) in 
(select nvl(TRANSACTION_TYPE_ID,0) as TRANSACTION_TYPE_ID,max(last_update_date) from bec_raw_dl_ext.oe_transaction_types_all 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(TRANSACTION_TYPE_ID,0));

END;