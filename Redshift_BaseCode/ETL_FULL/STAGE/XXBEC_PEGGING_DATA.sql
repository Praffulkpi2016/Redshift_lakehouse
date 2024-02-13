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

DROP TABLE IF EXISTS bec_ods_stg.XXBEC_PEGGING_DATA;

CREATE TABLE bec_ods_stg.xxbec_pegging_data 
DISTSTYLE AUTO 
SORTKEY (kca_operation)
AS 
SELECT * FROM bec_raw_dl_ext.xxbec_pegging_data
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = '';
END;