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

DROP TABLE IF EXISTS bec_ods_stg.XLA_DISTRIBUTION_LINKS;

CREATE TABLE bec_ods_stg.XLA_DISTRIBUTION_LINKS 
DISTKEY(AE_HEADER_ID)
SORTKEY (REF_AE_HEADER_ID, APPLICATION_ID, TEMP_LINE_NUM)
AS 
SELECT * FROM bec_raw_dl_ext.XLA_DISTRIBUTION_LINKS
where kca_operation != 'DELETE' 
and (nvl(APPLICATION_ID,0), nvl(REF_AE_HEADER_ID,0), nvl(TEMP_LINE_NUM,0), nvl(AE_HEADER_ID,0)) in 
(select nvl(APPLICATION_ID,0), nvl(REF_AE_HEADER_ID,0), nvl(TEMP_LINE_NUM,0), nvl(AE_HEADER_ID,0) from bec_raw_dl_ext.XLA_DISTRIBUTION_LINKS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(APPLICATION_ID,0), nvl(REF_AE_HEADER_ID,0), nvl(TEMP_LINE_NUM,0), nvl(AE_HEADER_ID,0));

END;