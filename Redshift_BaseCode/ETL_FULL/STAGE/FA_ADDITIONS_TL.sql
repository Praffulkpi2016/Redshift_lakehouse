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

DROP TABLE IF EXISTS bec_ods_stg.fa_additions_tl;

CREATE TABLE bec_ods_stg.fa_additions_tl 
DISTKEY (ASSET_ID)
SORTKEY (ASSET_ID, LANGUAGE, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.fa_additions_tl
where kca_operation != 'DELETE' 
and (nvl(ASSET_ID,0),nvl(LANGUAGE,'NA'),last_update_date) in 
(select nvl(ASSET_ID,0) as ASSET_ID,nvl(LANGUAGE,'NA') as LANGUAGE,max(last_update_date) from bec_raw_dl_ext.fa_additions_tl 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(ASSET_ID,0),nvl(LANGUAGE,'NA'));

END;