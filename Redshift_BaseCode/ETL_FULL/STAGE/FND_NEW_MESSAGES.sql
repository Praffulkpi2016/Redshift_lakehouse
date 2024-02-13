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

DROP TABLE IF EXISTS bec_ods_stg.fnd_new_messages;

CREATE TABLE bec_ods_stg.fnd_new_messages 
DISTKEY (MESSAGE_NUMBER)
SORTKEY ( APPLICATION_ID, MESSAGE_NAME, LANGUAGE_CODE, MESSAGE_NUMBER, ZD_EDITION_NAME, last_update_date)
AS 
SELECT * FROM bec_raw_dl_ext.fnd_new_messages
where kca_operation != 'DELETE' 
and (nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'),last_update_date) in 
(select nvl(APPLICATION_ID,0)  as APPLICATION_ID,
		nvl(MESSAGE_NAME,'NA') as MESSAGE_NAME,
		nvl(LANGUAGE_CODE,'NA') as LANGUAGE_CODE,
		nvl(MESSAGE_NUMBER,0) as MESSAGE_NUMBER,
		nvl(ZD_EDITION_NAME,'NA') as ZD_EDITION_NAME,
		max(last_update_date) from bec_raw_dl_ext.fnd_new_messages 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
group by nvl(APPLICATION_ID,0),nvl(MESSAGE_NAME,'NA'),nvl(LANGUAGE_CODE,'NA'),nvl(MESSAGE_NUMBER,0),nvl(ZD_EDITION_NAME,'NA'));

END;