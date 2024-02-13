/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.wf_item_types;

insert into	bec_ods_stg.wf_item_types
   (
	NAME,
	PROTECT_LEVEL,
	CUSTOM_LEVEL,
	WF_SELECTOR,
	READ_ROLE,
	WRITE_ROLE,
	EXECUTE_ROLE,
	PERSISTENCE_TYPE,
	PERSISTENCE_DAYS,
	SECURITY_GROUP_ID,
	NUM_ACTIVE,
	NUM_ERROR,
	NUM_DEFER,
	NUM_SUSPEND,
	NUM_COMPLETE,
	NUM_PURGEABLE,
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE
	)
(
	select
	NAME,
	PROTECT_LEVEL,
	CUSTOM_LEVEL,
	WF_SELECTOR,
	READ_ROLE,
	WRITE_ROLE,
	EXECUTE_ROLE,
	PERSISTENCE_TYPE,
	PERSISTENCE_DAYS,
	SECURITY_GROUP_ID,
	NUM_ACTIVE,
	NUM_ERROR,
	NUM_DEFER,
	NUM_SUSPEND,
	NUM_COMPLETE,
	NUM_PURGEABLE,
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.wf_item_types
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(NAME,'NA'),kca_seq_id) in 
	(select nvl(NAME,'NA') as NAME,max(kca_seq_id) 
from bec_raw_dl_ext.wf_item_types 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(NAME,'NA'))
        and	KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wf_item_types')

);
end;


