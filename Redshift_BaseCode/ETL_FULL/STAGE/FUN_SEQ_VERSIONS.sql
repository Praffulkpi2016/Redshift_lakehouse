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
begin;

drop table if exists bec_ods_stg.FUN_SEQ_VERSIONS;

create table bec_ods_stg.FUN_SEQ_VERSIONS 
DISTKEY (SEQ_VERSION_ID)
SORTKEY ( SEQ_VERSION_ID, last_update_date, kca_seq_id)
as 
select
	*
from
	bec_raw_dl_ext.FUN_SEQ_VERSIONS
where
	kca_operation != 'DELETE'
	and (SEQ_VERSION_ID,last_update_date,kca_seq_id) in 
	(
	select
		SEQ_VERSION_ID,max(last_update_date),max(kca_seq_id)
	from
		bec_raw_dl_ext.FUN_SEQ_VERSIONS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		SEQ_VERSION_ID
	);
end;