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

drop table if exists bec_ods_stg.AP_INV_APRVL_HIST_ALL   ;

create table bec_ods_stg.AP_INV_APRVL_HIST_ALL  
	DISTKEY (APPROVAL_HISTORY_ID)
SORTKEY (APPROVAL_HISTORY_ID, LAST_UPDATE_DATE)
	as (
	select
	*
from
	bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
where
	kca_operation != 'DELETE'
	and
(	nvl(APPROVAL_HISTORY_ID, '0'),

	last_update_date)
in (
	select
	    nvl(APPROVAL_HISTORY_ID, '0') as APPROVAL_HISTORY_ID,

		max(last_update_date)
	from
		bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(APPROVAL_HISTORY_ID, '0')
)
);
end;