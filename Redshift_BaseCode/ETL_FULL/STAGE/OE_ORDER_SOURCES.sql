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

drop table if exists bec_ods_stg.OE_ORDER_SOURCES;

create table bec_ods_stg.OE_ORDER_SOURCES 
	DISTKEY (ORDER_SOURCE_ID)
SORTKEY (ORDER_SOURCE_ID, last_update_date)
	as (
	select
	*
from
	bec_raw_dl_ext.OE_ORDER_SOURCES
where
	kca_operation != 'DELETE'
	and 
( nvl(ORDER_SOURCE_ID, 0) ,
	last_update_date)
in (
	select
		nvl(ORDER_SOURCE_ID, 0) as ORDER_SOURCE_ID ,
		max(last_update_date)
	from
		bec_raw_dl_ext.OE_ORDER_SOURCES
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		ORDER_SOURCE_ID
)
);
end;