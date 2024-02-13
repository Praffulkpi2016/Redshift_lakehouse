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

drop table if exists bec_ods_stg.RA_CUST_TRX_TYPES_ALL;

create table bec_ods_stg.RA_CUST_TRX_TYPES_ALL 
DISTKEY (CUST_TRX_TYPE_ID)
SORTKEY (CUST_TRX_TYPE_ID, org_id, last_update_date)
as 
select
	*
	from
	bec_raw_dl_ext.RA_CUST_TRX_TYPES_ALL
where
	kca_operation != 'DELETE'
	and (CUST_TRX_TYPE_ID,
	nvl(org_id, 0),
	--zd_edition_name,
	last_update_date) in 
(
	select
		CUST_TRX_TYPE_ID,
		nvl(org_id, 0) as org_id,
--		zd_edition_name,
		max(last_update_date)
	from
		bec_raw_dl_ext.RA_CUST_TRX_TYPES_ALL
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		CUST_TRX_TYPE_ID,
		org_id
--,zd_edition_name
);
end;

