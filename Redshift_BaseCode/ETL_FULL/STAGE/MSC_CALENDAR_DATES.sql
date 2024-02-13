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

drop table if exists bec_ods_stg.MSC_CALENDAR_DATES;

create table bec_ods_stg.MSC_CALENDAR_DATES 
DISTKEY (SR_INSTANCE_ID)
SORTKEY ( SR_INSTANCE_ID, CALENDAR_DATE, CALENDAR_CODE, EXCEPTION_SET_ID, last_update_date)
as 
select
    *
	from
    bec_raw_dl_ext.MSC_CALENDAR_DATES
where
    kca_operation != 'DELETE'
    and (nvl(SR_INSTANCE_ID,0),
	nvl(CALENDAR_DATE,'1900-01-01 00:00:00'),
	nvl(CALENDAR_CODE,'NA'),
	nvl(EXCEPTION_SET_ID,0),
	last_update_date )  
	in (
    select
        nvl(SR_INSTANCE_ID,0) as SR_INSTANCE_ID, 
        nvl(CALENDAR_DATE,'1900-01-01 00:00:00') as CALENDAR_DATE,
        nvl(CALENDAR_CODE,'NA') as CALENDAR_CODE, 
        nvl(EXCEPTION_SET_ID,0) as EXCEPTION_SET_ID,
        max(last_update_date)
    from
        bec_raw_dl_ext.MSC_CALENDAR_DATES
    where
        kca_operation != 'DELETE'
        and nvl(kca_seq_id, '') = ''
    group by
    nvl(SR_INSTANCE_ID,0), nvl(CALENDAR_DATE,'1900-01-01 00:00:00'),nvl(CALENDAR_CODE,'NA'),nvl(EXCEPTION_SET_ID,0) );
end;


