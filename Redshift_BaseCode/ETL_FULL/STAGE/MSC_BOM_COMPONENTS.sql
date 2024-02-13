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

drop table if exists bec_ods_stg.MSC_BOM_COMPONENTS;

create table bec_ods_stg.MSC_BOM_COMPONENTS 
DISTKEY (COMPONENT_SEQUENCE_ID)
SORTKEY (PLAN_ID, COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID, last_update_date)
as 
select
    *
	from
    bec_raw_dl_ext.MSC_BOM_COMPONENTS
where
    kca_operation != 'DELETE'
    and (nvl(PLAN_ID,0),nvl(COMPONENT_SEQUENCE_ID,0),nvl(SR_INSTANCE_ID,0),nvl(BILL_SEQUENCE_ID,0),last_update_date )  in (
    select
        nvl(PLAN_ID,0) as PLAN_ID, 
        nvl(COMPONENT_SEQUENCE_ID,0) as COMPONENT_SEQUENCE_ID,
        nvl(SR_INSTANCE_ID,0) as SR_INSTANCE_ID, 
        nvl(BILL_SEQUENCE_ID,0) as BILL_SEQUENCE_ID,
        max(last_update_date)
    from
        bec_raw_dl_ext.MSC_BOM_COMPONENTS
    where
        kca_operation != 'DELETE'
        and nvl(kca_seq_id, '') = ''
    group by
    nvl(PLAN_ID,0), nvl(COMPONENT_SEQUENCE_ID,0),nvl(SR_INSTANCE_ID,0),nvl(BILL_SEQUENCE_ID,0) );
end;