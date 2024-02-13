/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts Snapshot.
# File Version: KPI v1.0
*/
begin;

call fact_ascp_horizontal_plan_snp_proc(to_date(getdate(), 'yyyy-mm-dd'));
commit;

update bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP set plan_name_archive_date = 
(
select distinct plans.compile_designator||'-'||FACT_ASCP_HORIZONTAL_PLAN_SNP.snp_date
from BEC_dwh.DIM_ASCP_PLANS PLANS
where FACT_ASCP_HORIZONTAL_PLAN_SNP.plan_id = PLANS.plan_id
and FACT_ASCP_HORIZONTAL_PLAN_SNP.sr_instance_id = PLANS.sr_instance_id
);
commit;
end;


update 
  bec_etl_ctrl.batch_dw_info 
set 
  last_refresh_date = getdate() ,
      load_type = 'I'
where 
  dw_table_name = 'fact_ascp_horizontal_plan_snp' 
  and batch_name = 'ascp';
commit;