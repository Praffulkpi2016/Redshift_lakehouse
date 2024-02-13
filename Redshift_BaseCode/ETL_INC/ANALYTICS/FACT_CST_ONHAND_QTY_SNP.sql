/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts Snapshot.
# File Version: KPI v1.0
*/
begin;

call fact_cst_onhand_qty_snp_proc(to_date(getdate(), 'yyyy-mm-dd'));
commit;
end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cst_onhand_qty_snp' 
  and batch_name = 'costing';
commit;
