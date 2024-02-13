/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;
drop 
  table if exists bec_dwh.DIM_JTF_RESOURCE_GROUPS;
create table bec_dwh.DIM_JTF_RESOURCE_GROUPS diststyle all SORTKEY(owner_id, owner_type_code)
 as 
SELECT 
  owner_type_code, 
  owner_id, 
  owner_name, 
  descripton, 
  owner_number, 
  email, 
  created_by, 
  creation_date, 
  last_updated_by, 
  last_update_date, 
  last_update_login, 
  start_date_active, 
  end_date_active, 
  'N' as is_deleted_flg, 
  (
    select 
      system_id 
    from 
      bec_etl_ctrl.etlsourceappid 
    where 
      source_system = 'EBS'
  ) as source_app_id, 
  (
    select 
      system_id 
    from 
      bec_etl_ctrl.etlsourceappid 
    where 
      source_system = 'EBS'
  ) || '-' || nvl(owner_id, 0) || '-' || nvl(owner_type_code, 'NA') as dw_load_id, 
  getdate() as dw_insert_date, 
  getdate() as dw_update_date 
FROM 
  (
    SELECT 
      'RS_GROUP' owner_type_code, 
      b.group_id as owner_id, 
      t.group_name owner_name, 
      t.group_desc descripton, 
      b.group_number as owner_number, 
      b.email_address as email, 
      b.created_by, 
      b.creation_date, 
      b.last_updated_by, 
      b.last_update_date, 
      b.last_update_login, 
      b.start_date_active, 
      b.end_date_active 
    FROM 
      bec_ods.jtf_rs_groups_tl t, 
      bec_ods.jtf_rs_groups_b b 
    WHERE 
      b.group_id = t.group_id 
      AND t.language = 'US' 
    UNION ALL 
    SELECT 
      'RS_EMPLOYEE' owner_type_code, 
      b.resource_id as owner_id, 
      t.resource_name owner_name, 
      null descripton, 
      b.resource_number as owner_number, 
      b.source_email as email, 
      b.created_by, 
      b.creation_date, 
      b.last_updated_by, 
      b.last_update_date, 
      b.last_update_login, 
      b.start_date_active, 
      b.end_date_active 
    FROM 
      bec_ods.jtf_rs_resource_extns b, 
      bec_ods.jtf_rs_resource_extns_tl t 
    WHERE 
      t.language = 'US' 
      AND b.resource_id = t.resource_id 
      AND b.category = t.category
  );
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'dim_jtf_resource_groups' 
  and batch_name = 'inv';
commit;