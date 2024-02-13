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
  table if exists bec_dwh.FACT_WO_MES_ALERT_HISTORY;
create table bec_dwh.FACT_WO_MES_ALERT_HISTORY diststyle auto sortkey(organization_code) as (
  WITH alerts AS (
    SELECT 
      distinct aah.application_id, 
      aah.alert_id, 
      aah.check_id, 
      aah.last_update_date, 
      aoh.name, 
      aoh.row_number, 
      aoh.value, 
      aah.action_id 
    FROM 
      (
        select 
          * 
        from 
          bec_ods.alr_action_history 
        where 
          is_deleted_flg <> 'Y'
      ) aah, 
      (
        select 
          * 
        from 
          bec_ods.alr_output_history 
        where 
          is_deleted_flg <> 'Y'
      ) aoh 
    where 
      aoh.check_id = aah.check_id 
      AND aah.alert_id in (134021, 136021, 146021) 
      AND aah.action_id in (137181, 159181, 169181) 
      AND aoh.NAME in (
        'WIP_ENTITY_NAME', 'ERROR_EXPLANATION', 
        'ERROR_MESSAGE'
      )
  ) 
  SELECT 
    organization_code, 
    work_order, 
    error_message, 
    error_date, 
    error_end_date, 
    Decode(
      Regexp_Instr(error_message, 'Serial number '), 
      1, 
      Substring(
        error_message, 
        Regexp_Instr(error_message, 'Serial number ')+ 14, 
        Regexp_Instr(error_message, ' ', 1, 3)-(
          Regexp_Instr(error_message, 'Serial number ')+ 14
        )
      ), 
      null
    ) serial_nummber, 
    case when Regexp_Instr(error_message, ':')-1 < 0 then null else Substring(
      error_message, 
      1, 
      Regexp_Instr(error_message, ':')-1
    ) end part_number, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || organization_code organization_code_KEY, 
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
    ) || '-' || nvl(work_order, 'NA') || '-' || nvl(error_message, 'NA') as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM 
    (
      SELECT 
        DISTINCT 'NW1' organization_code, 
        wo.value work_order, 
        err.value error_message, 
        MIN (wo.last_update_date) error_date, 
        MAX (wo.last_update_date) error_end_date 
      FROM 
        alerts wo, 
        alerts err 
      where 
        wo.alert_id in (134021, 136021) 
        AND wo.action_id in (137181, 159181) 
        AND wo.NAME in ('WIP_ENTITY_NAME') 
        AND err.alert_id in (134021, 136021) 
        AND err.action_id in (137181, 159181) 
        AND err.NAME in (
          'ERROR_EXPLANATION', 'ERROR_MESSAGE'
        ) 
        AND wo.check_id = err.check_id 
        and wo.row_number = err.row_number 
      group by 
        wo.value, 
        err.value 
      UNION 
      SELECT 
        DISTINCT 'RF1' organization_code, 
        wo.value work_order, 
        err.value error_message, 
        MIN (wo.last_update_date) error_date, 
        MAX (wo.last_update_date) error_end_date 
      FROM 
        alerts wo, 
        alerts err 
      where 
        wo.alert_id in (146021) 
        AND wo.action_id in (169181) 
        AND wo.NAME in ('WIP_ENTITY_NAME') 
        AND err.alert_id in (146021) 
        AND err.action_id in (169181) 
        AND err.NAME in ('ERROR_EXPLANATION') 
        AND wo.check_id = err.check_id 
        and wo.row_number = err.row_number 
      group by 
        wo.value, 
        err.value
    )
);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_wo_mes_alert_history' 
  and batch_name = 'wip';
commit;