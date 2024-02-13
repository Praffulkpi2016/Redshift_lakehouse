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
  table if exists bec_dwh.fact_sla_details;
create table bec_dwh.fact_sla_details diststyle all sortkey(
  ledger_id, ae_header_id,gl_sl_link_id ) AS 
 (
select
	xah.ledger_id,
	xal.ae_header_id,
	xal.ae_line_num,
	xal.gl_sl_link_id,
	XAH.je_category_name,
	xah.description header_description,
	xal.gl_transfer_mode_code,
	xah.accounting_entry_status_code,
	xah.gl_transfer_status_code,
	xal.party_type_code,
	xal.ACCOUNTING_CLASS_CODE,
	xal.description line_description,
	xal.accounted_cr,
	xal.accounted_dr,
	xah.period_name,
	xte.application_id,
	xte.source_id_int_1,
	xte.entity_id,
	xte.entity_code,
	gir.gl_sl_link_table ,
	gir.je_header_id,
	gir.je_line_num,
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || xah.ledger_id as ledger_id_KEY, 
     (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || xal.ae_header_id as ae_header_id_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || xal.gl_sl_link_id as gl_sl_link_id_KEY ,
	  (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || gir.je_header_id as je_header_id_KEY,
-- audit columns
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
    ) || '-' || nvl(xah.ledger_id, 0)|| '-' || nvl(xal.ae_header_id, 0) || '-' || nvl(xal.gl_sl_link_id, 0)|| '-' || nvl(gir.je_header_id, 0)	as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
 from
	(SELECT * FROM bec_ods.gl_import_references WHERE IS_DELETED_FLG <> 'Y') gir,
	(SELECT * FROM bec_ods.xla_ae_lines WHERE IS_DELETED_FLG <> 'Y') xal,
	(SELECT * FROM bec_ods.xla_ae_headers WHERE IS_DELETED_FLG <> 'Y') xah,
	(SELECT * FROM bec_ods.xla_transaction_entities WHERE IS_DELETED_FLG <> 'Y') xte
where
	gir.gl_sl_link_id = xal.gl_sl_link_id
	and gir.gl_sl_link_table = xal.gl_sl_link_table
	and xal.ae_header_id = xah.ae_header_id
	and xal.application_id = xah.application_id
	and xal.ledger_id = xah.ledger_id
	--and xal.gl_transfer_mode_code = 'S'
	--and xah.accounting_entry_status_code = 'F'
	--and xah.gl_transfer_status_code = 'Y'
	and xte.application_id = xah.application_id
	and xte.entity_id = xah.entity_id
	and xal.application_id = 200
	--and xal.party_type_code = 'S'
	--and xal.creation_date >= '2020-01-01 00:00:00.000'::timestamp
	--and xah.creation_date >= '2020-01-01 00:00:00.000'::timestamp
	--and xte.creation_date >= '2020-01-01 00:00:00.000'::timestamp
	--and gir.creation_date >= '2020-01-01 00:00:00.000'::timestamp

);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_sla_details' 
  and batch_name = 'gl';
commit;
