/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;
-- Delete Records
delete from 
  bec_dwh.fact_sla_details 
where 
  (
	NVL(ledger_id, 0),NVL(ae_header_id, 0),NVL(gl_sl_link_id, 0)
	,NVL(je_header_id, 0)
  ) in (
    select 
      nvl(ods.ledger_id, 0) as ledger_id, 
      nvl(ods.ae_header_id, 0) as ae_header_id, 
      nvl(ods.gl_sl_link_id, 0) as gl_sl_link_id, 
      nvl(ods.je_header_id, 0) as je_header_id 
    from 
      bec_dwh.fact_sla_details dw, 
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
			gir.gl_sl_link_table,
			gir.je_header_id,
			gir.je_line_num
		from
			bec_ods.gl_import_references gir,
			bec_ods.xla_ae_lines xal,
			bec_ods.xla_ae_headers xah,
			bec_ods.xla_transaction_entities xte
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
             AND (
                    gir.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    ) 		   
			OR		XAL.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    )
			OR		xah.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    )					
			OR  	xte.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    )
			OR  	gir.IS_DELETED_FLG = 'Y'
			OR  	xal.IS_DELETED_FLG = 'Y'
			OR  	xah.IS_DELETED_FLG = 'Y'
			OR  	xte.IS_DELETED_FLG = 'Y'			
			)
      ) ods 
    where 
      dw.dw_load_id = (
        select 
          system_id 
        from 
          bec_etl_ctrl.etlsourceappid 
        where 
          source_system = 'EBS'
      ) || '-' || nvl(ODS.ledger_id, 0) || '-' || nvl(ODS.ae_header_id, 0)|| '-' || nvl(ODS.gl_sl_link_id, 0)
	|| '-' || nvl(ODS.je_header_id, 0)
	);
commit;
-- Insert records
insert into bec_dwh.fact_sla_details (
	ledger_id,
	ae_header_id,
	ae_line_num,
	gl_sl_link_id,
	je_category_name,
	header_description,
	gl_transfer_mode_code,
	accounting_entry_status_code,
	gl_transfer_status_code,
	party_type_code,
	accounting_class_code,
	line_description,
	accounted_cr,
	accounted_dr,
	period_name,
	application_id,
	source_id_int_1,
	entity_id,
	entity_code,
	gl_sl_link_table,
	je_header_id,
	je_line_num,
	ledger_id_key,
	ae_header_id_key,
	gl_sl_link_id_key,
	je_header_id_key,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
) (
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
            AND (
                    gir.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    ) 		   
			OR		XAL.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    )
			OR		xah.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    )					
			OR  	xte.kca_seq_date >= (
                      select 
                        (executebegints - prune_days) 
                      from 
                        bec_etl_ctrl.batch_dw_info 
                      where 
                        dw_table_name = 'fact_sla_details' 
                        and batch_name = 'gl'
                    )

			)
);

commit;

end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_sla_details' 
  and batch_name = 'gl';
commit;
