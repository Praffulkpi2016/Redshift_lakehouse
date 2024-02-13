/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

truncate table bec_ods.BEC_CVMI_CONSUMPTION_VIEW;

INSERT INTO bec_ods.BEC_CVMI_CONSUMPTION_VIEW (
   	consumption_date,
	po_number,
	release_num,
	vendor_id,
	vendor_name,
	vendor_site_code,
	organization_id,
	organization_code,
	organization_name,
	item,
	description,
	consumption_quantity,
	unit_price,
	consumption_value,
	consumption_processed_flag,
	po_distribution_id,
	invoice_id,
	invoice_num,
	creation_date,
	invoice_date,
	invoice_line_amount,
	invoice_amount,
	amount_paid,
	material_cost,
	extended_cost,
	consigned_ppv,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
    	consumption_date,
	po_number,
	release_num,
	vendor_id,
	vendor_name,
	vendor_site_code,
	organization_id,
	organization_code,
	organization_name,
	item,
	description,
	consumption_quantity,
	unit_price,
	consumption_value,
	consumption_processed_flag,
	po_distribution_id,
	invoice_id,
	invoice_num,
	creation_date,
	invoice_date,
	invoice_line_amount,
	invoice_amount,
	amount_paid,
	material_cost,
	extended_cost,
	consigned_ppv,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.BEC_CVMI_CONSUMPTION_VIEW;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bec_cvmi_consumption_view';
	
commit;