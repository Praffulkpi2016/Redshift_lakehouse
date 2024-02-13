/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.dim_ap_invoice
where INVOICE_ID in (
select ods.INVOICE_ID from bec_dwh.dim_ap_invoice dw, bec_ods.AP_INVOICES_ALL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.INVOICE_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_invoice' and batch_name = 'ap')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_AP_INVOICE
(
	INVOICE_ID,
	INVOICE_NUM,
	INVOICE_CURRENCY_CODE,
	PAYMENT_CURRENCY_CODE,
	INVOICE_DATE,
	SOURCE,
	INVOICE_TYPE_LOOKUP_CODE,
	DESCRIPTION,
	TAX_AMOUNT,
	PAYMENT_STATUS_FLAG,
	INVOICE_RECEIVED_DATE,
	CREATION_DATE,
	LAST_UPDATE_DATE,
	ORG_ID,
	GL_DATE,
	REMIT_TO_SUPPLIER_NAME,
	REMIT_TO_SUPPLIER_SITE,
	PAYMENT_METHOD_LOOKUP_CODE,
	PAY_GROUP_LOOKUP_CODE,
	ACCTS_PAY_CODE_COMBINATION_ID,
	PREPAY_FLAG,
	EXPENDITURE_TYPE,
	CREATED_BY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
	INVOICE_ID,
	INVOICE_NUM,
	INVOICE_CURRENCY_CODE,
	PAYMENT_CURRENCY_CODE,
	INVOICE_DATE,
	SOURCE,
	INVOICE_TYPE_LOOKUP_CODE,
	DESCRIPTION,
	TAX_AMOUNT,
	PAYMENT_STATUS_FLAG,
	INVOICE_RECEIVED_DATE,
	CREATION_DATE,
	LAST_UPDATE_DATE,
	ORG_ID,
	GL_DATE,
	REMIT_TO_SUPPLIER_NAME,
	REMIT_TO_SUPPLIER_SITE,
	PAYMENT_METHOD_CODE as PAYMENT_METHOD_LOOKUP_CODE,
	PAY_GROUP_LOOKUP_CODE,
	ACCTS_PAY_CODE_COMBINATION_ID,
	PREPAY_FLAG,
	EXPENDITURE_TYPE,
	CREATED_BY,
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
    )
    || '-'
       || nvl(INVOICE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.AP_INVOICES_ALL
Where 1=1 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_invoice' and batch_name = 'ap')
 )
);

commit;
-- Soft delete

update bec_dwh.dim_ap_invoice set is_deleted_flg = 'Y'
where INVOICE_ID not in (
select ods.INVOICE_ID from bec_dwh.dim_ap_invoice dw, bec_ods.AP_INVOICES_ALL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.INVOICE_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_invoice' and batch_name = 'ap';

COMMIT;