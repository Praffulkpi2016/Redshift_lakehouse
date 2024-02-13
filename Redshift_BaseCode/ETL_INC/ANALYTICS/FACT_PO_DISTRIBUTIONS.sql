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
DELETE FROM bec_dwh.FACT_PO_DISTRIBUTIONS
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.PO_DISTRIBUTIONS_ALL D
    LEFT JOIN bec_ods.po_headers_all H ON D.PO_HEADER_ID = H.PO_HEADER_ID
    LEFT JOIN bec_ods.PO_LINES_ALL P ON D.PO_LINE_ID = P.PO_LINE_ID
    LEFT JOIN bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL F ON P.ORG_ID = F.ORG_ID
    WHERE NVL(D.PO_DISTRIBUTION_ID, 0) = NVL(FACT_PO_DISTRIBUTIONS.PO_DISTRIBUTION_ID, 0)
    AND NVL(D.PO_HEADER_ID, 0) = NVL(FACT_PO_DISTRIBUTIONS.PO_HEADER_ID, 0)
    AND NVL(D.PO_LINE_ID, 0) = NVL(FACT_PO_DISTRIBUTIONS.PO_LINE_ID, 0)
    AND NVL(F.SET_OF_BOOKS_ID, 0) = NVL(FACT_PO_DISTRIBUTIONS.SET_OF_BOOKS_ID, 0)
    AND NVL(D.ORG_ID, 0) = NVL(FACT_PO_DISTRIBUTIONS.ORG_ID, 0)
    AND (
        D.kca_seq_date > (
            SELECT (executebegints - prune_days)
            FROM bec_etl_ctrl.batch_dw_info
            WHERE dw_table_name = 'fact_po_distributions'
            AND batch_name = 'po'
        )
        OR F.kca_seq_date > (
            SELECT (executebegints - prune_days)
            FROM bec_etl_ctrl.batch_dw_info
            WHERE dw_table_name = 'fact_po_distributions'
            AND batch_name = 'po'
        )
    )
)
;
commit;
-- Insert records
insert into bec_dwh.FACT_PO_DISTRIBUTIONS 
(
 po_distribution_id_key,
	po_distribution_id,
	po_header_id_key,
	po_header_id,
	po_line_id_key,
	po_line_id,
	line_location_id_key,
	line_location_id,
	po_release_id_key,
	po_release_id,
	dist_creation_date,
	code_combination_id_key,
	code_combination_id,
	quantity_billed,
	quantity_cancelled,
	quantity_delivered,
	quantity_financed,
	quantity_ordered,
	quantity_recouped,
	amount_billed,
	amount_cancelled,
	amount_delivered,
	amount_financed,
	amount_ordered,
	amount_recouped,
	amount_to_encumber,
	encumbered_flag,
	encumbered_amount,
	distribution_num,
	distribution_type,
	project_id_key,
	project_id,
	task_id_key,
	task_id,
	award_id,
	expenditure_item_date,
	expenditure_type,
	expenditure_organization_id,
	org_id_key,
	org_id,
	last_update_date,
	line_num,
	category_id_key,
	category_id,
	inventory_item_id,
	list_price_per_unit,
	unit_price,
	inventory_organization_id_key,
	inventory_organization_id,
	type_lookup_code,
	agent_id_key,
	agent_id,
	po_number,
	currency_code,
	po_date,
	vendor_site_id_key,
	vendor_site_id,
	SET_OF_BOOKS_ID,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date)
select
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.PO_DISTRIBUTION_ID as PO_DISTRIBUTION_ID_KEY,
	D.PO_DISTRIBUTION_ID,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.PO_HEADER_ID as PO_HEADER_ID_KEY,	
  D.PO_HEADER_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.PO_LINE_ID as PO_LINE_ID_KEY,
  D.PO_LINE_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.LINE_LOCATION_ID as LINE_LOCATION_ID_KEY,
  D.LINE_LOCATION_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.PO_RELEASE_ID as PO_RELEASE_ID_KEY,
  D.PO_RELEASE_ID,
  D.CREATION_DATE "DIST_CREATION_DATE",
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY,
  D.CODE_COMBINATION_ID ,
  D.QUANTITY_BILLED ,
  D.QUANTITY_CANCELLED ,
  D.QUANTITY_DELIVERED ,
  D.QUANTITY_FINANCED ,
  --D.QUANTITY_FUNDED ,
  D.QUANTITY_ORDERED ,
  D.QUANTITY_RECOUPED,
  D.AMOUNT_BILLED ,
  D.AMOUNT_CANCELLED ,
  --D.AMOUNT_ADJUSTED_FLAG ,
  D.AMOUNT_DELIVERED ,
  D.AMOUNT_FINANCED ,
  --D.AMOUNT_FUNDED ,
  D.AMOUNT_ORDERED ,
  D.AMOUNT_RECOUPED ,
  D.AMOUNT_TO_ENCUMBER ,
  D.ENCUMBERED_FLAG,
  D.ENCUMBERED_AMOUNT,
  D.DISTRIBUTION_NUM,
  D.DISTRIBUTION_TYPE,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.PROJECT_ID as PROJECT_ID_KEY,
  D.PROJECT_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.TASK_ID as TASK_ID_KEY,
  D.TASK_ID,
  D.AWARD_ID,
  D.EXPENDITURE_ITEM_DATE,
  D.EXPENDITURE_TYPE ,
  D.EXPENDITURE_ORGANIZATION_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||D.ORG_ID as ORG_ID_KEY,
  D.ORG_ID,
  D.LAST_UPDATE_DATE,
  P.LINE_NUM,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||P.CATEGORY_ID as CATEGORY_ID_KEY,
  P.CATEGORY_ID,
  P.ITEM_ID "INVENTORY_ITEM_ID",
  P.LIST_PRICE_PER_UNIT,
  P.UNIT_PRICE,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||F.INVENTORY_ORGANIZATION_ID as INVENTORY_ORGANIZATION_ID_KEY,
  F.INVENTORY_ORGANIZATION_ID ,
  H.TYPE_LOOKUP_CODE,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||H.AGENT_ID as AGENT_ID_KEY,
  H.AGENT_ID,
  H.SEGMENT1 "PO_NUMBER",
  H.CURRENCY_CODE,
  H.CREATION_DATE "PO_DATE",
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||H.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
  H.VENDOR_SITE_ID,
  F.SET_OF_BOOKS_ID,
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
    || '-' || nvl(D.PO_DISTRIBUTION_ID, 0)
	|| '-' || nvl(D.PO_HEADER_ID, 0)
	|| '-' || nvl(D.PO_LINE_ID, 0)	
	|| '-' || nvl(F.SET_OF_BOOKS_ID, 0)
	|| '-' || nvl(D.ORG_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM (select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg<>'Y') D,
	 (select * from bec_ods.po_headers_all where is_deleted_flg<>'Y') H,
	 (select * from bec_ods.PO_LINES_ALL where is_deleted_flg<>'Y') P,
	 (select * from bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL where is_deleted_flg<>'Y') F
WHERE D.PO_HEADER_ID = H.PO_HEADER_ID(+)
AND D.PO_LINE_ID     = P.PO_LINE_ID(+)
AND P.ORG_ID         = F.ORG_ID(+)
   AND (
        D.kca_seq_date > (
            SELECT (executebegints - prune_days)
            FROM bec_etl_ctrl.batch_dw_info
            WHERE dw_table_name = 'fact_po_distributions'
            AND batch_name = 'po'
        )
        OR F.kca_seq_date > (
            SELECT (executebegints - prune_days)
            FROM bec_etl_ctrl.batch_dw_info
            WHERE dw_table_name = 'fact_po_distributions'
            AND batch_name = 'po'
        )
    );
commit;
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_po_distributions'
	and batch_name = 'po';

COMMIT;