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
--delete 
delete from bec_dwh.DIM_PO_AP_METRIC
where exists 
(
select 1
from 
bec_ods.AP_INVOICES_ALL API,
bec_ods.AP_INVOICE_LINES_ALL APIL,
bec_ods.AP_INVOICE_PAYMENTS_ALL AIP
WHERE 1=1
AND (APIL.kca_seq_date > (
            SELECT (executebegints - prune_days) 
            FROM bec_etl_ctrl.batch_dw_info 
            WHERE dw_table_name = 'dim_po_ap_metric' 
              AND batch_name = 'po'
        )
or 
API.kca_seq_date > (
            SELECT (executebegints - prune_days) 
            FROM bec_etl_ctrl.batch_dw_info 
            WHERE dw_table_name = 'dim_po_ap_metric' 
              AND batch_name = 'po'
        ))
AND API.CANCELLED_DATE IS NULL
AND APIL.PO_HEADER_ID IS NOT NULL
AND APIL.CANCELLED_FLAG = 'N'
AND API.INVOICE_ID = APIL.INVOICE_ID 
AND API.ORG_ID = APIL.ORG_ID 
AND API.INVOICE_ID = AIP.INVOICE_ID (+)
and nvl(DIM_PO_AP_METRIC.ORG_ID,0) = nvl(API.ORG_ID,0)
and nvl(DIM_PO_AP_METRIC.INVOICE_ID,0) = nvl(API.INVOICE_ID,0)
and nvl(DIM_PO_AP_METRIC.PO_LINE_ID,0) = nvl(APIL.PO_LINE_ID,0)
and nvl(DIM_PO_AP_METRIC.PO_LINE_LOCATION_ID,0) = nvl(APIL.PO_LINE_LOCATION_ID,0)
);
commit;
--insert 
insert into  bec_dwh.DIM_PO_AP_METRIC 
  SELECT
    PO_HEADER_ID,
    PO_LINE_ID,
    PO_LINE_LOCATION_ID,
    INVOICE_ID,
    ORG_ID,
    SUM(INVOICE_AMOUNT) INVOICE_AMOUNT,
    SUM(AMOUNT_PAID) AMOUNT_PAID,
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
	|| '-'|| nvl(ORG_ID, 0)
    || '-'|| nvl(INVOICE_ID, 0)
	|| '-'|| nvl(PO_LINE_ID, 0)
	|| '-'|| nvl(PO_LINE_LOCATION_ID, 0)
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
    (
        SELECT
            APIL.PO_LINE_LOCATION_ID,
            APIL.PO_HEADER_ID,
            APIL.PO_LINE_ID,
            API.INVOICE_ID,
            API.ORG_ID,
            SUM(APIL.AMOUNT) INVOICE_AMOUNT,
            DECODE(DECODE(AIP.AMOUNT, NULL, 'N', 0, 'N','Y'), 'Y', SUM(APIL.AMOUNT), 0) AMOUNT_PAID
        FROM
         bec_ods.AP_INVOICES_ALL API,
         bec_ods.AP_INVOICE_LINES_ALL APIL,
         bec_ods.AP_INVOICE_PAYMENTS_ALL AIP
        WHERE 1=1
AND (APIL.kca_seq_date > (
            SELECT (executebegints - prune_days) 
            FROM bec_etl_ctrl.batch_dw_info 
            WHERE dw_table_name = 'dim_po_ap_metric' 
              AND batch_name = 'po'
        )
or 
API.kca_seq_date > (
            SELECT (executebegints - prune_days) 
            FROM bec_etl_ctrl.batch_dw_info 
            WHERE dw_table_name = 'dim_po_ap_metric' 
              AND batch_name = 'po'
        ))
		    AND API.CANCELLED_DATE IS NULL
            AND APIL.PO_HEADER_ID IS NOT NULL
            AND APIL.CANCELLED_FLAG = 'N'
            AND API.INVOICE_ID = APIL.INVOICE_ID 
            AND API.ORG_ID = APIL.ORG_ID 
            AND API.INVOICE_ID = AIP.INVOICE_ID (+)
        GROUP BY
            APIL.PO_LINE_LOCATION_ID,
            APIL.PO_HEADER_ID,
            APIL.PO_LINE_ID,
            API.INVOICE_ID,
            API.ORG_ID,
            AIP.AMOUNT
    )
GROUP BY
    PO_HEADER_ID,
    PO_LINE_ID,
    INVOICE_ID,
    PO_LINE_LOCATION_ID,
    ORG_ID
;
--soft DELETE
update  bec_dwh.DIM_PO_AP_METRIC set is_deleted_flg = 'Y'
where not exists 
(
select 1
from 
bec_ods.AP_INVOICES_ALL API,
bec_ods.AP_INVOICE_LINES_ALL APIL,
bec_ods.AP_INVOICE_PAYMENTS_ALL AIP
WHERE 1=1
AND (API.is_deleted_flg <> 'Y' or APIL.is_deleted_flg <> 'Y')
AND API.CANCELLED_DATE IS NULL
AND APIL.PO_HEADER_ID IS NOT NULL
AND APIL.CANCELLED_FLAG = 'N'
AND API.INVOICE_ID = APIL.INVOICE_ID 
AND API.ORG_ID = APIL.ORG_ID 
AND API.INVOICE_ID = AIP.INVOICE_ID (+)
and nvl(DIM_PO_AP_METRIC.ORG_ID,0) = nvl(API.ORG_ID,0)
and nvl(DIM_PO_AP_METRIC.INVOICE_ID,0) = nvl(API.INVOICE_ID,0)
and nvl(DIM_PO_AP_METRIC.PO_LINE_ID,0) = nvl(APIL.PO_LINE_ID,0)
and nvl(DIM_PO_AP_METRIC.PO_LINE_LOCATION_ID,0) = nvl(APIL.PO_LINE_LOCATION_ID,0)
);
end;



update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_ap_metric'
	and batch_name = 'po';

commit;