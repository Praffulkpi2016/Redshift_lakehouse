/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.DIM_PO_AP_METRIC;

create table bec_dwh.DIM_PO_AP_METRIC 
	DISTKEY(ORG_ID)
	sortkey (PO_HEADER_ID, PO_LINE_ID, INVOICE_ID, PO_LINE_LOCATION_ID, ORG_ID)
as
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