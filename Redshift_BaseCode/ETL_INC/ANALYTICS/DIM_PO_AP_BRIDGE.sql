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

truncate table bec_dwh.DIM_PO_AP_BRIDGE;

insert into bec_dwh.DIM_PO_AP_BRIDGE
(
	SELECT distinct 
	INV.ORG_ID,
    INV.INVOICE_ID,
	INV.VENDOR_ID,
	INV.VENDOR_SITE_ID,
    PAY.CHECK_ID,
    INV.PO_HEADER_ID,
    INV.PO_LINE_ID,
    INV.PO_RELEASE_ID,
	INV.HOLD_ID,
    INV.PO_LINE_LOCATION_ID,
    INV.RCV_SHIPMENT_LINE_ID,
    INV.PO_DISTRIBUTION_ID,
	INV.LINE_NUMBER,
    INV.INVOICE_NUM,
    PAY.CHECK_NUMBER,
    INV.INVOICE_DATE,
    INV.GL_DATE AS INV_GL_DATE,
    INV.INVOICE_CREATION_DATE,
    INV.INVOICE_LINE_AMOUNT,
    INV.INVOICE_TOTAL_AMOUNT,
    INV.IS_INV_ON_HOLD,
    INV.HOLD_REASON,
    INV.RELEASE_REASON,
    INV.PAYMENT_TERMS,
    SCH_DD.INVOICE_DUE_DATE,
	PAY.INVOICE_PAYMENT_ID,
    PAY.PAYMENT_DATE,
    PAY.CHECK_DATE,
    PAY.CHECK_CREATION_DATE,
    PAY.PAYMENT_METHOD_CODE,
    PAY.AMOUNT_PAID,
	SCH_DD.PAYMENT_NUM,
	INV.INVOICE_TYPE_LOOKUP_CODE,
	RSH.PO_RELEASE_NUMBER,
    RSH.RECEIPT_DATE,
    RSH.RECEIPT_NUMBER,
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
    || '-'|| nvl(INV.INVOICE_ID, 0)
	  ||'-'|| nvl(INV.LINE_NUMBER,0)
	  ||'-'||nvl(PAY.INVOICE_PAYMENT_ID,0)
     ||'-'||nvl(INV.HOLD_ID,0)
      ||'-'||nvl(SCH_DD.PAYMENT_NUM,0)
	  || '-'|| nvl(INV.PO_HEADER_ID, 0)
	  || '-'|| nvl(INV.PO_LINE_ID, 0)
	  || '-'|| nvl(INV.PO_RELEASE_ID, 0)
	  || '-'|| nvl(INV.PO_LINE_LOCATION_ID, 0)
	|| '-'|| nvl(INV.PO_DISTRIBUTION_ID, 0)
	|| '-'|| nvl(PAY.CHECK_ID, 0)
	|| '-'|| nvl(INV.RCV_SHIPMENT_LINE_ID, 0)
	   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	FROM
    (
        SELECT
            API.INVOICE_ID,
            API.ORG_ID,
			API.VENDOR_ID,
			API.VENDOR_SITE_ID,
            API.INVOICE_NUM,
            API.INVOICE_DATE,
            API.GL_DATE,
            API.CREATION_DATE    INVOICE_CREATION_DATE,
            APIL.LINE_NUMBER,
            APIL.AMOUNT          INVOICE_LINE_AMOUNT,
            API.INVOICE_AMOUNT   INVOICE_TOTAL_AMOUNT,
            DECODE(APH.HOLD_REASON, NULL, 'N', DECODE(APH.RELEASE_LOOKUP_CODE, NULL, 'Y', 'N')) IS_INV_ON_HOLD,
            APH.HOLD_REASON,
            APH.HOLD_ID,
            APH.RELEASE_REASON,
            APIL.PO_HEADER_ID,
            APIL.PO_LINE_ID,
            APIL.PO_RELEASE_ID,
            APIL.PO_LINE_LOCATION_ID,
            APIL.RCV_SHIPMENT_LINE_ID,
            APIL.PO_DISTRIBUTION_ID,
            APT.NAME             PAYMENT_TERMS,
			API.INVOICE_TYPE_LOOKUP_CODE
        FROM
            bec_ods.ap_invoices_all        API,
            bec_ods.AP_INVOICE_LINES_ALL   APIL,
            bec_ods.AP_TERMS               APT,
            bec_ods.AP_HOLDS_ALL           APH,
            bec_ods.AP_LOOKUP_CODES        ALC
        WHERE
            API.INVOICE_ID = APIL.INVOICE_ID (+)
            AND API.CANCELLED_DATE IS NULL
            AND API.TERMS_ID = APT.TERM_ID (+)
            AND APIL.INVOICE_ID = APH.INVOICE_ID (+)
            AND APIL.PO_LINE_LOCATION_ID = APH.LINE_LOCATION_ID (+)
            AND APH.HOLD_LOOKUP_CODE = ALC.LOOKUP_CODE (+)
            AND ALC.LOOKUP_TYPE (+) = 'HOLD CODE'
			AND API.IS_DELETED_FLG <> 'Y'
    ) INV,
    (
        SELECT
            INVOICE_ID   SCH_INVO_ID,
            DUE_DATE     INVOICE_DUE_DATE,
            PAYMENT_NUM
        FROM
            bec_ods.AP_PAYMENT_SCHEDULES_ALL
			where 1=1 AND IS_DELETED_FLG <> 'Y'
    ) SCH_DD,
    (
        SELECT
            AIP.INVOICE_ID,APC.CHECK_ID,
            AIP.INVOICE_PAYMENT_ID,
            AIP.CREATION_DATE   PAYMENT_DATE,
            APC.CHECK_DATE,
            APC.CREATION_DATE   CHECK_CREATION_DATE,
            APC.PAYMENT_METHOD_CODE,
            APC.CHECK_NUMBER,
            APC.AMOUNT          AMOUNT_PAID
        FROM
            bec_ods.AP_INVOICE_PAYMENTS_ALL   AIP,
            bec_ods.AP_CHECKS_ALL             APC
        WHERE
            ( AIP.REVERSAL_FLAG IS NULL
              OR AIP.REVERSAL_FLAG = 'N' )
            AND AIP.CHECK_ID = APC.CHECK_ID (+)
			and AIP.IS_DELETED_FLG <> 'Y'
    ) PAY,
	(SELECT RSL.PO_LINE_LOCATION_ID ,POR.PO_RELEASE_ID,
       POR.RELEASE_NUM  PO_RELEASE_NUMBER,
       MAX(RSH.CREATION_DATE) RECEIPT_DATE,
       MAX(RSH.RECEIPT_NUM) RECEIPT_NUMBER
    FROM 
    bec_ods.po_line_locations_all poll,
    bec_ods.RCV_SHIPMENT_LINES         RSL,
    bec_ods.RCV_SHIPMENT_HEADERS       RSH,
    bec_ods.PO_RELEASES_ALL            POR
    WHERE 1=1
    AND   POLL.PO_RELEASE_ID = POR.PO_RELEASE_ID (+)
    AND   POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID (+)
    AND   RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID (+)
	AND POLL.IS_DELETED_FLG <> 'Y'
    GROUP BY RSL.PO_LINE_LOCATION_ID ,
    POR.PO_RELEASE_ID,
    POR.RELEASE_NUM) RSH
	
    WHERE
    1 = 1
    AND INV.INVOICE_ID = PAY.INVOICE_ID (+)
    AND INV.INVOICE_ID = SCH_DD.SCH_INVO_ID (+)
	AND INV.PO_LINE_LOCATION_ID = RSH.PO_LINE_LOCATION_ID(+)
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_ap_bridge'
	and batch_name = 'po';

commit;