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
delete from bec_dwh.FACT_PO_PAYMENT_HIST 
where (
 nvl(PO_HEADER_ID, 0) 
,nvl(PO_LINE_ID, 0) 
,nvl(LINE_LOCATION_ID, 0) 
,nvl(SHIPMENT_LINE_ID, 0) 
,nvl(CODE_COMBINATION_ID,0) 
,nvl(RECEIPT_ENTERED_AS_DATE, '1900-01-01') 
,nvl(receipt_number, 'NA') 

) in
(
SELECT
nvl(TMP.PO_HEADER_ID, 0) as PO_HEADER_ID
,nvl(TMP.PO_LINE_ID, 0) as PO_LINE_ID
,nvl(TMP.LINE_LOCATION_ID, 0) as LINE_LOCATION_ID
,nvl(TMP.SHIPMENT_LINE_ID, 0) as SHIPMENT_LINE_ID
,nvl(TMP.CODE_COMBINATION_ID,0) as CODE_COMBINATION_ID
,nvl(TMP.RECEIPT_ENTERED_AS_DATE, '1900-01-01') as RECEIPT_ENTERED_AS_DATE
,nvl(TMP.receipt_number, 'NA')  AS RECEIPT_NUMBER
FROM bec_dwh.FACT_PO_PAYMENT_HIST  dw,
(
SELECT
    POF.PO_HEADER_ID,
    POF.PO_LINE_ID,
    POF.LINE_LOCATION_ID,
    POF.RECEIPT_ENTERED_AS_DATE,
    POF.SHIPMENT_LINE_ID,
    POF.CODE_COMBINATION_ID,
	POF.RECEIPT_NUMBER
    
FROM
    (
        SELECT 
            POH.PO_HEADER_ID,
            POL.PO_LINE_ID,
            POLL.LINE_LOCATION_ID,		
            RT.TRANSACTION_DATE         RECEIPT_ENTERED_AS_DATE,
            RSL.SHIPMENT_LINE_ID,
            POD.CODE_COMBINATION_ID		,
			RSH.RECEIPT_NUM             RECEIPT_NUMBER
         FROM
            bec_ods.PO_HEADERS_ALL              POH,
            bec_ods.PO_LINES_ALL                POL,
            bec_ods.PO_LINE_LOCATIONS_ALL       POLL,
            bec_ods.RCV_SHIPMENT_LINES          RSL,
            bec_ods.RCV_SHIPMENT_HEADERS        RSH,
            bec_ods.RCV_TRANSACTIONS            RT,
            bec_ods.PO_LINE_TYPES_tl            PLT,
            bec_ods.PO_RELEASES_ALL             PORL,
            bec_ods.PO_DISTRIBUTIONS_ALL        POD
        WHERE
            1 = 1
            AND POH.PO_HEADER_ID = POL.PO_HEADER_ID (+)
            AND POL.PO_LINE_ID = POLL.PO_LINE_ID (+)
            AND POLL.PO_LINE_ID = RSL.PO_LINE_ID (+)
            AND POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID (+)
            AND RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID (+)
            AND RSL.SHIPMENT_LINE_ID = RT.SHIPMENT_LINE_ID (+)
            AND RSL.PO_RELEASE_ID = RT.PO_RELEASE_ID (+)
            AND RSL.PO_LINE_LOCATION_ID = RT.PO_LINE_LOCATION_ID (+)
            AND RT.DESTINATION_TYPE_CODE (+) = 'INVENTORY'
            AND POLL.PO_RELEASE_ID = PORL.PO_RELEASE_ID (+)
            AND POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID (+)
--            AND POH.ORG_ID = 4534
            AND POH.TYPE_LOOKUP_CODE = 'BLANKET'
            AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID (+)
            AND POLL.PO_RELEASE_ID = POD.PO_RELEASE_ID (+)
            AND NVL(plt.language, 'US') = 'US'
			and (
			nvl(POD.kca_seq_date) > (select (executebegints-prune_days) 
					 from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(RSL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
         OR
		 nvl(POLL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 
		 OR
		 nvl(POL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(POH.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		or POH.is_deleted_flg = 'Y'
		or POL.is_deleted_flg = 'Y'
		or POLL.is_deleted_flg = 'Y'
		or RSL.is_deleted_flg = 'Y'
		or RSH.is_deleted_flg = 'Y'
		or RT.is_deleted_flg = 'Y'
		or PLT.is_deleted_flg = 'Y'
		or PORL.is_deleted_flg = 'Y'
		or POD.is_deleted_flg = 'Y'
		  )
        UNION
        SELECT
            POH.PO_HEADER_ID,
            POL.PO_LINE_ID,
            POLL.LINE_LOCATION_ID,			
            RT.TRANSACTION_DATE         RECEIPT_ENTERED_AS_DATE,
            RSL.SHIPMENT_LINE_ID,
            POD.CODE_COMBINATION_ID	,
			RSH.RECEIPT_NUM             RECEIPT_NUMBER			
        FROM
           bec_ods.PO_HEADERS_ALL             POH,
           bec_ods.PO_LINES_ALL               POL,
           bec_ods.PO_LINE_LOCATIONS_ALL      POLL,
           bec_ods.RCV_SHIPMENT_LINES         RSL,
           bec_ods.RCV_TRANSACTIONS           RT,
           bec_ods.RCV_SHIPMENT_HEADERS       RSH,
			bec_ods.PO_LINE_TYPES_tl           PLT,
           bec_ods.PO_DISTRIBUTIONS_ALL       POD
        WHERE
            1 = 1
            AND POH.PO_HEADER_ID = POL.PO_HEADER_ID (+)
            AND POL.PO_LINE_ID = POLL.PO_LINE_ID (+)
            AND POLL.PO_LINE_ID = RSL.PO_LINE_ID (+)
            AND POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID (+)
            AND RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID (+)
            AND RSL.SHIPMENT_LINE_ID = RT.SHIPMENT_LINE_ID (+)
            AND RSL.PO_LINE_LOCATION_ID = RT.PO_LINE_LOCATION_ID (+)
            AND RSL.PO_LINE_ID = RT.PO_LINE_ID (+)
            AND RT.DESTINATION_TYPE_CODE (+) = 'INVENTORY'
            AND POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID (+)
--            AND POH.ORG_ID = 4534
            AND POH.TYPE_LOOKUP_CODE = 'STANDARD'
            AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID (+)
            AND NVL(plt.language, 'US') = 'US'
			and (nvl(POD.kca_seq_date) > (select (executebegints-prune_days) 
					 from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(RSL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
         OR
		 nvl(POLL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		  OR
		 nvl(POL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(POH.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 or POH.is_deleted_flg = 'Y'
		or POL.is_deleted_flg = 'Y'
		or POLL.is_deleted_flg = 'Y'
		or RSL.is_deleted_flg = 'Y'
		or RSH.is_deleted_flg = 'Y'
		or RT.is_deleted_flg = 'Y'
		or PLT.is_deleted_flg = 'Y'
		or POD.is_deleted_flg = 'Y'		 
		 )
    ) POF
) TMP
where 1=1
and dw.dw_load_id = 			(
			select
				system_id
			from
				bec_etl_ctrl.etlsourceappid
			where
				source_system = 'EBS'
			)
			||'-'|| nvl(TMP.PO_HEADER_ID, 0)
			||'-'|| nvl(TMP.PO_LINE_ID, 0)
			||'-'|| nvl(TMP.LINE_LOCATION_ID, 0) 
			||'-'|| nvl(TMP.SHIPMENT_LINE_ID, 0)
			||'-'|| nvl(TMP.CODE_COMBINATION_ID,0)
			||'-'|| nvl(TMP.RECEIPT_ENTERED_AS_DATE, '1900-01-01') 
			||'-'|| nvl(TMP.receipt_number, 'NA')			
);


commit;
-- Insert Records

INSERT INTO bec_dwh.FACT_PO_PAYMENT_HIST
(
org_id,
	po_header_id,
	po_line_id,
	item_id,
	po_release_id,
	line_location_id,
	po_type,
	po_number,
	agent_id,
	po_release_number,
	po_date,
	po_line_num,
	line_quantity,
	po_unit_of_measure,
	need_by_date,
	promised_date,
	rcv_quantity_received,
	po_unit_price,
	extended_po_rcv_price,
	receipt_number,
	receipt_date,
	receipt_entered_as_date,
	vendor_id,
	vendor_site_id,
	closed_code,
	authorization_status,
	po_currency_code,
	shipment_header_id,
	shipment_line_id,
	line_type,
	po_header_cancel,
	po_line_cancel,
	po_shipment_cancel,
	code_combination_id,
	cvmi_flag,
	org_id_key,
	item_id_key,
	agent_id_key,
	vendor_id_key,
	vendor_site_id_key,
	gl_account_id_key,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
SELECT
    POF.ORG_ID,
    POF.PO_HEADER_ID,
    POF.PO_LINE_ID,
    POF.ITEM_ID,
    POF.PO_RELEASE_ID,
    POF.LINE_LOCATION_ID,
    POF.PO_TYPE,
    POF.PO_NUMBER,
    POF.AGENT_ID,
    POF.PO_RELEASE_NUMBER,
    POF.PO_DATE,
    POF.PO_LINE_NUM,
    POF.LINE_QUANTITY,
    POF.PO_UNIT_OF_MEASURE,
    POF.NEED_BY_DATE,
    POF.PROMISED_DATE,
    POF.RCV_QUANTITY_RECEIVED,
    POF.PO_UNIT_PRICE,
    POF.EXTENDED_PO_RCV_PRICE,
    POF.RECEIPT_NUMBER,
    POF.RECEIPT_DATE,
    POF.RECEIPT_ENTERED_AS_DATE,
    POF.VENDOR_ID ,
    POF.VENDOR_SITE_ID,
    POF.CLOSED_CODE,
    POF.AUTHORIZATION_STATUS,
    POF.PO_CURRENCY_CODE,
    POF.SHIPMENT_HEADER_ID,
    POF.SHIPMENT_LINE_ID,
    POF.LINE_TYPE,
    POF.PO_HEADER_CANCEL,
    POF.PO_LINE_CANCEL,
    POF.PO_SHIPMENT_CANCEL,
    POF.CODE_COMBINATION_ID,
	POF.CVMI_FLAG,
	
    			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||pof.ORG_ID       		 as ORG_ID_KEY,
			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||pof.ITEM_ID          	 as ITEM_ID_KEY,
			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||pof.AGENT_ID          	 as AGENT_ID_KEY,
			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||pof.VENDOR_ID           as VENDOR_ID_KEY,
			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||pof.VENDOR_SITE_ID      as VENDOR_SITE_ID_KEY,
			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||pof.CODE_COMBINATION_ID as GL_ACCOUNT_ID_KEY,
			'N' AS IS_DELETED_FLG,
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
			||'-'|| nvl(POF.PO_HEADER_ID, 0)
			||'-'|| nvl(POF.PO_LINE_ID, 0)
			||'-'|| nvl(POF.LINE_LOCATION_ID, 0) 
			||'-'|| nvl(POF.SHIPMENT_LINE_ID, 0)
			||'-'|| nvl(POF.CODE_COMBINATION_ID,0)
			||'-'|| nvl(POF.RECEIPT_ENTERED_AS_DATE, '1900-01-01') 
			||'-'|| nvl(POF.receipt_number, 'NA')
			as dw_load_id,
				getdate() as dw_insert_date,
	          getdate() as dw_update_date
    FROM
    (
        SELECT 
            POH.ORG_ID,
            POH.PO_HEADER_ID,
            POL.PO_LINE_ID,
            POL.ITEM_ID,
            PORL.PO_RELEASE_ID,
            POLL.LINE_LOCATION_ID,		
            POH.TYPE_LOOKUP_CODE        PO_TYPE,
            POH.SEGMENT1                PO_NUMBER,
            POH.AGENT_ID,
            PORL.RELEASE_NUM            PO_RELEASE_NUMBER,
            PORL.RELEASE_DATE           PO_DATE,
            POL.LINE_NUM                PO_LINE_NUM,
            POL.ITEM_DESCRIPTION        ITEM_DESCRIPTION,
            POLL.QUANTITY               LINE_QUANTITY,
            POL.UNIT_MEAS_LOOKUP_CODE   PO_UNIT_OF_MEASURE,
            POLL.NEED_BY_DATE,
            POLL.PROMISED_DATE,
            RSL.QUANTITY_RECEIVED       RCV_QUANTITY_RECEIVED,
            ROUND(POL.UNIT_PRICE, 2) PO_UNIT_PRICE,
            ROUND(POL.UNIT_PRICE * RSL.QUANTITY_RECEIVED, 2) EXTENDED_PO_RCV_PRICE,
            RSH.RECEIPT_NUM             RECEIPT_NUMBER,
            RSH.CREATION_DATE           RECEIPT_DATE,
            RT.TRANSACTION_DATE         RECEIPT_ENTERED_AS_DATE,
            POH.VENDOR_ID ,
            POH.VENDOR_SITE_ID,
            POL.CLOSED_CODE,
            PORL.AUTHORIZATION_STATUS,
            POH.CURRENCY_CODE           PO_CURRENCY_CODE,
            RSL.SHIPMENT_HEADER_ID,
            RSL.SHIPMENT_LINE_ID,
            PLT.LINE_TYPE,
            NVL(POH.CANCEL_FLAG, 'N') PO_HEADER_CANCEL,
            NVL(POL.CANCEL_FLAG, 'N') PO_LINE_CANCEL,
            NVL(POLL.CANCEL_FLAG, 'N') PO_SHIPMENT_CANCEL,
            POD.CODE_COMBINATION_ID	,
			nvl(POLL.CONSIGNED_FLAG, 'N') CVMI_FLAG
       FROM
            (select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg <> 'Y')             POH,
            (select * from bec_ods.PO_LINES_ALL where is_deleted_flg <> 'Y')               POL,
            (select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg <> 'Y')      POLL,
            (select * from bec_ods.RCV_SHIPMENT_LINES where is_deleted_flg <> 'Y')         RSL,
            (select * from bec_ods.RCV_SHIPMENT_HEADERS where is_deleted_flg <> 'Y')       RSH,
            (select * from bec_ods.RCV_TRANSACTIONS where is_deleted_flg <> 'Y')           RT,
            (select * from bec_ods.PO_LINE_TYPES_tl where is_deleted_flg <> 'Y')           PLT,
            (select * from bec_ods.PO_RELEASES_ALL where is_deleted_flg <> 'Y')            PORL,
            (select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y')       POD
        WHERE
            1 = 1
            AND POH.PO_HEADER_ID = POL.PO_HEADER_ID (+)
            AND POL.PO_LINE_ID = POLL.PO_LINE_ID (+)
            AND POLL.PO_LINE_ID = RSL.PO_LINE_ID (+)
            AND POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID (+)
            AND RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID (+)
            AND RSL.SHIPMENT_LINE_ID = RT.SHIPMENT_LINE_ID (+)
            AND RSL.PO_RELEASE_ID = RT.PO_RELEASE_ID (+)
            AND RSL.PO_LINE_LOCATION_ID = RT.PO_LINE_LOCATION_ID (+)
            AND RT.DESTINATION_TYPE_CODE (+) = 'INVENTORY'
            AND POLL.PO_RELEASE_ID = PORL.PO_RELEASE_ID (+)
            AND POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID (+)
--            AND POH.ORG_ID = 4534
            AND POH.TYPE_LOOKUP_CODE = 'BLANKET'
            AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID (+)
            AND POLL.PO_RELEASE_ID = POD.PO_RELEASE_ID (+)
            AND NVL(plt.language, 'US') = 'US'
			and  (nvl(POD.kca_seq_date) > (select (executebegints-prune_days) 
					 from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(RSL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
         OR
		 nvl(POLL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		  OR
		 nvl(POL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(POH.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po'))
			
        UNION
        SELECT
            POH.ORG_ID,
            POH.PO_HEADER_ID,
            POL.PO_LINE_ID,
            POL.ITEM_ID,
            NULL PO_RELEASE_ID,
            POLL.LINE_LOCATION_ID,			
            POH.TYPE_LOOKUP_CODE        PO_TYPE,
            POH.SEGMENT1                PO_NUMBER,
            POH.AGENT_ID,
            NULL PO_RELEASE_NUMBER,
            POH.CREATION_DATE           PO_DATE,
            POL.LINE_NUM                PO_LINE_NUM,
            POL.ITEM_DESCRIPTION        ITEM_DESCRIPTION,
            POL.QUANTITY                LINE_QUANTITY,
            POL.UNIT_MEAS_LOOKUP_CODE   PO_UNIT_OF_MEASURE,
            POLL.NEED_BY_DATE,
            POLL.PROMISED_DATE,
            RSL.QUANTITY_RECEIVED       RCV_QUANTITY_RECEIVED,
            ROUND(POL.UNIT_PRICE, 2) PO_UNIT_PRICE,
            ROUND(POL.UNIT_PRICE * RSL.QUANTITY_RECEIVED, 2) EXTENDED_PO_RCV_PRICE,
            RSH.RECEIPT_NUM             RECEIPT_NUMBER,
            RSH.CREATION_DATE           RECEIPT_DATE,
            RT.TRANSACTION_DATE         RECEIPT_ENTERED_AS_DATE,
            POH.VENDOR_ID ,
            POH.VENDOR_SITE_ID,
            POL.CLOSED_CODE,
            POH.AUTHORIZATION_STATUS,
            POH.CURRENCY_CODE           PO_CURRENCY_CODE,
            RSL.SHIPMENT_HEADER_ID,
            RSL.SHIPMENT_LINE_ID,
            PLT.LINE_TYPE,
            NVL(POH.CANCEL_FLAG, 'N') PO_HEADER_CANCEL,
            NVL(POL.CANCEL_FLAG, 'N') PO_LINE_CANCEL,
            NVL(POLL.CANCEL_FLAG, 'N') PO_SHIPMENT_CANCEL,
            POD.CODE_COMBINATION_ID	,	
			nvl(POLL.CONSIGNED_FLAG, 'N') CVMI_FLAG
       FROM
            (select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg <> 'Y')             POH,
            (select * from bec_ods.PO_LINES_ALL where is_deleted_flg <> 'Y')               POL,
            (select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg <> 'Y')      POLL,
            (select * from bec_ods.RCV_SHIPMENT_LINES where is_deleted_flg <> 'Y')         RSL,
            (select * from bec_ods.RCV_TRANSACTIONS where is_deleted_flg <> 'Y')           RT,
            (select * from bec_ods.RCV_SHIPMENT_HEADERS where is_deleted_flg <> 'Y')       RSH,
			(select * from bec_ods.PO_LINE_TYPES_tl where is_deleted_flg <> 'Y')           PLT,
            (select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y')       POD
        WHERE
            1 = 1
            AND POH.PO_HEADER_ID = POL.PO_HEADER_ID (+)
            AND POL.PO_LINE_ID = POLL.PO_LINE_ID (+)
            AND POLL.PO_LINE_ID = RSL.PO_LINE_ID (+)
            AND POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID (+)
            AND RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID (+)
            AND RSL.SHIPMENT_LINE_ID = RT.SHIPMENT_LINE_ID (+)
            AND RSL.PO_LINE_LOCATION_ID = RT.PO_LINE_LOCATION_ID (+)
            AND RSL.PO_LINE_ID = RT.PO_LINE_ID (+)
            AND RT.DESTINATION_TYPE_CODE (+) = 'INVENTORY'
            AND POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID (+)
--            AND POH.ORG_ID = 4534
            AND POH.TYPE_LOOKUP_CODE = 'STANDARD'
            AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID (+)
            AND NVL(plt.language, 'US') = 'US' 
			and 
			(nvl(POD.kca_seq_date) > (select (executebegints-prune_days) 
					 from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(RSL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
         OR
		 nvl(POLL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		  OR
		 nvl(POL.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po')
		 OR
		 nvl(POH.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		 where dw_table_name ='fact_po_payment_hist' and batch_name = 'po'))
--            
			) POF
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_payment_hist'
	and batch_name = 'po';

commit;