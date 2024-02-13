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
drop table if exists bec_dwh.DIM_PO_RECEIPT_DETAILS;

CREATE TABLE bec_dwh.DIM_PO_RECEIPT_DETAILS 
DISTKEY (rcv_txn_id)
SORTKEY (rcv_txn_id, destination_type_code, po_header_id, po_line_id, shipment_header_id)
AS
(
SELECT rct.transaction_id rcv_txn_id,
    rct.destination_type_code,
	poh.po_header_id,
	pol.po_line_id,
	rch.shipment_header_id,
    poh.segment1 po_number,
    pol.line_num po_line_number,
    rch.receipt_num,
    rch.creation_date receipt_date,
	'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(rcv_txn_id,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM bec_ods.rcv_transactions rct,
     bec_ods.po_headers_all poh,
     bec_ods.po_lines_all pol,
     bec_ods.rcv_shipment_headers rch
WHERE rct.po_header_id = poh.po_header_id
AND rct.shipment_header_id = rch.shipment_header_id
AND rct.po_line_id = pol.po_line_id
AND rct.destination_type_code = 'INVENTORY'
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_receipt_details' 
	and batch_name = 'po';

COMMIT;