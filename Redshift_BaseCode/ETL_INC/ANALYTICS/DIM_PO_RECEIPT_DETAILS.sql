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
DELETE FROM bec_dwh.DIM_PO_RECEIPT_DETAILS
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.rcv_transactions rct
    INNER JOIN bec_ods.po_headers_all poh ON rct.po_header_id = poh.po_header_id
    INNER JOIN bec_ods.po_lines_all pol ON rct.po_line_id = pol.po_line_id
    INNER JOIN bec_ods.rcv_shipment_headers rch ON rct.shipment_header_id = rch.shipment_header_id
    WHERE rct.destination_type_code = 'INVENTORY'
    AND rct.kca_seq_date > (
        SELECT (executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_po_receipt_details'
        AND batch_name = 'po'
    )
    AND NVL(DIM_PO_RECEIPT_DETAILS.rcv_txn_id, 0) = NVL(rct.transaction_id, 0)
);

commit;
-- Insert Records
insert into bec_dwh.DIM_PO_RECEIPT_DETAILS (
  rcv_txn_id, 
  destination_type_code, 
  po_header_id, 
  po_line_id, 
  shipment_header_id, 
  po_number, 
  po_line_number, 
  receipt_num, 
  receipt_date, 
  is_deleted_flg, 
  source_app_id, 
  dw_load_id, 
  dw_insert_date, 
  dw_update_date
) (
  SELECT 
    rct.transaction_id rcv_txn_id, 
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
    ) AS source_app_id, 
    (
      SELECT 
        system_id 
      FROM 
        bec_etl_ctrl.etlsourceappid 
      WHERE 
        source_system = 'EBS'
    ) || '-' || nvl(rcv_txn_id, 0) AS dw_load_id, 
    getdate() AS dw_insert_date, 
    getdate() AS dw_update_date 
  FROM 
    bec_ods.rcv_transactions rct, 
    bec_ods.po_headers_all poh, 
    bec_ods.po_lines_all pol, 
    bec_ods.rcv_shipment_headers rch 
  WHERE 
    rct.po_header_id = poh.po_header_id 
    AND rct.shipment_header_id = rch.shipment_header_id 
    AND rct.po_line_id = pol.po_line_id 
    AND rct.destination_type_code = 'INVENTORY' 
    and (
      rct.kca_seq_date > (
        select 
          (executebegints - prune_days) 
        from 
          bec_etl_ctrl.batch_dw_info 
        where 
          dw_table_name = 'dim_po_receipt_details' 
          and batch_name = 'po'
      )
    )
);
commit;
-- Soft Delete
update bec_dwh.DIM_PO_RECEIPT_DETAILS set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
    SELECT 1
    FROM bec_ods.rcv_transactions rct
    INNER JOIN bec_ods.po_headers_all poh ON rct.po_header_id = poh.po_header_id
    INNER JOIN bec_ods.po_lines_all pol ON rct.po_line_id = pol.po_line_id
    INNER JOIN bec_ods.rcv_shipment_headers rch ON rct.shipment_header_id = rch.shipment_header_id
    WHERE rct.destination_type_code = 'INVENTORY'
    AND rct.is_deleted_flg <> 'Y' 
    AND NVL(DIM_PO_RECEIPT_DETAILS.rcv_txn_id, 0) = NVL(rct.transaction_id, 0)
);
commit;
end;
UPDATE 
  bec_etl_ctrl.batch_dw_info 
SET 
  load_type = 'I', 
  last_refresh_date = getdate() 
WHERE 
  dw_table_name = 'dim_po_receipt_details' 
  and batch_name = 'po';
COMMIT;