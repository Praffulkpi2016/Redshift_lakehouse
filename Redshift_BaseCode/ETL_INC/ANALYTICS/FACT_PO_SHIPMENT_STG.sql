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

delete
from
	bec_dwh.FACT_PO_SHIPMENT_STG
where
	(nvl(vendor_id,0),
	nvl(SHIPMENT_HEADER_ID,0),
	nvl(SHIPMENT_LINE_ID,'0'),
	nvl(routing_header_id,'0')) in (
	select
		nvl(ods.vendor_id,0) as vendor_id,
		nvl(ods.SHIPMENT_HEADER_ID,0) as SHIPMENT_HEADER_ID,
		nvl(ods.SHIPMENT_LINE_ID,'0') as SHIPMENT_LINE_ID,
		nvl(ods.routing_header_id,'0') as routing_header_id
	from
		bec_dwh.FACT_PO_SHIPMENT_STG dw,
		(	
		SELECT
			   rsh.vendor_id,
			   RSL.SHIPMENT_HEADER_ID, 
			   RSL.SHIPMENT_LINE_ID,	  
			   rsl.routing_header_id
		FROM
	    bec_ods.rcv_shipment_headers          rsh,
        bec_ods.rcv_shipment_lines            rsl,
        bec_ods.fnd_lookup_values               polc,
        bec_ods.fnd_lookup_values               polc2,
        bec_ods.po_requisition_lines_all         reql,
        bec_ods.po_requisition_headers_all    reqh,
        bec_ods.rcv_routing_headers           rrh,
        bec_ods.mtl_transaction_reasons       mtr,
        bec_ods.po_hazard_classes_tl             pohc,
        bec_ods.po_un_numbers_tl                 poun,
        bec_ods.mtl_system_items_b              msi,
        bec_ods.hr_locations_all_tl           hl,
        bec_ods.per_all_people_f              he
		WHERE 1=1
	    AND rsh.shipment_header_id = rsl.shipment_header_id
	    AND rsl.source_document_code IN ('INVENTORY','REQ')
        AND polc.lookup_code = rsl.destination_type_code
        AND polc.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
        AND polc2.lookup_code = rsl.source_document_code
        AND polc2.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
        AND reql.requisition_line_id (+) = decode(rsl.source_document_code, 'REQ', rsl.requisition_line_id, - 1)
        AND reqh.requisition_header_id (+) = reql.requisition_header_id
        AND rrh.routing_header_id (+) = nvl(rsl.routing_header_id, - 1)
        AND mtr.reason_id (+) = nvl(rsl.reason_id, - 1)
        AND hl.location_id (+) = nvl(rsl.deliver_to_location_id, - 1)
        AND hl.language (+) = 'US'--userenv('LANG')
        AND he.person_id (+) = nvl(rsl.deliver_to_person_id, - 1)
        AND trunc(sysdate) BETWEEN he.effective_start_date (+) AND he.effective_end_date (+)
        AND msi.organization_id (+) = rsl.to_organization_id
        AND msi.inventory_item_id (+) = rsl.item_id
        AND msi.hazard_class_id = pohc.hazard_class_id (+)
        AND msi.un_number_id = poun.un_number_id (+)
		and (rsl.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_po_shipment_stg'
				and batch_name = 'po'))		
        UNION
        SELECT
			   rsh.vendor_id,
			   RSL.SHIPMENT_HEADER_ID, 
			   RSL.SHIPMENT_LINE_ID,	  
			   rsl.routing_header_id			   
      FROM
        bec_ods.rcv_shipment_headers       rsh,
        bec_ods.rcv_shipment_lines         rsl,
        bec_ods.fnd_lookup_values            polc,
        bec_ods.fnd_lookup_values            polc2,
        bec_ods.po_headers_all             poh,
        bec_ods.po_lines_all               pol,
        bec_ods.po_line_locations_all      poll,
        bec_ods.po_releases_all            por,
        bec_ods.po_vendors                 pov,
        bec_ods.ap_supplier_sites_all            povs,
        bec_ods.rcv_routing_headers        rrh,
        bec_ods.mtl_transaction_reasons    mtr,
        bec_ods.po_hazard_classes_tl          pohc,
        bec_ods.po_un_numbers_tl              poun,
        bec_ods.hr_locations_all_tl        hl,
        bec_ods.per_all_people_f           he
    WHERE 1=1
	    AND nvl(poll.approved_flag, 'N') = 'Y'
        AND nvl(poll.cancel_flag, 'N') = 'N'
        AND nvl(poll.closed_code, 'OPEN') != 'FINALLY CLOSED'
        AND polc.lookup_code = rsl.destination_type_code
        AND polc2.lookup_code = rsl.source_document_code
        AND polc2.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
        AND polc.lookup_type = 'RCV DESTINATION TYPE'
        AND rsl.source_document_code = 'PO'
        AND rsl.po_header_id = poh.po_header_id
        AND rsl.po_line_id = pol.po_line_id
        AND pol.hazard_class_id = pohc.hazard_class_id (+)
        AND pol.un_number_id = poun.un_number_id (+)
        AND rsl.po_line_location_id = poll.line_location_id
        AND rsl.po_release_id = por.po_release_id (+)
        AND rrh.routing_header_id (+) = nvl(rsl.routing_header_id, - 1)
        AND mtr.reason_id (+) = nvl(rsl.reason_id, - 1)
        AND hl.location_id (+) = nvl(rsl.deliver_to_location_id, - 1)
        AND hl.language (+) = 'US'--userenv('LANG')
        AND he.person_id (+) = nvl(rsl.deliver_to_person_id, - 1)
        AND trunc(sysdate) BETWEEN he.effective_start_date (+) AND he.effective_end_date (+)
        AND pov.vendor_id (+) = rsh.vendor_id
        AND povs.vendor_site_id (+) = rsh.vendor_site_id
       /* AND rsh.asn_type IN (
            'ASN',
            'ASBN'
        )
        AND rsl.shipment_line_status_code IN (
            'EXPECTED',
            'PARTIALLY RECEIVED',
            'FULLY RECEIVED'
        ) */
        AND rsh.shipment_header_id = rsl.shipment_header_id
		and (rsl.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_po_shipment_stg'
				and batch_name = 'po') )				
    ) ods
	where
		dw.dw_load_id = 			(
			select
				system_id
			from
				bec_etl_ctrl.etlsourceappid
			where
				source_system = 'EBS'
			)
			   || '-' || nvl(ods.vendor_id, 0)
			   || '-' || nvl(ods.SHIPMENT_HEADER_ID, 0) 
			   || '-' || nvl(ods.SHIPMENT_LINE_ID, '0')	  
			   || '-' || nvl(ods.routing_header_id, '0')
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_PO_SHIPMENT_STG
(   SOURCE_TYPE,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    asn_type,
    rsh_packing_slip,
    shipment_num,
    waybill_airbill_num,
    receipt_num,
    RSH_creation_date,
    shipped_date,
    expected_receipt_date,
    rsh_comments,
    bill_of_lading,
    freight_carrier_code,
    num_of_containers,
    receipt_source_code,
    CHARGE_ACCOUNT_ID,
    rsl_comments,
    DELIVER_TO_LOCATION_ID,
    DELIVER_TO_PERSON_ID,
    DESTINATION_CONTEXT,
    DESTINATION_TYPE_CODE,
    DESTINATION_TYPE, --
    EMPLOYEE_ID,
    FROM_ORGANIZATION_ID,
    ITEM_DESCRIPTION,
    ITEM_ID,
    ITEM_REVISION,
    HAZARD_CLASS,
    UN_NUMBER,
    LINE_NUM,
    ITEM_CATEGORY_ID,
    LOCATOR_ID,
    NEED_BY_DATE,
    rsl_packing_slip ,
    QUANTITY_RECEIVED,
    QUANTITY_SHIPPED,
    REQUISITION_LINE_ID,
    REQUISITION_HEADER_ID,
    ORDER_NUM,
    ORDER_LINE_NUM,
    REQ_DISTRIBUTION_ID,
    SHIPMENT_HEADER_ID,
    SHIPMENT_LINE_ID,
    SHIPMENT_LINE_STATUS_CODE,
    SOURCE_DOCUMENT_CODE,
    SOURCE_DOCUMENT_TYPE,
    TO_ORGANIZATION_ID,
    TO_SUBINVENTORY,
    TRANSFER_COST,
    TRANSPORTATION_ACCOUNT_ID,
    TRANSPORTATION_COST,
    UNIT_OF_MEASURE,
    UOM_CONVERSION_RATE,
    ROUTING_HEADER_ID,
    ROUTING_NAME,
    REASON_ID,
    REASON_NAME,
    LOCATION_CODE,
    DELIVER_TO_PERSON,
    PO_HEADER_ID,
    PO_LINE_ID,
    PO_LINE_LOCATION_ID,
    PO_RELEASE_ID,
    RELEASE_NUM,
    VENDOR_NAME,
    VENDOR_SITE_CODE,
    SHIP_TO_LOCATION_ID,
    PRIMARY_UNIT_OF_MEASURE,
    VENDOR_ID,
    BAR_CODE_LABEL,
    TRUCK_NUM,
    CONTAINER_NUM,
    VENDOR_LOT_NUM,
    SECONDARY_QUANTITY_RECEIVED,
    SECONDARY_QUANTITY_SHIPPED,
    SECONDARY_UNIT_OF_MEASURE,
    QC_GRADE,
    ORG_ID,
	is_deleted_flg,
	source_app_id,
	dw_load_id, 
	dw_insert_date,
	dw_update_date)
( select 
    shp.SOURCE_TYPE,
    shp.CREATED_BY,
    shp.CREATION_DATE,
    shp.LAST_UPDATED_BY,
    shp.LAST_UPDATE_DATE,
    shp.LAST_UPDATE_LOGIN,
    shp.REQUEST_ID,
    shp.PROGRAM_APPLICATION_ID,
    shp.PROGRAM_ID,
    shp.PROGRAM_UPDATE_DATE,
    shp.asn_type,
    shp.rsh_packing_slip,
    shp.shipment_num,
    shp.waybill_airbill_num,
    shp.receipt_num,
    shp.RSH_creation_date,
    shp.shipped_date,
    shp.expected_receipt_date,
    shp.rsh_comments,
    shp.bill_of_lading,
    shp.freight_carrier_code,
    shp.num_of_containers,
    shp.receipt_source_code,
    shp.CHARGE_ACCOUNT_ID,
    shp.rsl_comments,
    shp.DELIVER_TO_LOCATION_ID,
    shp.DELIVER_TO_PERSON_ID,
    shp.DESTINATION_CONTEXT,
    shp.DESTINATION_TYPE_CODE,
    shp.DESTINATION_TYPE, --
    shp.EMPLOYEE_ID,
    shp.FROM_ORGANIZATION_ID,
    shp.ITEM_DESCRIPTION,
    shp.ITEM_ID,
    shp.ITEM_REVISION,
    shp.HAZARD_CLASS,
    shp.UN_NUMBER,
    shp.LINE_NUM,
    shp.ITEM_CATEGORY_ID,
    shp.LOCATOR_ID,
    shp.NEED_BY_DATE,
    shp.rsl_packing_slip ,
    shp.QUANTITY_RECEIVED,
    shp.QUANTITY_SHIPPED,
    shp.REQUISITION_LINE_ID,
    shp.REQUISITION_HEADER_ID,
    shp.ORDER_NUM,
    shp.ORDER_LINE_NUM,
    shp.REQ_DISTRIBUTION_ID,
    shp.SHIPMENT_HEADER_ID,
    shp.SHIPMENT_LINE_ID,
    shp.SHIPMENT_LINE_STATUS_CODE,
    shp.SOURCE_DOCUMENT_CODE,
    shp.SOURCE_DOCUMENT_TYPE,
    shp.TO_ORGANIZATION_ID,
    shp.TO_SUBINVENTORY,
    shp.TRANSFER_COST,
    shp.TRANSPORTATION_ACCOUNT_ID,
    shp.TRANSPORTATION_COST,
    shp.UNIT_OF_MEASURE,
    shp.UOM_CONVERSION_RATE,
    shp.ROUTING_HEADER_ID,
    shp.ROUTING_NAME,
    shp.REASON_ID,
    shp.REASON_NAME,
    shp.LOCATION_CODE,
    shp.DELIVER_TO_PERSON,
    shp.PO_HEADER_ID,
    shp.PO_LINE_ID,
    shp.PO_LINE_LOCATION_ID,
    shp.PO_RELEASE_ID,
    shp.RELEASE_NUM,
    shp.VENDOR_NAME,
    shp.VENDOR_SITE_CODE,
    shp.SHIP_TO_LOCATION_ID,
    shp.PRIMARY_UNIT_OF_MEASURE,
    shp.VENDOR_ID,
    shp.BAR_CODE_LABEL,
    shp.TRUCK_NUM,
    shp.CONTAINER_NUM,
    shp.VENDOR_LOT_NUM,
    shp.SECONDARY_QUANTITY_RECEIVED,
    shp.SECONDARY_QUANTITY_SHIPPED,
    shp.SECONDARY_UNIT_OF_MEASURE,
    shp.QC_GRADE,
    shp.ORG_ID,
	shp.is_deleted_flg,
	shp.source_app_id,
	shp.dw_load_id, 
	shp.dw_insert_date,
	shp.dw_update_date	
from  
(SELECT
        ' INTERNAL' SOURCE_TYPE,
        rsl.created_by,
        rsl.creation_date,
        rsl.last_updated_by,
        rsl.last_update_date,
        rsl.last_update_login,
        rsl.request_id,
        rsl.program_application_id,
        rsl.program_id,
        rsl.program_update_date,
        rsh.asn_type,
        rsh.packing_slip rsh_packing_slip,
        rsh.shipment_num,
        rsh.waybill_airbill_num,
        rsh.receipt_num,
        rsh.creation_date RSH_creation_date,
        rsh.shipped_date,
        rsh.expected_receipt_date,
        rsh.comments rsh_comments,
        rsh.bill_of_lading,
        rsh.freight_carrier_code,
        rsh.num_of_containers,
        rsh.receipt_source_code,
        rsl.charge_account_id,
        rsl.comments rsl_comments,
        rsl.deliver_to_location_id,
        rsl.deliver_to_person_id,
        rsl.destination_context,
        rsl.destination_type_code,
        polc.meaning    destination_type,
        rsl.employee_id,
        rsl.from_organization_id,
        rsl.item_description,
        rsl.item_id,
        rsl.item_revision,
        pohc.hazard_class,
        poun.un_number,
        rsl.line_num,
        rsl.category_id         item_category_id,
        rsl.locator_id,
        decode(rsl.source_document_code, 'REQ', reql.need_by_date, NULL) need_by_date,
        rsl.packing_slip rsl_packing_slip,
        rsl.quantity_received,
        rsl.quantity_shipped,
        rsl.requisition_line_id,  --kv
        reqh.requisition_header_id,  --kv
        decode(rsl.source_document_code, 'REQ', reqh.segment1, NULL) order_num,
        decode(rsl.source_document_code, 'REQ', reql.line_num, NULL) ORDER_LINE_NUM,
        rsl.req_distribution_id, --kv
        rsl.shipment_header_id,
        rsl.shipment_line_id,
        rsl.shipment_line_status_code,
        rsl.source_document_code,
        polc2.meaning   source_document_type,
        rsl.to_organization_id,
        rsl.to_subinventory,
        rsl.transfer_cost,
        rsl.transportation_account_id,
        rsl.transportation_cost,
        rsl.unit_of_measure,
        rsl.uom_conversion_rate,
        rsl.routing_header_id,
        rrh.routing_name,
        rsl.reason_id,
        mtr.reason_name,
        hl.location_code,
        he.full_name            deliver_to_person,
        NULL PO_HEADER_ID, -- to_number(NULL),
        NULL PO_LINE_ID, -- to_number(NULL),
        NULL PO_LINE_LOCATION_ID, -- to_number(NULL),
        NULL PO_RELEASE_ID, -- to_number(NULL),
        NULL RELEASE_NUM, --  to_number(NULL),
        NULL VENDOR_NAME,
        NULL VENDOR_SITE_CODE, -- povs
        null SHIP_TO_LOCATION_ID,  -- to_number(NULL),
        NULL PRIMARY_UNIT_OF_MEASURE,
        rsh.vendor_id,
        NULL BAR_CODE_LABEL,
        NULL TRUCK_NUM,
        NULL CONTAINER_NUM,
        NULL VENDOR_LOT_NUM,
        rsl.secondary_quantity_received,
        rsl.secondary_quantity_shipped,
        rsl.secondary_unit_of_measure,
        rsl.qc_grade,
        reql.org_id,
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
			   || '-' || nvl(rsh.vendor_id, 0)
			   || '-' || nvl(RSL.SHIPMENT_HEADER_ID, 0) 
			   || '-' || nvl(RSL.SHIPMENT_LINE_ID, '0')	  
			   || '-' || nvl(rsl.routing_header_id, '0')		as dw_load_id, 
			getdate() as dw_insert_date,
			getdate() as dw_update_date			
		FROM
	    (select * from bec_ods.rcv_shipment_headers where is_deleted_flg <> 'Y')         rsh,
        (select * from bec_ods.rcv_shipment_lines where is_deleted_flg <> 'Y')           rsl,
        (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y')              polc,
        (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y')              polc2,
        (select * from bec_ods.po_requisition_lines_all where is_deleted_flg <> 'Y')        reql,
        (select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y')   reqh,
        (select * from bec_ods.rcv_routing_headers where is_deleted_flg <> 'Y')          rrh,
        (select * from bec_ods.mtl_transaction_reasons where is_deleted_flg <> 'Y')      mtr,
        (select * from bec_ods.po_hazard_classes_tl where is_deleted_flg <> 'Y')            pohc,
        (select * from bec_ods.po_un_numbers_tl where is_deleted_flg <> 'Y')                poun,
        (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')             msi,
        (select * from bec_ods.hr_locations_all_tl where is_deleted_flg <> 'Y')          hl,
        (select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y')             he
		WHERE 1=1
	    AND rsh.shipment_header_id = rsl.shipment_header_id
	    AND rsl.source_document_code IN ('INVENTORY','REQ')
        AND polc.lookup_code = rsl.destination_type_code
        AND polc.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
        AND polc2.lookup_code = rsl.source_document_code
        AND polc2.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
        AND reql.requisition_line_id (+) = decode(rsl.source_document_code, 'REQ', rsl.requisition_line_id, - 1)
        AND reqh.requisition_header_id (+) = reql.requisition_header_id
        AND rrh.routing_header_id (+) = nvl(rsl.routing_header_id, - 1)
        AND mtr.reason_id (+) = nvl(rsl.reason_id, - 1)
        AND hl.location_id (+) = nvl(rsl.deliver_to_location_id, - 1)
        AND hl.language (+) = 'US'--userenv('LANG')
        AND he.person_id (+) = nvl(rsl.deliver_to_person_id, - 1)
        AND trunc(sysdate) BETWEEN he.effective_start_date (+) AND he.effective_end_date (+)
        AND msi.organization_id (+) = rsl.to_organization_id
        AND msi.inventory_item_id (+) = rsl.item_id
        AND msi.hazard_class_id = pohc.hazard_class_id (+)
        AND msi.un_number_id = poun.un_number_id (+)
		AND (rsl.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_po_shipment_stg'
				and batch_name = 'po'))       
    union 
    SELECT
        'ASN' SOURCE_TYPE,
        rsl.created_by,
        rsl.creation_date RSL_creation_date,
        rsl.last_updated_by,
        rsl.last_update_date,
        rsl.last_update_login,
        rsl.request_id,
        rsl.program_application_id,
        rsl.program_id,
        rsl.program_update_date,
        rsh.asn_type,
        rsh.packing_slip rsh_packing_slip,
        rsh.shipment_num,
        rsh.waybill_airbill_num,
        rsh.receipt_num,
        rsh.creation_date RSH_creation_date,
        rsh.shipped_date,
        rsh.expected_receipt_date,
        rsh.comments rsh_comments,
        rsh.bill_of_lading,
        rsh.freight_carrier_code,
        rsh.num_of_containers,
        rsh.receipt_source_code,
        rsl.charge_account_id,
        rsl.comments rsl_comments,
        rsl.deliver_to_location_id,
        rsl.deliver_to_person_id,
        rsl.destination_context,
        rsl.destination_type_code,
        polc.meaning destination_type,--polc.po_type_display    destination_type,
        rsl.employee_id,
        rsl.from_organization_id,
        rsl.item_description,
        rsl.item_id,
        rsl.item_revision,
        pohc.hazard_class,
        poun.un_number,
        rsl.line_num,
        rsl.category_id         item_category_id,
        rsl.locator_id,
        decode(rsl.source_document_code, 'PO', poll.need_by_date, NULL) need_by_date,
        rsl.packing_slip rsl_packing_slip,
        rsl.quantity_received,
        rsl.quantity_shipped,
        NULL requisition_line_id,  -- to_number('NULL'),
        NULL requisition_header_id,  -- to_number('NULL'),
        decode(rsl.source_document_code, 'PO', poh.segment1, NULL) order_num,
        decode(rsl.source_document_code, 'PO', pol.line_num, NULL) ORDER_LINE_NUM,
        NULL req_distribution_id, --  to_number('NULL'),
        rsl.shipment_header_id,
        rsl.shipment_line_id,
        rsl.shipment_line_status_code,
        rsl.source_document_code,
       -- NULL source_document_type,--polc2.po_type_display   source_document_type,
		polc2.meaning source_document_type,
        rsl.to_organization_id,
        rsl.to_subinventory,
        rsl.transfer_cost,
        rsl.transportation_account_id,
        rsl.transportation_cost,
        rsl.unit_of_measure,
        rsl.uom_conversion_rate,
        rsl.routing_header_id,
        rrh.routing_name,
        rsl.reason_id,
        mtr.reason_name,
        hl.location_code,
        he.full_name            deliver_to_person,
        rsl.po_header_id,  ---kv
        rsl.po_line_id,
        rsl.po_line_location_id,
        rsl.po_release_id,
        por.release_num, ---kv
        pov.vendor_name,
        povs.vendor_site_code,  --kv
        rsh.ship_to_location_id,  --kv
        rsl.primary_unit_of_measure,
        rsh.vendor_id,
        rsl.bar_code_label,
        rsl.truck_num,
        rsl.container_num,
        rsl.vendor_lot_num,
        rsl.secondary_quantity_received,
        rsl.secondary_quantity_shipped,
        rsl.secondary_unit_of_measure,
        rsl.qc_grade,
        poll.org_id,
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
			   || '-' || nvl(rsh.vendor_id, 0)	
			   || '-' || nvl(RSL.SHIPMENT_HEADER_ID, 0) 
			   || '-' || nvl(RSL.SHIPMENT_LINE_ID, '0')	  
			   || '-' || nvl(rsl.routing_header_id, '0')	as dw_load_id,  
			getdate() as dw_insert_date,
			getdate() as dw_update_date			
      FROM
        (select * from bec_ods.rcv_shipment_headers where is_deleted_flg <> 'Y')      rsh,
        (select * from bec_ods.rcv_shipment_lines where is_deleted_flg <> 'Y')        rsl,
        (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y')           polc,
        (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y')           polc2,
        (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y')            poh,
        (select * from bec_ods.po_lines_all where is_deleted_flg <> 'Y')              pol,
        (select * from bec_ods.po_line_locations_all where is_deleted_flg <> 'Y')     poll,
        (select * from bec_ods.po_releases_all where is_deleted_flg <> 'Y')           por,
        (select * from bec_ods.po_vendors where is_deleted_flg <> 'Y')                pov,
        (select * from bec_ods.ap_supplier_sites_all where is_deleted_flg <> 'Y')           povs,
        (select * from bec_ods.rcv_routing_headers where is_deleted_flg <> 'Y')       rrh,
        (select * from bec_ods.mtl_transaction_reasons where is_deleted_flg <> 'Y')   mtr,
        (select * from bec_ods.po_hazard_classes_tl where is_deleted_flg <> 'Y')         pohc,
        (select * from bec_ods.po_un_numbers_tl where is_deleted_flg <> 'Y')             poun,
        (select * from bec_ods.hr_locations_all_tl where is_deleted_flg <> 'Y')       hl,
        (select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y')          he
    WHERE 1=1
	    AND nvl(poll.approved_flag, 'N') = 'Y'
        AND nvl(poll.cancel_flag, 'N') = 'N'
        AND nvl(poll.closed_code, 'OPEN') != 'FINALLY CLOSED'
        AND polc.lookup_code = rsl.destination_type_code
        AND polc2.lookup_code = rsl.source_document_code
        AND polc2.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
        AND polc.lookup_type = 'RCV DESTINATION TYPE'
        AND rsl.source_document_code = 'PO'
        AND rsl.po_header_id = poh.po_header_id
        AND rsl.po_line_id = pol.po_line_id
        AND pol.hazard_class_id = pohc.hazard_class_id (+)
        AND pol.un_number_id = poun.un_number_id (+)
        AND rsl.po_line_location_id = poll.line_location_id
        AND rsl.po_release_id = por.po_release_id (+)
        AND rrh.routing_header_id (+) = nvl(rsl.routing_header_id, - 1)
        AND mtr.reason_id (+) = nvl(rsl.reason_id, - 1)
        AND hl.location_id (+) = nvl(rsl.deliver_to_location_id, - 1)
        AND hl.language (+) = 'US'--userenv('LANG')
        AND he.person_id (+) = nvl(rsl.deliver_to_person_id, - 1)
        AND trunc(sysdate) BETWEEN he.effective_start_date (+) AND he.effective_end_date (+)
        AND pov.vendor_id (+) = rsh.vendor_id
        AND povs.vendor_site_id (+) = rsh.vendor_site_id
        AND rsh.asn_type IN (
            'ASN',
            'ASBN'
        )
        AND rsl.shipment_line_status_code IN (
            'EXPECTED',
            'PARTIALLY RECEIVED',
            'FULLY RECEIVED'
        )
        AND rsh.shipment_header_id = rsl.shipment_header_id
		AND (rsl.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_po_shipment_stg'
				and batch_name = 'po')) 
) shp
);


end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_shipment_stg'
	and batch_name = 'po';

commit;