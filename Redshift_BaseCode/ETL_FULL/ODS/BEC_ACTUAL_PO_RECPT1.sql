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

DROP TABLE if exists bec_ods.BEC_ACTUAL_PO_RECPT1;

CREATE TABLE IF NOT EXISTS bec_ods.BEC_ACTUAL_PO_RECPT1
(
    wip_entity_name VARCHAR(240)   ENCODE lzo
	,po_type VARCHAR(25)   ENCODE lzo
	,po_number VARCHAR(20)   ENCODE lzo
	,pr_approved_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reaction_time NUMERIC(38,0)   ENCODE az64
	,po_placed_ontime NUMERIC(15,0)   ENCODE az64
	,buyer_name VARCHAR(240)   ENCODE lzo
	,po_release_number NUMERIC(15,0)   ENCODE az64
	,po_shipment_num NUMERIC(15,0)   ENCODE az64
	,po_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,po_last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,processing_leadtime NUMERIC(15,0)   ENCODE az64
	,po_line_num NUMERIC(28,10)   ENCODE az64
	,item_name VARCHAR(40)   ENCODE lzo
	,item_description VARCHAR(240)   ENCODE lzo
	,line_quantity NUMERIC(28,10)   ENCODE az64
	,quantity_shipped NUMERIC(28,10)   ENCODE az64
	,po_unit_of_measure VARCHAR(25)   ENCODE lzo
	,primary_uom_code VARCHAR(3)   ENCODE lzo
	,primary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,promised_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,rcv_quantity_received NUMERIC(28,10)   ENCODE az64
	,primary_quantity NUMERIC(28,10)   ENCODE az64
	,po_unit_price NUMERIC(28,10)   ENCODE az64
	,extended_po_rcv_price NUMERIC(28,10)   ENCODE az64
	,receipt_number VARCHAR(30)   ENCODE lzo
	,packing_slip VARCHAR(25)   ENCODE lzo
	,shipment_num VARCHAR(30)   ENCODE lzo
	,waybill_airbill_num VARCHAR(30)   ENCODE lzo
	,bill_of_lading VARCHAR(25)   ENCODE lzo
	,shipment_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,vendor_number VARCHAR(30)   ENCODE lzo
	,vendor_name VARCHAR(240)   ENCODE lzo
	,closed_code VARCHAR(25)   ENCODE lzo
	,po_currency_code VARCHAR(15)   ENCODE lzo
	,shipment_header_id NUMERIC(15,0)   ENCODE az64
	,shipment_line_id NUMERIC(15,0)   ENCODE az64
	,po_line_type VARCHAR(25)   ENCODE lzo
	,line_location_id NUMERIC(15,0)   ENCODE az64
	,unit_of_measure VARCHAR(25)   ENCODE lzo
	,po_conversion NUMERIC(28,10)   ENCODE az64
	,rcv_conversion NUMERIC(28,10)   ENCODE az64
	,asn_type VARCHAR(25)   ENCODE lzo
	,asn_status VARCHAR(10)   ENCODE lzo
	,receipt_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,std_item_cost NUMERIC(28,10)   ENCODE az64
	,material_cost NUMERIC(28,10)   ENCODE az64
	,ext_material_cost NUMERIC(28,10)   ENCODE az64
	,purchase_price_variance NUMERIC(28,10)   ENCODE az64
	,outside_processing_cost NUMERIC(28,10)   ENCODE az64
	,overhead_cost NUMERIC(28,10)   ENCODE az64
	,material_overhead_cost NUMERIC(28,10)   ENCODE az64
	,resource_cost NUMERIC(28,10)   ENCODE az64
	,fob_code VARCHAR(25)   ENCODE lzo
	,ship_to_location VARCHAR(60)   ENCODE lzo
	,ship_to_organization VARCHAR(240)   ENCODE lzo
	,organization_code VARCHAR(3)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,category VARCHAR(81)   ENCODE lzo
	,item_revision VARCHAR(3)   ENCODE lzo
	,attribute10 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_commit_need_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,freight_terms VARCHAR(25)   ENCODE lzo
	,cvmi_flag VARCHAR(1)   ENCODE lzo
	,vendor_country VARCHAR(60)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;


INSERT INTO bec_ods.BEC_ACTUAL_PO_RECPT1 (
	wip_entity_name,
	po_type,
	po_number,
	pr_approved_date,
	reaction_time,
	po_placed_ontime,
	buyer_name,
	po_release_number,
	po_shipment_num,
	po_date,
	po_last_update_date,
	processing_leadtime,
	po_line_num,
	item_name,
	item_description,
	line_quantity,
	quantity_shipped,
	po_unit_of_measure,
	primary_uom_code,
	primary_unit_of_measure,
	need_by_date,
	promised_date,
	rcv_quantity_received,
	primary_quantity,
	po_unit_price,
	extended_po_rcv_price,
	receipt_number,
	packing_slip,
	shipment_num,
	waybill_airbill_num,
	bill_of_lading,
	shipment_date,
	vendor_number,
	vendor_name,
	closed_code,
	po_currency_code,
	shipment_header_id,
	shipment_line_id,
	po_line_type,
	line_location_id,
	unit_of_measure,
	po_conversion,
	rcv_conversion,
	asn_type,
	asn_status,
	receipt_date,
	std_item_cost,
	material_cost,
	ext_material_cost,
	purchase_price_variance,
	outside_processing_cost,
	overhead_cost,
	material_overhead_cost,
	resource_cost,
	fob_code,
	ship_to_location,
	ship_to_organization,
	organization_code,
	organization_id,
	category,
	item_revision,
	attribute10,
	actual_commit_need_date,
	freight_terms,
	cvmi_flag,
	vendor_country,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)

 SELECT
     	wip_entity_name,
	po_type,
	po_number,
	pr_approved_date,
	reaction_time,
	po_placed_ontime,
	buyer_name,
	po_release_number,
	po_shipment_num,
	po_date,
	po_last_update_date,
	processing_leadtime,
	po_line_num,
	item_name,
	item_description,
	line_quantity,
	quantity_shipped,
	po_unit_of_measure,
	primary_uom_code,
	primary_unit_of_measure,
	need_by_date,
	promised_date,
	rcv_quantity_received,
	primary_quantity,
	po_unit_price,
	extended_po_rcv_price,
	receipt_number,
	packing_slip,
	shipment_num,
	waybill_airbill_num,
	bill_of_lading,
	shipment_date,
	vendor_number,
	vendor_name,
	closed_code,
	po_currency_code,
	shipment_header_id,
	shipment_line_id,
	po_line_type,
	line_location_id,
	unit_of_measure,
	po_conversion,
	rcv_conversion,
	asn_type,
	asn_status,
	receipt_date,
	std_item_cost,
	material_cost,
	ext_material_cost,
	purchase_price_variance,
	outside_processing_cost,
	overhead_cost,
	material_overhead_cost,
	resource_cost,
	fob_code,
	ship_to_location,
	ship_to_organization,
	organization_code,
	organization_id,
	category,
	item_revision,
	attribute10,
	actual_commit_need_date,
	freight_terms,
	cvmi_flag,
	vendor_country,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.BEC_ACTUAL_PO_RECPT1;

end;	

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bec_actual_po_recpt1';
	
commit;	