/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

TRUNCATE TABLE bec_ods.BEC_CVMI_AGING_VIEW;

INSERT INTO bec_ods.BEC_CVMI_AGING_VIEW (
	as_of_date,
	organization_id,
	vendor_id,
	vendor_site_id,
	item_id,
	primary_vendor_item,
	"name",
	vendorname,
	vendorsitecode,
	items,
	description,
	uom,
	subinventory,
	"locator",
	revision,
	receipt_date,
	consumebefore,
	quantity,
	aging_period,
	aging_days,
	aging_bucket,
	received_date_time,
	serial_lot_number,
	unit_price,
	extended_cost,
	percent_aging,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
     	as_of_date,
	organization_id,
	vendor_id,
	vendor_site_id,
	item_id,
	primary_vendor_item,
	"name",
	vendorname,
	vendorsitecode,
	items,
	description,
	uom,
	subinventory,
	"locator",
	revision,
	receipt_date,
	consumebefore,
	quantity,
	aging_period,
	aging_days,
	aging_bucket,
	received_date_time,
	serial_lot_number,
	unit_price,
	extended_cost,
	percent_aging,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.BEC_CVMI_AGING_VIEW;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bec_cvmi_aging_view';