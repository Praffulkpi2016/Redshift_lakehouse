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

DROP TABLE if exists bec_ods.BEC_CVMI_AGING_VIEW;

CREATE TABLE IF NOT EXISTS bec_ods.BEC_CVMI_AGING_VIEW
(
    as_of_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,item_id NUMERIC(15,0)   ENCODE az64
	,primary_vendor_item VARCHAR(25)   ENCODE lzo
	,name VARCHAR(240)   ENCODE lzo
	,vendorname VARCHAR(240)   ENCODE lzo
	,vendorsitecode VARCHAR(45)   ENCODE lzo
	,items VARCHAR(40)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,uom VARCHAR(3)   ENCODE lzo
	,subinventory VARCHAR(10)   ENCODE lzo
	,locator VARCHAR(122)   ENCODE lzo
	,revision VARCHAR(3)   ENCODE lzo
	,receipt_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,consumebefore TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,quantity NUMERIC(28,10)   ENCODE az64
	,aging_period NUMERIC(15,0)   ENCODE az64
	,aging_days NUMERIC(15,0)   ENCODE az64
	,aging_bucket VARCHAR(10)   ENCODE lzo
	,received_date_time VARCHAR(19)   ENCODE lzo
	,serial_lot_number VARCHAR(80)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,extended_cost NUMERIC(28,10)   ENCODE az64
	,percent_aging NUMERIC(28,10)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

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
	is_deleted_flg,
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
	
commit;