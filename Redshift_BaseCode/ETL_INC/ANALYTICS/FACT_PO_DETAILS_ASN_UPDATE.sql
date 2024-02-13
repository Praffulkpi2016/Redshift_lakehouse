/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents update the column for Fact table.
# File Version: KPI v1.0
*/ 
begin;

update bec_dwh.fact_po_details 
set asn_po_header_id = shp.po_header_id
from (select distinct po_header_id from bec_dwh.fact_po_shipment) shp
where fact_po_details.po_header_id = shp.po_header_id ;
commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_details_asn_update'
	and batch_name = 'po';

commit;