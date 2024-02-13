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

drop table if exists bec_dwh.DIM_AP_INVOICE_POS;

create table bec_dwh.dim_ap_invoice_pos
DISTKEY (invoice_id)
SORTKEY (invoice_id, POSTED_FLAG)
as
SELECT invoice_id, MAX(POSTED_FLAG) AS POSTED_FLAG 
FROM bec_ods.ap_invoice_distributions_all 
WHERE is_deleted_flg <> 'Y' 
GROUP BY invoice_id;
;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ap_invoice_pos'
	and batch_name = 'ap';

commit;