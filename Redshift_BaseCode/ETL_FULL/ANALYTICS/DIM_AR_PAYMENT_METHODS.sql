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

drop table if exists bec_dwh.DIM_AR_PAYMENT_METHODS;

create table bec_dwh.DIM_AR_PAYMENT_METHODS diststyle all sortkey (RECEIPT_METHOD_ID,RECEIPT_CLASS_ID)
as
(
select
	ARM.RECEIPT_METHOD_ID RECEIPT_METHOD_ID
,
	ARM.NAME PAYMENT_METHOD_NAME 
,
	ARM.RECEIPT_CLASS_ID RECEIPT_CLASS_ID
,
	ARC.NAME RECEIPT_CLASS_NAME
,
	ARC.CREATION_METHOD_CODE RECEIPT_CREATION_METHOD
,
	ARC.REMIT_FLAG
,
	ARC.CREATION_STATUS
,
	ARC.REMIT_METHOD_CODE,
	ARM.last_update_date,
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
    || '-' || nvl(ARM.RECEIPT_METHOD_ID, 0) || '-' || nvl(ARM.RECEIPT_CLASS_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.AR_RECEIPT_CLASSES ARC
,
	bec_ods.AR_RECEIPT_METHODS ARM
where
	1 = 1
	and ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID
	--AND ARM.RECEIPT_METHOD_ID = ARMAA.RECEIPT_METHOD_ID
	and ARM.END_DATE is null
);
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ar_payment_methods'
	and batch_name = 'ar';

commit;
