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

delete from bec_dwh.DIM_GLOBAL_CONVERSION
where (NVL(FROM_CURRENCY,'NA'),NVL(TO_CURRENCY,'NA'),NVL(CONVERSION_DATE::DATE,'1900-01-01'),NVL(CONVERSION_TYPE,'NA'),NVL(CONVERSION_RATE,0)) 
in (
select 
NVL(ods.FROM_CURRENCY,'NA') as FROM_CURRENCY,
NVL(ods.TO_CURRENCY,'NA') as TO_CURRENCY,
NVL(ods.CONVERSION_DATE::DATE,'1900-01-01') as CONVERSION_DATE,
NVL(ods.CONVERSION_TYPE,'NA') as CONVERSION_TYPE,
NVL(ods.CONVERSION_RATE,0) as CONVERSION_RATE
from bec_dwh.DIM_GLOBAL_CONVERSION dw,
 (SELECT 
	GLR.FROM_CURRENCY AS FROM_CURRENCY,
	GLR.TO_CURRENCY AS TO_CURRENCY,
	GLR.CONVERSION_DATE::DATE AS CONVERSION_DATE,
	GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
	GLR.CONVERSION_RATE AS CONVERSION_RATE,
	GLR.LAST_UPDATE_DATE as LAST_UPDATE_DATE,
	GLR.kca_seq_date as kca_seq_date
 FROM BEC_ODS.GL_DAILY_RATES GLR
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'||NVL(ods.FROM_CURRENCY,'NA')
||'-'||NVL(ods.TO_CURRENCY,'NA')
||'-'||NVL(ods.CONVERSION_DATE::DATE,'1900-01-01')
||'-'||NVL(ods.CONVERSION_TYPE,'NA')
||'-'||NVL(ods.CONVERSION_RATE,0)
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_global_conversion' and batch_name = 'gl')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_GLOBAL_CONVERSION
(
from_currency,
	to_currency,
	conversion_date,
	conversion_type,
	rate_source_code,
	conversion_rate,
	inverse_conversion_rate,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
SELECT 
	GLR.FROM_CURRENCY AS FROM_CURRENCY,
	GLR.TO_CURRENCY AS TO_CURRENCY,
	GLR.CONVERSION_DATE::DATE AS CONVERSION_DATE,
	GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
	GLR.RATE_SOURCE_CODE AS RATE_SOURCE_CODE,
	GLR.CONVERSION_RATE AS CONVERSION_RATE,
	GLR.CONVERSION_RATE AS INVERSE_CONVERSION_RATE,
	-- AUDIT COLUMNS
	'N' AS is_deleted_flg,
	(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM='EBS') AS SOURCE_APP_ID,
	(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM='EBS')
	||'-'||NVL(FROM_CURRENCY,'NA')||'-'||NVL(TO_CURRENCY,'NA')||'-'||NVL(CONVERSION_DATE::DATE,'1900-01-01')||'-'||
	NVL(CONVERSION_TYPE,'NA')||'-'||NVL(CONVERSION_RATE,0) AS DW_LOAD_ID,
	GETDATE() AS DW_INSERT_DATE,
	GETDATE() AS DW_UPDATE_DATE       
FROM BEC_ODS.GL_DAILY_RATES GLR
Where 1=1
and (GLR.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_global_conversion' and batch_name = 'gl')
 )
);

-- Soft delete

update bec_dwh.DIM_GLOBAL_CONVERSION set is_deleted_flg = 'Y'
where (NVL(FROM_CURRENCY,'NA'),NVL(TO_CURRENCY,'NA'),NVL(CONVERSION_DATE::DATE,'1900-01-01'),NVL(CONVERSION_TYPE,'NA'),NVL(CONVERSION_RATE,0)
) not in (
select 
NVL(ods.FROM_CURRENCY,'NA') as FROM_CURRENCY,
NVL(ods.TO_CURRENCY,'NA') as TO_CURRENCY,
NVL(ods.CONVERSION_DATE::DATE,'1900-01-01') as CONVERSION_DATE,
NVL(ods.CONVERSION_TYPE,'NA') as CONVERSION_TYPE,
NVL(ods.CONVERSION_RATE,0) as CONVERSION_RATE 
from bec_dwh.DIM_GLOBAL_CONVERSION dw, 
(SELECT 
	GLR.FROM_CURRENCY AS FROM_CURRENCY,
	GLR.TO_CURRENCY AS TO_CURRENCY,
	GLR.CONVERSION_DATE::DATE AS CONVERSION_DATE,
	GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
	GLR.CONVERSION_RATE AS CONVERSION_RATE
 FROM bec_ods.GL_DAILY_RATES GLR
 where GLR.is_deleted_flg <> 'Y') ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'||NVL(ods.FROM_CURRENCY,'NA')
||'-'||NVL(ods.TO_CURRENCY,'NA')
||'-'||NVL(ods.CONVERSION_DATE::DATE,'1900-01-01')
||'-'||NVL(ods.CONVERSION_TYPE,'NA')
||'-'||NVL(ods.CONVERSION_RATE,0)  
);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_global_conversion'
	and batch_name = 'gl';

COMMIT;