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
BEGIN;
	-- Delete Records
	DELETE
	FROM
	bec_dwh.FACT_INV_RELATED_ITEMS
	WHERE
	(
		(nvl(RELATED_ITEM_ID, 0) )
	) IN 
	(
		SELECT
		nvl(ODS.RELATED_ITEM_ID,0) as RELATED_ITEM_ID  
		
		FROM
		bec_dwh.FACT_INV_RELATED_ITEMS dw,
		( 
			SELECT 
       	 	RELATED_ITEM_ID	
			from
			bec_ods.mtl_related_items 
			
			where ( kca_seq_date > (
				SELECT
				(executebegints - prune_days)
				FROM
				bec_etl_ctrl.batch_dw_info
				WHERE
				dw_table_name = 'fact_inv_related_items'
			AND batch_name = 'inv') 
			or  is_deleted_flg = 'Y' 
			)	
			
			
		) ODS
		WHERE
		dw.dw_load_id = 
		(
			SELECT
			system_id
			FROM
			bec_etl_ctrl.etlsourceappid
			WHERE
		source_system = 'EBS')    
		|| '-' || nvl(ods.RELATED_ITEM_ID, 0)  
	);
	COMMIT;
	
	-- Insert records
	
	INSERT
	INTO
	bec_dwh.FACT_INV_RELATED_ITEMS
	(
		inventory_item_id,
		organization_id,
		related_item_id,
		relationship_type_id,
		reciprocal_flag,
		planning_enabled_flag,
		start_date,
		end_date,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		inventory_item_id_key,
		organization_id_key,
		related_item_id_key,
		relationship_type_id_key,
		source_app_id,
		dw_load_id,
		dw_insert_date,
		dw_update_date
	)	
	( 
		 SELECT 
       	
		INVENTORY_ITEM_ID 
        ,ORGANIZATION_ID 
        ,RELATED_ITEM_ID 
        ,relationship_type_id
        ,reciprocal_flag
        ,PLANNING_ENABLED_FLAG 
        ,START_DATE 
        ,END_DATE 
        ,CREATION_DATE 
        ,CREATED_BY 
        ,LAST_UPDATE_DATE 
        ,LAST_UPDATED_BY
		,(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' || INVENTORY_ITEM_ID  INVENTORY_ITEM_ID_KEY,
        (SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' || ORGANIZATION_ID    ORGANIZATION_ID_KEY,
        (SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' || RELATED_ITEM_ID        RELATED_ITEM_ID_KEY,
        (SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||RELATIONSHIP_TYPE_ID   RELATIONSHIP_TYPE_ID_KEY, 		
        (
			SELECT
			SYSTEM_ID
			FROM
			BEC_ETL_CTRL.ETLSOURCEAPPID
			WHERE
			SOURCE_SYSTEM = 'EBS'
		) AS SOURCE_APP_ID,
		(
			SELECT
			SYSTEM_ID
			FROM
			BEC_ETL_CTRL.ETLSOURCEAPPID
			WHERE
			SOURCE_SYSTEM = 'EBS'
		)
 		|| '-' || NVL(RELATED_ITEM_ID, 0)  AS DW_LOAD_ID, 
		GETDATE() AS DW_INSERT_DATE,
		GETDATE() AS DW_UPDATE_DATE			
		from
		( select * from bec_ods.mtl_related_items where is_deleted_flg <> 'Y' )
			
			
			where  ( kca_seq_date > (
				SELECT
				(executebegints - prune_days)
				FROM
				bec_etl_ctrl.batch_dw_info
				WHERE
				dw_table_name = 'fact_inv_related_items'
			AND batch_name = 'inv') )
			 
		 );
		
		commit;	
END;

UPDATE
bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name = 'fact_inv_related_items'
AND batch_name = 'inv';