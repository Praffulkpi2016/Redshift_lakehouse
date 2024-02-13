--drop table bec_install.FACT_INSTALL_FORECAST_DETAILS_SNP;
CREATE TABLE bec_install.FACT_INSTALL_FORECAST_DETAILS_SNP (
	batch_id VARCHAR(100)   ENCODE lzo 
	,forecast_designator VARCHAR(20)   ENCODE lzo
	,part_number VARCHAR(100)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,forecast_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_forecast_quantity NUMERIC(28,10)   ENCODE az64
	,available_onhand_qty NUMERIC(28,10)   ENCODE az64
	,available_po_qty NUMERIC(28,10)   ENCODE az64
	,available_plan_qty NUMERIC(28,10)   ENCODE az64
	,remaining_onhand_qty NUMERIC(28,10)   ENCODE az64
	,remaining_po_qty NUMERIC(28,10)   ENCODE az64
	,remaining_plan_qty NUMERIC(28,10)   ENCODE az64
	,total_used_qty NUMERIC(28,10)   ENCODE az64
	,snp_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
);
