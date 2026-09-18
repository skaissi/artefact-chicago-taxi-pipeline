-- Execute only AFTER promoting the six physical parquet folders in Dremio.
-- Create Spaces ChicagoTaxi and ChicagoTaxi_Operations in Dremio UI FIRST.
-- Check actual dataset paths by opening the dataset SQL editor; source configured as chicago_lake.
CREATE OR REPLACE VIEW ChicagoTaxi.daily_revenue AS
SELECT * FROM chicago_lake."taxi-datalake".gold.chicago_taxi.daily_revenue;
CREATE OR REPLACE VIEW ChicagoTaxi.daily_trip_metrics AS
SELECT * FROM chicago_lake."taxi-datalake".gold.chicago_taxi.daily_trip_metrics;
CREATE OR REPLACE VIEW ChicagoTaxi.top_pickup_zones AS
SELECT * FROM chicago_lake."taxi-datalake".gold.chicago_taxi.top_pickup_zones;
CREATE OR REPLACE VIEW ChicagoTaxi_Operations.rejected_records AS
SELECT * FROM chicago_lake."taxi-datalake".quality.chicago_taxi.rejected_records;
CREATE OR REPLACE VIEW ChicagoTaxi_Operations.quality_batch AS
SELECT * FROM chicago_lake."taxi-datalake".quality.chicago_taxi.quality_batch;
CREATE OR REPLACE VIEW ChicagoTaxi_Operations.quality_reasons AS
SELECT * FROM chicago_lake."taxi-datalake".quality.chicago_taxi.quality_reasons;
-- Explore rejects safely:
-- SELECT batch_id, reject_type, reject_reason, COUNT(*) AS rows_n
-- FROM ChicagoTaxi_Operations.rejected_records
-- GROUP BY batch_id, reject_type, reject_reason ORDER BY rows_n DESC;
