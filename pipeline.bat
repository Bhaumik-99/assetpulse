@echo off
setlocal

echo ========================================
echo AssetPulse Industrial Data Pipeline
echo ========================================

echo.
echo [1/6] Ingesting CMAPSS sensor data...
python -m src.ingestion.cmapss_ingestor
if errorlevel 1 goto :error

echo.
echo [2/6] Validating bronze layer...
python -m src.validation.quality_runner --layer bronze
if errorlevel 1 goto :error

echo.
echo [3/6] Transforming bronze to silver...
python -m src.transformations.bronze_to_silver
if errorlevel 1 goto :error

echo.
echo [4/6] Validating silver layer...
python -m src.validation.quality_runner --layer silver
if errorlevel 1 goto :error

echo.
echo [5/6] Engineering predictive maintenance features...
python -m src.transformations.feature_engineering
if errorlevel 1 goto :error

echo.
echo [6/6] Calculating equipment health metrics...
python -m src.transformations.health_metrics
if errorlevel 1 goto :error

echo.
echo ========================================
echo Pipeline complete.
echo ========================================
exit /b 0

:error
echo.
echo ========================================
echo Pipeline FAILED.
echo Check the error above.
echo ========================================
exit /b 1