SELECT
    *
FROM `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker`;

SELECT
    *
FROM `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker`
WHERE rtl_flag = 1;

SELECT
    *
FROM `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker`
WHERE whs_flag = 1;
