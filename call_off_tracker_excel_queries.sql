-- Populates Raw Data tab
SELECT
    *
FROM `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker_base`;

-- Populates Retail Data tab
SELECT
    *
FROM `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker_base`
WHERE rtl_flag = 1;

-- Populates Wholesale Data tab
SELECT
    *
FROM `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker_base`
WHERE whs_flag = 1;
