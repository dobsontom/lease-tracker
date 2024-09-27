CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_wholesale_external_data` AS (
    SELECT
        lsp AS `LSP`,
        special_comments AS `Special_Comments__c`,
        new_ssp_number AS `New SSP`,
        ssp_number AS `SSP number`,
        wholesale_pricing_comments AS `Wholesale Pricing Comments`
    FROM `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    WHERE wholesale_external_flag = 1
);

CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_segovia_data` AS (
    SELECT
        lsp AS `LSP`,
        special_comments AS `Special_Comments__c`,
        new_ssp_number AS `New SSP`,
        ssp_number AS `SSP number`,
        wholesale_pricing_comments AS `Wholesale Pricing Comments`
    FROM `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    WHERE segovia_flag = 1
);

CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_retail_data` AS (
    SELECT
        lsp AS `LSP`,
        special_comments AS `Special_Comments__c`,
        ssp_number AS `SSP number`,
        wholesale_pricing_comments AS `Wholesale Pricing Comments`
    FROM `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    WHERE retail_flag = 1
);

CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_gx_wholesale_external_data` AS (
    SELECT
        lsp AS `LSP`,
        special_comments AS `Special_Comments__c`,
        new_ssp_number AS `New SSP`,
        ssp_number AS `SSP number`,
        wholesale_pricing_comments AS `Wholesale Pricing Comments`
    FROM `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    WHERE gx_wholesale_external_flag = 1
);


CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_gx_segovia_data` AS (
    SELECT
        lsp AS `LSP`,
        special_comments AS `Special_Comments__c`,
        new_ssp_number AS `New SSP`,
        ssp_number AS `SSP number`,
        wholesale_pricing_comments AS `Wholesale Pricing Comments`
    FROM `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base`
    WHERE gx_segovia_flag = 1
);
