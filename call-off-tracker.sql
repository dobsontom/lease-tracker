SELECT
    lr.id,
    cob.any_other_technical_details_required_cob_c,
    lr.approval_document_id_c,
    lr.business_unit_c,
    lr.name AS end_customer,
    lr.lsp_c,
    lr.lsr_c,
    lr.project_id_c AS project_id,
    lr.project_name_c AS project_name,
    lr.contract_number_c AS ssp_number,
    lr.total_value_of_the_order_c AS total_value,
    cob.cob_end_date_c AS end_date,
    cob.cob_end_time_c,
    cob_start_date_c AS start_date,
    cob_start_time_c,
    cob.forward_bandwidth_k_hz_cob_c AS forward_bandwidth_khz,
    cob.power_d_bw_cob_c AS power_dbw,
    cob.price_plan_cob_c AS price_plan,
    cob.retail_contract_value_cob_c AS retail_block_value,
    cob.return_bandwidth_k_hz_cob_c AS return_bandwidth_khz,
    cob.id,
    cob.name AS call_off_block_name,
    cob.status_cob_c AS status,
    cob.wholesale_contract_value_cob_c AS wholesale_block_value,
    cob.daily_usage_charge_cob_c,
    cob.daily_usage_charge_retail_c,
    CONCAT(cob_start_date_c, ' ', cob_start_time_c) AS start_date_time,
    CONCAT(cob_end_date_c, ' ', cob_end_time_c) AS end_date_time,
    COALESCE(
        cob.retail_contract_value_cob_c,
        cob.wholesale_contract_value_cob_c
    ) AS call_off_block_value
FROM
    inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c lr
    INNER JOIN inm-iar-data-warehouse-dev.sdp_salesforce_src.call_off_block_c cob ON lr.id = cob.leasing_request_cob_c