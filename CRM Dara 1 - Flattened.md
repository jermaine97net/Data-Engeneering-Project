# Task 1: CRM Data 1 - Flattened + Balance Summary

## Status: STRUCTURALLY COMPLETE 

## What I Created:
- **Database**: wtc_prod
- **Schema**: prepared_layers
- **Table**: crm_flattened_balance

## Table Structure:
Flattened CRM data combining:
- Account information (account_id, owner_name, email, msisdn)
- Device information (device_id, device_name, device_type, device_os)
- Address information (street_address, city, state, postal_code, country)
- Cost calculation placeholders (ready for CDR data)

## Query:
```sql
CREATE TABLE prepared_layers.crm_flattened_balance AS
SELECT 
    a.account_id, a.owner_name, a.email, a.phone_number as msisdn,
    d.device_id, d.device_name, d.device_type, d.device_os,
    addr.street_address, addr.city, addr.state, addr.postal_code, addr.country,
    0::BIGINT as total_data_bytes,
    0::DECIMAL(10,2) as data_cost_zar,
    0::INTEGER as total_call_seconds,
    0::DECIMAL(10,2) as voice_cost_zar,
    0::DECIMAL(10,2) as total_cost_zar,
    0::DECIMAL(10,2) as running_balance_zar,
    CURRENT_TIMESTAMP as created_at
FROM crm_system.accounts a
LEFT JOIN crm_system.devices d ON a.account_id = d.account_id
LEFT JOIN crm_system.addresses addr ON a.account_id = addr.account_id;
```

## Next Steps:
- Complete Task 2 (Persistence System) to load CDR data into PostgreSQL
- Update this table with actual usage costs once CDR data is available
