{{ config(materialized='table') }}
select '{{ var("target_month") }}' as test