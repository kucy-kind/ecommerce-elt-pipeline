{{
    config(
        materialized='incremental',
        unique_key=['stat_year', 'user_id'],
        schema='ads'
    )
}}

{% set target_date = var('target_date') %}
{% if target_date is not none %}
    {% set stat_year = target_date[0:4] | int - 1 %}
    {% set year_start = stat_year ~ '-01-01' %}
    {% set year_end = (stat_year + 1) ~ '-01-01' %}

    with user_monthly_behavior as (
        select *
        from {{ ref('ads_user_monthly_snapshot') }}
        where stat_month >= '{{ year_start }}'
          and stat_month < '{{ year_end }}'
    ),

    user_monthly_all as (
        select *
        from {{ ref('ads_user_monthly_snapshot') }}
        where stat_month >= '{{ year_start }}'
          and stat_month <= '{{ year_end }}'
    ),

    yearly_behavior as (
        select
            user_id,
            sum(month_login_days) as year_login_days,
            sum(month_view_cnt) as year_view_cnt,
            sum(month_order_cnt) as year_order_cnt,
            sum(month_order_amount) as year_order_amount,
            sum(month_return_cnt) as year_return_cnt
        from user_monthly_behavior
        group by user_id
    ),

    yearly_cumulative as (
        select
            user_id,
            total_order_amount,
            total_order_count,
            total_return_count
        from user_monthly_all
        where stat_month = '{{ year_end }}'
    ),

    monthly_levels as (
        select
            user_id,
            stat_month,
            user_level
        from user_monthly_behavior
    ),

    yearly_level as (
        select
            user_id,
            count(case when user_level = '优质' then 1 end) as cnt_quality,
            count(case when user_level in ('优质', '高潜质') then 1 end) as cnt_high_potential,
            count(case when user_level in ('优质', '高潜质', '中潜质') then 1 end) as cnt_potential,
            count(*) as total_months,
            case
                when count(case when user_level = '优质' then 1 end) >= 9
                     and count(case when user_level in ('中潜质', '普通') then 1 end) = 0
                then '年度优质用户'
                when count(case when user_level in ('优质', '高潜质') then 1 end) >= 9
                     and count(case when user_level = '普通' then 1 end) = 0
                then '年度高潜质用户'
                when count(case when user_level in ('优质', '高潜质', '中潜质') then 1 end) >= 9
                then '年度潜质用户'
                else '普通用户'
            end as user_level
        from monthly_levels
        group by user_id
    ),

    yearly_risk as (
        select
            user_id,
            count(case when risk_flag != '正常' then 1 end) as risk_month_cnt
        from user_monthly_behavior
        group by user_id
    ),

    user_reg as (
        select
            u.user_id,
            u.user_name,
            dws_u.register_date,
            year(register_date) as register_year
        from {{ source('dwd', 'users') }} u 
        left join {{ ref('dws_user_profile') }} dws_u on u.user_id=dws_u.user_id
    ),

    historical_risk as (
        select
            user_id,
            stat_year,
            risk_flag as prev_risk_flag
        from {{ this }}
        where stat_year < {{ stat_year }}
    ),

    base_risk as (
        select
            yr.user_id,
            coalesce(yr.risk_month_cnt, 0) as risk_month_cnt,
            case
                when coalesce(yr.risk_month_cnt, 0) > 3 then '高风险'
                else '正常'
            end as base_flag
        from yearly_risk yr
    ),

    risk_with_history as (
        select
            br.user_id,
            br.risk_month_cnt,
            br.base_flag,
            hr1.prev_risk_flag as prev1_flag,
            hr2.prev_risk_flag as prev2_flag,
            u.register_year
        from base_risk br
        left join historical_risk hr1 on br.user_id = hr1.user_id and hr1.stat_year = {{ stat_year - 1 }}
        left join historical_risk hr2 on br.user_id = hr2.user_id and hr2.stat_year = {{ stat_year - 2 }}
        left join user_reg u on br.user_id = u.user_id
    ),

    risk_final as (
        select
            user_id,
            case
                when register_year = {{ stat_year }} then
                    case when risk_month_cnt > 3 then '高风险' else '正常' end
                when register_year = {{ stat_year }} - 1 then
                    case
                        when prev1_flag in ('高风险', '超级高风险') then
                            case when risk_month_cnt > 1 then '高风险' else '正常' end
                        else
                            case when risk_month_cnt > 3 then '高风险' else '正常' end
                    end
                when register_year <= {{ stat_year }} - 2 then
                    case
                        when (prev1_flag in ('高风险', '超级高风险') and
                              prev2_flag in ('高风险', '超级高风险')) then
                            case when risk_month_cnt > 0 then '超级高风险' else '正常' end
                        else
                            case when risk_month_cnt > 3 then '高风险' else '正常' end
                    end
                else '正常'
            end as risk_flag
        from risk_with_history
    ),

    category_split as (
        select
            um.user_id,
            trim(substring_index(substring_index(um.preferred_category, ',', numbers.n), ',', -1)) as category
        from user_monthly_behavior um
        cross join (
            select 1 as n union all select 2 union all select 3 union all select 4 union all select 5
        ) numbers
        on numbers.n <= length(um.preferred_category) - length(replace(um.preferred_category, ',', '')) + 1
        where um.preferred_category is not null and um.preferred_category != ''
    ),

    category_cnt as (
        select
            user_id,
            category,
            count(*) as cnt
        from category_split
        where category is not null and category != ''
        group by user_id, category
    ),

    top_categories as (
        select
            user_id,
            group_concat(category order by cnt desc, category separator ',') as preferred_category
        from (
            select
                user_id,
                category,
                cnt,
                row_number() over (partition by user_id order by cnt desc, category) as rn
            from category_cnt
        ) t
        where rn <= 5
        group by user_id
    ),

    lifecycle as (
        select
            ub.user_id,
            case
                when coalesce(um_last3.order_cnt, 0) = 0 then '流失'
                when yl.user_level in ('优质', '高潜质') then '活跃'
                else '沉睡'
            end as lifecycle_stage
        from user_reg ub
        left join (
            select user_id, sum(month_order_cnt) as order_cnt
            from user_monthly_behavior
            where month(stat_month) >= 10
            group by user_id
        ) um_last3 on ub.user_id = um_last3.user_id
        left join yearly_level yl on ub.user_id = yl.user_id
    )

    select
        {{ stat_year }} as stat_year,
        u.user_id,
        u.user_name,
        u.register_date,
        coalesce(yl.user_level, '普通') as user_level,
        coalesce(yc.total_order_amount, 0) as total_order_amount,
        coalesce(yc.total_order_count, 0) as total_order_count,
        coalesce(yc.total_return_count, 0) as total_return_count,
        coalesce(yb.year_login_days, 0) as year_login_days,
        coalesce(yb.year_view_cnt, 0) as year_view_cnt,
        coalesce(yb.year_order_cnt, 0) as year_order_cnt,
        coalesce(yb.year_order_amount, 0) as year_order_amount,
        coalesce(yb.year_return_cnt, 0) as year_return_cnt,
        case
            when coalesce(yb.year_order_cnt, 0) = 0 then 0
            else yb.year_order_amount / yb.year_order_cnt
        end as year_avg_order_amount,
        coalesce(tc.preferred_category, '未知') as preferred_category,
        coalesce(lc.lifecycle_stage, '其他') as lifecycle_stage,
        coalesce(rf.risk_flag, '正常') as risk_flag,
        now() as etl_time
    from user_reg u
    left join yearly_behavior yb on u.user_id = yb.user_id
    left join yearly_cumulative yc on u.user_id = yc.user_id
    left join yearly_level yl on u.user_id = yl.user_id
    left join risk_final rf on u.user_id = rf.user_id
    left join top_categories tc on u.user_id = tc.user_id
    left join lifecycle lc on u.user_id = lc.user_id

{% else %}
    {{ log("WARNING: target_date not provided for ads_user_yearly_snapshot, returning empty result", info=True) }}
    select * from (select 1 as dummy) where 1=0
{% endif %}