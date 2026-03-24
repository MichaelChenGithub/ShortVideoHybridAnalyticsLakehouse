with retention_failures as (
    select
        'bt_retention_daily' as model_name,
        'cohort_users_negative' as check_name,
        cast(cohort_date as varchar) as key_1,
        cast(day_n as varchar) as key_2,
        category as key_3,
        region as key_4,
        new_vs_returning_user as key_5
    from {{ ref('bt_retention_daily') }}
    where cohort_users < 0

    union all

    select
        'bt_retention_daily',
        'retained_users_negative',
        cast(cohort_date as varchar),
        cast(day_n as varchar),
        category,
        region,
        new_vs_returning_user
    from {{ ref('bt_retention_daily') }}
    where retained_users < 0

    union all

    select
        'bt_retention_daily',
        'retained_users_gt_cohort_users',
        cast(cohort_date as varchar),
        cast(day_n as varchar),
        category,
        region,
        new_vs_returning_user
    from {{ ref('bt_retention_daily') }}
    where retained_users > cohort_users

    union all

    select
        'bt_retention_daily',
        'retention_rate_non_null_when_cohort_users_zero',
        cast(cohort_date as varchar),
        cast(day_n as varchar),
        category,
        region,
        new_vs_returning_user
    from {{ ref('bt_retention_daily') }}
    where cohort_users = 0
      and retention_rate is not null

    union all

    select
        'bt_retention_daily',
        'retention_rate_null_when_cohort_users_positive',
        cast(cohort_date as varchar),
        cast(day_n as varchar),
        category,
        region,
        new_vs_returning_user
    from {{ ref('bt_retention_daily') }}
    where cohort_users > 0
      and retention_rate is null

    union all

    select
        'bt_retention_daily',
        'retention_rate_formula_mismatch',
        cast(cohort_date as varchar),
        cast(day_n as varchar),
        category,
        region,
        new_vs_returning_user
    from {{ ref('bt_retention_daily') }}
    where cohort_users > 0
      and retention_rate is not null
      and abs(retention_rate - (retained_users * 1.0 / cohort_users)) > 1e-9
),
engagement_failures as (
    select
        'bt_engagement_daily' as model_name,
        'negative_count_field' as check_name,
        cast(data_date as varchar) as key_1,
        category as key_2,
        region as key_3,
        new_vs_returning_user as key_4,
        'counts' as key_5
    from {{ ref('bt_engagement_daily') }}
    where impressions < 0
       or play_start < 0
       or play_finish < 0
       or likes < 0
       or shares < 0
       or skips < 0

    union all

    select
        'bt_engagement_daily',
        'rate_out_of_range',
        cast(data_date as varchar),
        category,
        region,
        new_vs_returning_user,
        'rates'
    from {{ ref('bt_engagement_daily') }}
    where play_start_rate < 0 or play_start_rate > 1
       or completion_rate < 0 or completion_rate > 1
       or interaction_rate < 0 or interaction_rate > 1
       or skip_rate < 0 or skip_rate > 1

    union all

    select
        'bt_engagement_daily',
        'play_start_rate_formula_mismatch',
        cast(data_date as varchar),
        category,
        region,
        new_vs_returning_user,
        'play_start_rate'
    from {{ ref('bt_engagement_daily') }}
    where abs(play_start_rate - (play_start * 1.0 / greatest(impressions, 1))) > 1e-9

    union all

    select
        'bt_engagement_daily',
        'completion_rate_formula_mismatch',
        cast(data_date as varchar),
        category,
        region,
        new_vs_returning_user,
        'completion_rate'
    from {{ ref('bt_engagement_daily') }}
    where abs(completion_rate - (play_finish * 1.0 / greatest(play_start, 1))) > 1e-9

    union all

    select
        'bt_engagement_daily',
        'interaction_rate_formula_mismatch',
        cast(data_date as varchar),
        category,
        region,
        new_vs_returning_user,
        'interaction_rate'
    from {{ ref('bt_engagement_daily') }}
    where abs(interaction_rate - ((likes + shares) * 1.0 / greatest(play_finish, 1))) > 1e-9

    union all

    select
        'bt_engagement_daily',
        'skip_rate_formula_mismatch',
        cast(data_date as varchar),
        category,
        region,
        new_vs_returning_user,
        'skip_rate'
    from {{ ref('bt_engagement_daily') }}
    where abs(skip_rate - (skips * 1.0 / greatest(play_start, 1))) > 1e-9
),
sessionization_failures as (
    select
        'bt_sessionization_daily' as model_name,
        'negative_metric_field' as check_name,
        cast(data_date as varchar) as key_1,
        category as key_2,
        region as key_3,
        new_vs_returning_user as key_4,
        'metrics' as key_5
    from {{ ref('bt_sessionization_daily') }}
    where sessions < 0
       or sessions_per_user < 0
       or avg_session_duration_sec < 0
       or events_per_session < 0
       or watch_time_per_session_ms < 0
)
select * from retention_failures
union all
select * from engagement_failures
union all
select * from sessionization_failures
