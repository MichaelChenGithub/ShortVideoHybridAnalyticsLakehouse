select *
from {{ source('gold', 'batch_engagement_daily') }}
