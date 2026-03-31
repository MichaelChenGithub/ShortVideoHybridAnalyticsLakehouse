select *
from {{ wap_source('gold', 'batch_engagement_daily') }} as _src
