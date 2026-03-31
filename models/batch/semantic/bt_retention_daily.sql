select *
from {{ wap_source('gold', 'batch_retention_daily') }} as _src
