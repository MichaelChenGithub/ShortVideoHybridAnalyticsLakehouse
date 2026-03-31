select *
from {{ wap_source('gold', 'batch_sessionization_daily') }} as _src
