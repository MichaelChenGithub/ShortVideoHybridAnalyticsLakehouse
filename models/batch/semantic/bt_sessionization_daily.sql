select *
from {{ source('gold', 'batch_sessionization_daily') }}
