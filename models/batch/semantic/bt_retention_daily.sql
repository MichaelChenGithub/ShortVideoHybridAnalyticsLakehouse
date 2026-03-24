select *
from {{ source('gold', 'batch_retention_daily') }}
