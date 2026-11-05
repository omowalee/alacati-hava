-- The single most recent complete ingestion run (empty while there is none).
select
    run_id,
    started_at
from {{ ref('stg_complete_runs') }}
qualify row_number() over (order by started_at desc, run_id desc) = 1
