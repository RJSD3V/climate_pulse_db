select
    state_code,
    state_name

from {{ source('ghcn', 'ghcn_states') }}
