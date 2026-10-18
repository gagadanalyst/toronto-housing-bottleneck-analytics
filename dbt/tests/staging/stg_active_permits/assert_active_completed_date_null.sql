select * from
    {{ref('stg_active_permits')}}
where completed_date is not null
