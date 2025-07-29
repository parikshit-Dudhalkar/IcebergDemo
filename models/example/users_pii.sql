{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = 'user_id',
        schema='fds',
        tags='user_pii',

    )
}}


select
src.user_id,
src.full_name,
src.email,
src.phone_number,
src.address,
src.date_of_birth,
current_timestamp as  etl_insert_time ,
src.etl_update_time 

From {{ source('udl','users_pii') }} as src

{% if  is_incremental() %}

 left join
{{ this }} as tgt 
 on src.user_id=tgt.user_id
where lower(src.full_name)<>lower(tgt.full_name)
or lower(src.phone_number)<>lower(tgt.phone_number)


{% endif %}


 
 