{% snapshot address_info %}

{{
    config(
        target_schema='fds',
        unique_key='user_id',
        strategy='check',
        check_cols=["name", "address", "city", "state", "postal_code"],
        invalidate_hard_deletes=True,
        post_hook=['update {{this}} set active_Rec_ind=0 where DBT_VALID_TO is not null', 'update {{this}} set active_Rec_ind=1 where DBT_VALID_TO is  null' ]
    )
}}

select 
user_id, name, address, city, state, postal_code, last_updated, 1 as active_rec_ind
From
{{ source('udl','address_info')  }}


{% endsnapshot %} 