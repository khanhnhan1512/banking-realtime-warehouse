{% snapshot snap_accounts %}

{{
    config(
        target_schema='snapshots',
        unique_key='account_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('stg_accounts') }}

{% endsnapshot %}