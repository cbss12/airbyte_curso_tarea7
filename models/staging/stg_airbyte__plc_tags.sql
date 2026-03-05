-- stg_airbyte__plc_tags.sql
-- Limpieza y estandarización de las variables PLC
-- Source: airbyte_curso.main.plc_tags (16 registros)

with source as (

    select * from {{ source('airbyte_raw', 'plc_tags') }}

),

renamed as (

    select
        -- Identificador natural de la variable
        name                                        as tag_name,

        -- Atributos de la variable
        data_type,
        logical_address,
        path                                        as path_description,

        -- Extraer el prefijo de la dirección lógica para clasificar la zona
        -- %I = Entrada, %Q = Salida, %M = Marca interna
        left(logical_address, 2)                    as address_prefix,

        -- Castear TEXT a BOOLEAN
        (hmi_visible = 'True')                      as is_hmi_visible,
        (hmi_accessible = 'True')                   as is_hmi_accessible,

        -- Normalizar campo comment: NULL → string vacío
        coalesce(comment, '')                       as tag_comment,

        -- Metadatos de Airbyte
        _airbyte_raw_id,
        _airbyte_extracted_at                       as extracted_at

    from source

)

select * from renamed
