-- int_plc_tags_enriched.sql
-- Enriquece las variables PLC con clasificaciones de negocio derivadas
-- de la dirección lógica. Este modelo prepara los datos para los marts.

with plc_tags as (

    select * from {{ ref('stg_airbyte__plc_tags') }}

),

enriched as (

    select
        tag_name,
        data_type,
        logical_address,
        address_prefix,
        is_hmi_visible,
        is_hmi_accessible,
        path_description,
        tag_comment,
        extracted_at,

        -- Clasificar la zona del PLC según el prefijo de la dirección lógica
        case address_prefix
            when '%I' then 'Entrada'
            when '%Q' then 'Salida'
            when '%M' then 'Marca'
            else 'Desconocida'
        end                                         as zone_label,

        -- Descripción extendida de la zona
        case address_prefix
            when '%I' then 'Señal de entrada digital desde sensor o botón'
            when '%Q' then 'Señal de salida digital hacia actuador o indicador'
            when '%M' then 'Variable interna de memoria del PLC'
            else 'Zona no clasificada'
        end                                         as zone_description,

        -- Extraer solo el número de byte de la dirección (ej. %Q0.4 → 0)
        try_cast(
            regexp_extract(logical_address, '[A-Z]+(\d+)\.\d+', 1)
        as integer)                                 as address_byte,

        -- Extraer el bit de la dirección (ej. %Q0.4 → 4)
        try_cast(
            regexp_extract(logical_address, '\d+\.(\d+)$', 1)
        as integer)                                 as address_bit,

        -- Flag: variable de control crítica (en el sistema, variables de
        -- arranque/parada son consideradas críticas)
        case
            when tag_name in ('MOTOR', 'INICIO', 'STOP', 'AUXINICIO')
            then true
            else false
        end                                         as is_control_critical,

        -- Etiqueta de visibilidad HMI combinada
        case
            when is_hmi_visible and is_hmi_accessible then 'Visible y Accesible'
            when is_hmi_visible and not is_hmi_accessible then 'Solo Lectura HMI'
            when not is_hmi_visible and is_hmi_accessible then 'Accesible sin Visualizar'
            else 'Sin Acceso HMI'
        end                                         as hmi_access_label

    from plc_tags

)

select * from enriched
