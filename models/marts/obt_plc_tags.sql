-- obt_plc_tags.sql
-- One Big Table para análisis y dashboards del sistema PLC.
-- Tabla final desnormalizada lista para conectar con Metabase u otras
-- herramientas BI. Sin necesidad de JOINs en las queries de análisis.

with plc_enriched as (

    select * from {{ ref('int_plc_tags_enriched') }}

)

select
    -- === IDENTIFICACIÓN ===
    tag_name,
    logical_address,

    -- === CLASIFICACIÓN DE ZONA ===
    address_prefix,
    zone_label,
    zone_description,
    address_byte,
    address_bit,

    -- === TIPO DE DATO ===
    data_type,

    -- === ACCESO HMI ===
    is_hmi_visible,
    is_hmi_accessible,
    hmi_access_label,

    -- === CONTROL ===
    is_control_critical,

    -- === DOCUMENTACIÓN ===
    path_description,
    tag_comment,

    -- === METADATA ===
    extracted_at,
    current_timestamp                               as transformed_at

from plc_enriched
order by zone_label, address_byte, address_bit
