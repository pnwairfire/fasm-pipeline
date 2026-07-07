with active_outlooks as (
    select
        cf.collection_id,
        cf.display_name,
        cf.author_name,
        cf.forecast_date,
        cf.north,
        cf.south,
        cf.east,
        cf.west,
        cf.creation_time,
        'LINESTRING(' || cf.west::varchar || ' ' || cf.north::varchar ||
        ',' || cf.east::varchar || ' ' || cf.north::varchar ||
        ',' || cf.east::varchar || ' ' || cf.south::varchar ||
        ',' || cf.west::varchar || ' ' || cf.south::varchar ||
        ',' || cf.west::varchar || ' ' || cf.north::varchar ||
        ')' AS linestring
    from outlook_v7.collection_forecast cf
    where cf.publication_time is not null
    and cf.forecast_date::date >= (now() at time zone 'US/Mountain')::date - interval '24 hours'
    and cf.objectid in (
        select MAX(objectid)
        from outlook_v7.collection_forecast
        where publication_time is not null
        group by collection_id
    )
    order by cf.collection_id, cf.publication_time desc
),

unique_outlook_areas as (
    select distinct on (collection_id)
        collection_id as outlook_path,
        to_char(TO_DATE(forecast_date,'YYYYMMDD'), 'MM/DD/YYYY') as forecast_date,
        author_name as author,
        display_name as region_title,
        creation_time as create_date_utc,
        ST_MakePolygon( ST_GeomFromText(linestring, 4326)) as shape
    from active_outlooks
)

select row_to_json(gjson_fc)
from (
    select
        'FeatureCollection' as "type",
        array_to_json(array_agg(f)) as "features"
    from (
        select
            'Feature' as "type",
            ST_AsGeoJSON(ST_Transform(shape, 4326), 6) :: json as "geometry",
            (
                select json_strip_nulls(row_to_json(t))
                from (
                    select
                        outlook_path,
                        forecast_date,
                        author,
                        region_title,
                        create_date_utc
                ) t
            ) as "properties"
        from unique_outlook_areas
    ) as f
) as gjson_fc;
