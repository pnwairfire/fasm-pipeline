select
    fasm_fire_id,
    irwin_id,
    incident_name,
    incident_type,
    cumulative_acres,
    cumulative_ha,
    last_modified,
    geom
from fire_summary.fasm_fire_perimeters;