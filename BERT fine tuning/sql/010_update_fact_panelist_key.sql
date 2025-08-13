UPDATE fact_retailer_events f
SET panelist_key = d.id
FROM dim_panelist d
WHERE f.panelist_id = d.panelist_id;
