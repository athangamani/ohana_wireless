SELECT COUNT(*) FROM ohana.topology;

SELECT COUNT(*) FROM ohana.events_calendar;

INVALIDATE METADATA ohana.cell_neighbors;

select count(*) from ohana.cell_neighbors;


SELECT market, COUNT(*) AS cell_sectors,
       COUNT(DISTINCT site_id) AS sites,
       SUM(CASE WHEN special_venue_id IS NOT NULL THEN 1 ELSE 0 END) AS venue_cells,
       COUNT(DISTINCT carrier_technology) AS tech_types
FROM   ohana.topology
GROUP BY market ORDER BY market;


SELECT event_category, COUNT(*) AS n_events,
       AVG(expected_attendance) AS avg_attendance,
       AVG(surge_multiplier_estimate) AS avg_surge
FROM   ohana.events_calendar
GROUP BY event_category ORDER BY n_events DESC;

SELECT COUNT(*) FROM ohana.pm_curated;

SELECT COUNT(*) FROM ohana.events_calendar;

