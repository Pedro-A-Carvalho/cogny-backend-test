DROP VIEW IF EXISTS ${schema:raw}.vw_population_sum CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.vw_population_sum AS
WITH pop_year AS (
    SELECT 
        (jsonb_array_elements(doc_record)->>'Population')::INTEGER AS population,
        (jsonb_array_elements(doc_record)->>'Year')::INTEGER AS year
    FROM 
        ${schema:raw}.api_data
)
SELECT 
    SUM(population) AS total_population
FROM 
    pop_year
WHERE 
    year IN (2018, 2019, 2020)
;