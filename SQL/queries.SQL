-- Considerando únicamente la plataforma de Netflix, ¿qué actor aparece más veces?
SELECT UNNEST(STRING_TO_ARRAY(ccast, ',')) AS actor, COUNT(*) AS apariciones
FROM team
WHERE catalog_name = 'Netflix'
GROUP BY actor
ORDER BY apariciones DESC
LIMIT 1;

-- Top 10 de actores participantes considerando ambas plataformas en el año actual. Se aprecia flexibilidad.
SELECT UNNEST(STRING_TO_ARRAY(ccast, ',')) AS actor, COUNT(*) AS apariciones
FROM team JOIN year AS y 
ON team.show_id = y.show_id and team.catalog_name = y.catalog_name
WHERE EXTRACT( 'Year' FROM date_added) = EXTRACT('Year' FROM CURRENT_DATE)
GROUP BY actor
ORDER BY apariciones DESC
LIMIT 10;

-- Crear  un  Stored  Proceadure  que  tome  como  parámetro  un  año  y devuelva una tabla con las 5 películas con mayor duración en minutos.
-- Nota: un stored procedure no puede retornar una tabla o realizar una consulta, en este caso se utiliza function.
CREATE OR REPLACE FUNCTION top5_peliculas_mayor_duracion_año(año INT)
RETURNS TABLE (show_id VARCHAR(10), title VARCHAR(180), minutos INT, catalog_name VARCHAR(20))
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY 
    SELECT s.show_id, s.title, substring(s.duration, 1, length(s.duration)-4)::INTEGER AS minutos, s.catalog_name 
    FROM show AS s
    JOIN year AS y
    ON s.show_id = y.show_id and s.catalog_name = y.catalog_name
    JOIN category AS c
    ON s.show_id = c.show_id and s.catalog_name = c.catalog_name
    WHERE type = 'Movie' AND release_year = año
    ORDER BY minutos DESC
    LIMIT 5;
END;
$$;

-- Para probar function:
SELECT * FROM top5_peliculas_mayor_duracion_año(2021);