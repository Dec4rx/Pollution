CREATE TABLE pollution_data (
  state text NOT NULL,
  city text NOT NULL,
  aqi_value double precision NOT NULL,
  aqi_color text NOT NULL,
  hour TIME NOT NULL, -- Formato esperado HH:MM
  date DATE NOT NULL, -- Formato esperado YYYY-MM-DD
  created timestamp NOT NULL -- Tipo timestamp para almacenar información de zona horaria
);
