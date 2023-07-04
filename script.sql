--antes de la migracion modificar los siguientes campos y agregar registros
INSERT INTO administrativo.direcciones_viento
(id_dir_viento, nombre, abreviacion, grados, estado)
VALUES(18, 'No disponible', 'ND', 0, true);


ALTER TABLE convencionales2."_293161h" ALTER COLUMN term_seco TYPE numeric(10, 2) USING term_seco::numeric;
ALTER TABLE convencionales2."_293161h" ALTER COLUMN term_hmd TYPE numeric(10, 2) USING term_hmd::numeric;

ALTER TABLE convencionales2."_171481h" ALTER COLUMN valor TYPE numeric(10, 2) USING valor::numeric;

ALTER TABLE convencionales2."_614161h" ALTER COLUMN evaporacion_media TYPE numeric(10, 2) USING evaporacion_media::numeric;

ALTER TABLE convencionales2."_3711161h" ALTER COLUMN velocidad TYPE numeric(10, 2) USING velocidad::numeric;
ALTER TABLE convencionales2."_3711161h" ALTER COLUMN recorrido TYPE numeric(20, 2) USING recorrido::numeric;


ALTER TABLE convencionales2."_12827161h" ALTER COLUMN octas TYPE numeric(10, 2) USING octas::numeric;

ALTER TABLE convencionales2."_293161d" ALTER COLUMN temp_max TYPE numeric(10, 2) USING temp_max::numeric;
ALTER TABLE convencionales2."_293161d" ALTER COLUMN temp_min TYPE numeric(10, 2) USING temp_min::numeric;




INSERT INTO seguridades.usuarios
(id_usuario, usuario, contrasenia, email, persona_fk, estado, conv)
VALUES(999, 'asandoval', '24913d0addfeaf00966e33db3df5296e045b8eded4bd4cd75e418e7853690ec0', 'sandovalleonel16@gmail.com', 1, '1', true);




