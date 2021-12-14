-- View: animal_biome_production.pet_results_v

-- DROP MATERIALIZED VIEW animal_biome_production.pet_results_v;

CREATE MATERIALIZED VIEW animal_biome_production.pet_results_v
TABLESPACE pg_default
AS
 SELECT k.row_number,
    k.pet_id,
    k.sample_id,
    k.taxon_id,
    k.user_id,
    k.assmentid,
    k.pet_genus_level_information,
    k.pets_result,
    k.low,
    k.high,
    k.reference_range,
    k.date_collected,
    k.sample_type,
    k.color_indication,
    k.species,
    k.tool_tip,
    k.taxon_type,
        CASE
            WHEN k.extraction::text = 'powersoil pro'::text OR k.extraction::text = 'powersoil-pro-1'::text THEN 'powersoil-pro'::character varying
            WHEN k.extraction::text = 'none'::text OR k.extraction::text = ''::text THEN 'powersoil'::character varying
            ELSE k.extraction
        END AS extraction
   FROM ( --part1 start
   SELECT row_number() OVER () AS row_number,
            k_1.pet_id,
            k_1.sample_id,
            k_1.taxon_id,
            k_1.user_id,
            k_1.assmentid,
            k_1.pet_genus_level_information,
            k_1.pets_result,
            k_1.low,
            k_1.high,
            k_1.reference_range,
            k_1.date_collected,
            k_1.sample_type,
            k_1.color_indication,
            k_1.species,
            k_1.tool_tip,
            k_1.taxon_type,
            k_1.extraction
           FROM ( --part2 start  
		   SELECT DISTINCT g.pet_id,
                    g.sample_id,
                    g.taxon_id,
                    g.user_id,
                    g.assmentid,
                    g.pet_genus_level_information,
                    g.pets_result,
                    g.low,
                    g.high,
                    g.reference_range,
                    g.date_collected,
                    g.sample_type,
                    g.color_indication,
                    g.species,
                    g.tool_tip,
                    g.taxon_type,
                    g.extraction
                   FROM ( --part3 start 
				   SELECT DISTINCT k_2.pet_id,
                            k_2.sample_id,
                            k_2.taxon_id,
                            k_2.user_id,
                            k_2.assmentid,
                            k_2.pet_genus_level_information,
                            k_2.pets_result,
                            k_2.low,
                            k_2.high,
                            k_2.reference_range,
                            k_2.date_collected,
                            COALESCE(k_2.sample_type, 'gut'::animal_biome_production.enum_sample_type) AS sample_type,
                            k_2.color_indication,
                            k_2.species,
                            hc.tool_tip,
                            k_2.taxon_type,
                            k_2.extraction
                           FROM 
						   ( --part4 start 
						   SELECT DISTINCT g_1.pet_id,
                                    g_1.sample_id,
                                    g_1.taxon_id,
                                    g_1.user_id,
                                    g_1.assmentid,
                                    g_1.pet_genus_level_information,
                                    g_1.pets_result,
CASE
 WHEN g_1.sample_type = 'gut'::animal_biome_production.enum_sample_type THEN
 CASE
  WHEN g_1.extraction::text = 'powersoil'::text OR g_1.extraction::text = 'none'::text OR g_1.extraction::text = ''::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.low
   ELSE NULL::numeric
  END
  WHEN g_1.extraction::text = 'powersoil-pro'::text OR g_1.extraction::text = 'powersoil pro'::text OR g_1.extraction::text = 'powersoil-pro-1'::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.low3
   ELSE NULL::numeric
  END
  ELSE g_1.low
 END
 ELSE
 CASE
  WHEN g_1.sample_type1 = 'oral'::animal_biome_production.enum_sample_type THEN g_1.low3
  ELSE NULL::numeric
 END
END AS low,
CASE
 WHEN g_1.sample_type = 'gut'::animal_biome_production.enum_sample_type THEN
 CASE
  WHEN g_1.extraction::text = 'powersoil'::text OR g_1.extraction::text = 'none'::text OR g_1.extraction::text = ''::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.high
   ELSE NULL::numeric
  END
  WHEN g_1.extraction::text = 'powersoil-pro'::text OR g_1.extraction::text = 'powersoil pro'::text OR g_1.extraction::text = 'powersoil-pro-1'::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.high3
   ELSE NULL::numeric
  END
  ELSE g_1.high
 END
 ELSE
 CASE
  WHEN g_1.sample_type1 = 'oral'::animal_biome_production.enum_sample_type THEN g_1.high3
  ELSE NULL::numeric
 END
END AS high,
CASE
 WHEN g_1.sample_type = 'gut'::animal_biome_production.enum_sample_type THEN
 CASE
  WHEN g_1.extraction::text = 'powersoil'::text OR g_1.extraction::text = 'none'::text OR g_1.extraction::text = ''::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.reference_range
   ELSE NULL::text
  END
  WHEN g_1.extraction::text = 'powersoil-pro'::text OR g_1.extraction::text = 'powersoil pro'::text OR g_1.extraction::text = 'powersoil-pro-1'::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.reference_range1
   ELSE NULL::text
  END
  ELSE g_1.reference_range
 END
 ELSE
 CASE
  WHEN g_1.sample_type1 = 'oral'::animal_biome_production.enum_sample_type THEN g_1.reference_range1
  ELSE NULL::text
 END
END AS reference_range,
                                    g_1.species,
                                    g_1.date_collected,
CASE
 WHEN g_1.sample_type = 'gut'::animal_biome_production.enum_sample_type THEN
 CASE
  WHEN g_1.extraction::text = 'powersoil'::text OR g_1.extraction::text = 'none'::text OR g_1.extraction::text = ''::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN
   CASE
    WHEN g_1.pets_result >= 0::numeric AND g_1.pets_result <= g_1.low1 THEN g_1."0to2.5"
    WHEN g_1.pets_result >= g_1.low1 AND g_1.pets_result <= g_1.low THEN g_1."2.5to10"
    WHEN g_1.pets_result >= g_1.low AND g_1.pets_result <= g_1.high THEN g_1."10to90"
    WHEN g_1.pets_result >= g_1.high AND g_1.pets_result <= g_1.high1 THEN g_1."90to97.5"
    ELSE g_1."97.5up"
   END
   ELSE NULL::character varying
  END
  WHEN g_1.extraction::text = 'powersoil-pro'::text OR g_1.extraction::text = 'powersoil pro'::text OR g_1.extraction::text = 'powersoil-pro-1'::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN
   CASE
    WHEN g_1.pets_result >= 0::numeric AND g_1.pets_result <= g_1.low2 THEN g_1."0to2.51"
    WHEN g_1.pets_result >= g_1.low2 AND g_1.pets_result <= g_1.low3 THEN g_1."2.5to101"
    WHEN g_1.pets_result >= g_1.low3 AND g_1.pets_result <= g_1.high3 THEN g_1."10to901"
    WHEN g_1.pets_result >= g_1.high3 AND g_1.pets_result <= g_1.high4 THEN g_1."90to97.51"
    ELSE g_1."97.5up1"
   END
   ELSE NULL::character varying
  END
  ELSE
  CASE
   WHEN g_1.pets_result >= 0::numeric AND g_1.pets_result <= g_1.low1 THEN g_1."0to2.5"
   WHEN g_1.pets_result >= g_1.low1 AND g_1.pets_result <= g_1.low THEN g_1."2.5to10"
   WHEN g_1.pets_result >= g_1.low AND g_1.pets_result <= g_1.high THEN g_1."10to90"
   WHEN g_1.pets_result >= g_1.high AND g_1.pets_result <= g_1.high1 THEN g_1."90to97.5"
   ELSE g_1."97.5up"
  END
 END
 ELSE
 CASE
  WHEN g_1.sample_type1 = 'oral'::animal_biome_production.enum_sample_type THEN
  CASE
   WHEN g_1.pets_result >= 0::numeric AND g_1.pets_result <= g_1.low1 THEN g_1."0to2.51"
   WHEN g_1.pets_result >= g_1.low1 AND g_1.pets_result <= g_1.low THEN g_1."2.5to101"
   WHEN g_1.pets_result >= g_1.low AND g_1.pets_result <= g_1.high THEN g_1."10to901"
   WHEN g_1.pets_result >= g_1.high AND g_1.pets_result <= g_1.high1 THEN g_1."90to97.51"
   ELSE g_1."97.5up"
  END
  ELSE NULL::character varying
 END
END AS color_indication,
                                    g_1.sample_type,
CASE
 WHEN g_1.sample_type = 'gut'::animal_biome_production.enum_sample_type THEN
 CASE
  WHEN g_1.extraction::text = 'powersoil'::text OR g_1.extraction::text = 'none'::text OR g_1.extraction::text = ''::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.type1
   ELSE NULL::animal_biome_production.taxon_type
  END
  WHEN g_1.extraction::text = 'powersoil-pro'::text OR g_1.extraction::text = 'powersoil pro'::text OR g_1.extraction::text = 'powersoil-pro-1'::text THEN
  CASE
   WHEN g_1.taxon_id = g_1.taxon1 THEN g_1.type2
   ELSE NULL::animal_biome_production.taxon_type
  END
  ELSE g_1.type1
 END
 ELSE
 CASE
  WHEN g_1.sample_type1 = 'oral'::animal_biome_production.enum_sample_type THEN g_1.type2
  ELSE NULL::animal_biome_production.taxon_type
 END
END AS taxon_type,
       g_1.extraction,
       max(g_1.sum) OVER (PARTITION BY g_1.sample_id, g_1.seq_date) AS max
      FROM 
		( --part5 start 
								   SELECT pet.id AS pet_id,
    asmnt.id AS assmentid,
    taxon.taxon_id,
    healthy_p.taxon_id AS taxon1,
    healthy_p1.taxon_id AS taxon2,
    users.id AS user_id,
    sample.sample_id,
  CASE
   WHEN taxon.genus::text = 'UC'::text THEN ((taxon.family_1::text || ' '::text) || 'Type 1'::text)::character varying
   WHEN taxon.genus::text = 'uncultured'::text THEN ((taxon.family_1::text || ' '::text) || 'Type 2'::text)::character varying
   ELSE taxon.genus
  END AS pet_genus_level_information,
    round(f_genus.value * 100::numeric, 1) AS pets_result,
    sum(f_genus.count) OVER (PARTITION BY f_genus.sample_id, f_genus.seq_date, f_genus.sample_id_src) AS sum,
    healthy_p.low_2pnt5pct AS low1,
    healthy_p.low_10pct AS low,
    healthy_p.high_90pct AS high,
    healthy_p.high_97pnt5pct AS high1,
    (((round(healthy_p.low_10pct, 1) || ' '::text) || '-'::text) || ' '::text) || round(healthy_p.high_90pct, 1) AS reference_range,
    asmnt.date_collected,
    healthy_p.f0to2pnt5 AS "0to2.5",
    healthy_p.f2pnt5to10 AS "2.5to10",
    healthy_p.f10to90 AS "10to90",
    healthy_p.f90to97pnt5 AS "90to97.5",
    healthy_p.f97pnt5up AS "97.5up",
    healthy_p1.low_2pnt5pct AS low2,
    healthy_p1.low_10pct AS low3,
    healthy_p1.high_90pct AS high3,
    healthy_p.high_97pnt5pct AS high4,
    (((round(healthy_p1.low_10pct, 1) || ' '::text) || '-'::text) || ' '::text) || round(healthy_p1.high_90pct, 1) AS reference_range1,
    healthy_p1.f0to2pnt5 AS "0to2.51",
    healthy_p1.f2pnt5to10 AS "2.5to101",
    healthy_p1.f10to90 AS "10to901",
    healthy_p1.f90to97pnt5 AS "90to97.51",
    healthy_p1.f97pnt5up AS "97.5up1",
    pet.species,
    COALESCE(bulk.sample_type, 'gut'::animal_biome_production.enum_sample_type) AS sample_type,
    healthy_p.taxon_type AS type1,
    healthy_p1.taxon_type AS type2,
    healthy_p1.taxon_id AS taxon_id1,
    COALESCE(f_genus.extraction_type, 'powersoil'::character varying) AS extraction,
    healthy_p1.sample_type AS sample_type1,
    f_genus.seq_date
   FROM animal_biome_production.pets_pet pet
     LEFT JOIN animal_biome_production.assessments_assessment asmnt ON pet.id = asmnt.pet_id
     LEFT JOIN animal_biome_production.dim_samples sample ON asmnt.sample_registration_key::text = sample.sample_id::text
     LEFT JOIN animal_biome_production.fact_genus_level2 f_genus ON f_genus.sample_id::text = sample.sample_id::text
     LEFT JOIN animal_biome_production.dim_taxon taxon ON taxon.taxon_id = f_genus.taxon_id
     LEFT JOIN animal_biome_production.assessments_assessmentbulkcreate bulk ON bulk.id = asmnt.bulk_create_id
     LEFT JOIN animal_biome_production.dim_healthy_power_soil_pro healthy_p1 ON healthy_p1.taxon_id = taxon.taxon_id AND healthy_p1.type_1::text =
  CASE
   WHEN pet.species::text = 'feline'::text THEN 'cat'::text
   WHEN pet.species::text = 'canine'::text THEN 'dog'::text
   ELSE NULL::text
  END AND bulk.sample_type = healthy_p1.sample_type
     LEFT JOIN animal_biome_production.dim_healthy healthy_p ON healthy_p.taxon_id = taxon.taxon_id AND healthy_p.type_1::text =
  CASE
   WHEN pet.species::text = 'feline'::text THEN 'cat'::text
   WHEN pet.species::text = 'canine'::text THEN 'dog'::text
   ELSE NULL::text
  END
     LEFT JOIN animal_biome_production.users_user users ON pet.user_id = users.id
  WHERE 
  ( 
   (f_genus.seq_date, f_genus.sample_id::text) 
  IN  (
  	  SELECT max(fgl.seq_date) AS max,fgl.sample_id
      FROM animal_biome_production.fact_genus_level2 fgl
      GROUP BY fgl.sample_id
	  )
	)
  ) g_1 --part 5 end 
WHERE ( ----where clause start
   (g_1.sum, g_1.sample_id::text, g_1.seq_date) 
   IN 
   ( --In clause start
   SELECT max(g_1_1.cnt) AS cnt,
    g_1_1.sample_id,
    g_1_1.seq_date
   FROM ( --part6 start 
   SELECT sum(fgl2.count) AS cnt,
      fgl2.sample_id,
      fgl2.sample_id_src,
      fgl2.seq_date
     FROM animal_biome_production.fact_genus_level2 fgl2
    GROUP BY fgl2.sample_id, fgl2.sample_id_src, fgl2.seq_date) g_1_1 --part6 end
  GROUP BY g_1_1.sample_id, g_1_1.seq_date) --in clause end
  ) --where clause end
  ) k_2,--part4 end 
                            animal_biome_production.dim_healthy_color hc
                          WHERE hc.type_1::text = k_2.species::text AND hc.color::text = k_2.color_indication::text AND hc.sample_type = k_2.sample_type) g --part3 end
						      ) --part2 end
						  k_1
						  ) k --part1 end
WITH DATA;

ALTER TABLE animal_biome_production.pet_results_v
    OWNER TO postgres;


CREATE INDEX pet_results_v_row3_number_idx
    ON animal_biome_production.pet_results_v USING btree
    (row_number)
    TABLESPACE pg_default;