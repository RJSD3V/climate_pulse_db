CREATE TABLE IF NOT EXISTS dev_sode.noaa_ghcn_2022 as SELECT * FROM read_csv('./seeds/raw_data/2022.csv' ,AUTO_DETECT=TRUE);

CREATE TABLE IF NOT EXISTS dev_sode.noaa_ghcn_2023 as SELECT * FROM read_csv('./seeds/raw_data/2023.csv',AUTO_DETECT=TRUE);