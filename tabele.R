# Funkcija, ki uvozi podatke iz datoteke muzej.csv

muzej <- read_delim("Podatki/muzej.csv", 
                    ";", escape_double = FALSE, col_types = cols(ustanovitev = col_integer()), 
                    trim_ws = TRUE)
