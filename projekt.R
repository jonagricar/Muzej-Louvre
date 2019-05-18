# Neposredno klicanje SQL ukazov v R
library(dplyr)
library(dbplyr)
library(RPostgreSQL)

source("auth.R", encoding="UTF-8")
source("tabele.R", encoding="UTF-8")


# Povežemo se z gonilnikom za PostgreSQL
drv <- dbDriver("PostgreSQL")

#brisanje tabel
delete_table <- function(){
  # Uporabimo funkcijo tryCatch,
  # da prisilimo prekinitev povezave v primeru napake
  tryCatch({
    # Vzpostavimo povezavo z bazo
    conn <- dbConnect(drv, dbname = db, host = host, user = user, password = password)
    
    # Če tabela obstaja, jo zbrišemo, ter najprej zbrišemo tiste,
    # ki se navezujejo na druge
    dbSendQuery(conn,build_sql("DROP TABLE IF EXISTS muzej CASCADE", con=conn))
    dbSendQuery(conn,build_sql("DROP TABLE IF EXISTS prostor CASCADE", con=conn))
    dbSendQuery(conn,build_sql("DROP TABLE IF EXISTS obdobje CASCADE", con=conn))
    dbSendQuery(conn,build_sql("DROP TABLE IF EXISTS umetnik CASCADE", con=conn))
    dbSendQuery(conn,build_sql("DROP TABLE IF EXISTS umetnina CASCADE", con=conn))
    dbSendQuery(conn,build_sql("DROP TABLE IF EXISTS deloval CASCADE", con=conn))
    
  }, finally = {
    dbDisconnect(conn)
  })
}

ustvari_tabele <- function(){
    tryCatch({
    conn <- dbConnect(drv, dbname = db, host = host,
                      user = user, password = password)
    #tabele
    muzej <- dbSendQuery(conn, build_sql("CREATE TABLE muzej (
                                ime text PRIMARY KEY,
                                mesto text,
                                drzava text,
                                ustanovitev numeric,
                                vodja text)", con=conn))
    
    prostor <- dbSendQuery(conn, build_sql("CREATE TABLE prostor (
                                stevilka numeric PRIMARY KEY,
                                ime text,
                                muzej text,
                                oddelek text,
                                etaza text,
                                krilo text)", con=conn))
    
    obdobje <- dbSendQuery(conn, build_sql("CREATE TABLE obdobje (
                                id text PRIMARY KEY,
                                ime text,
                                zacetek numeric,
                                konec numeric)", con=conn))
    
    umetnik <- dbSendQuery(conn, build_sql("CREATE TABLE umetnik (
                                id text PRIMARY KEY,
                                ime text,
                                rojstvo numeric,
                                drzava text,
                                stil text)", con=conn))
    
    umetnina <- dbSendQuery(conn, build_sql("CREATE TABLE umetnina (
                                id text PRIMARY KEY,
                                ime text,
                                umetnik text,
                                nastanek numeric,
                                obdobje text,
                                tip text,
                                material text,
                                sirina numeric,
                                visina numeric,
                                prostor text,
                                pridobitev numeric)", con=conn))
    
    #tabele relacij:
    
    deloval <- dbSendQuery(conn, build_sql("CREATE TABLE deloval(
                                          umetnik_id TEXT NOT NULL REFERENCES umetnik(id),
                                          obdobje_id TEXT NOT NULL REFERENCES obdobje(id))", con=conn))
    
    
    }, finally = {
      # Na koncu nujno prekinemo povezavo z bazo,
      # saj preveč odprtih povezav ne smemo imeti
      dbDisconnect(conn)
      # Koda v finally bloku se izvede v vsakem primeru
      # - bodisi ob koncu izvajanja try bloka,
      # ali pa po tem, ko se ta konča z napako
    })
}

#Funcija, ki vstavi podatke
insert_data <- function(){
  tryCatch({
    conn <- dbConnect(drv, dbname = db, host = host, user = user, password = password)
    
    dbWriteTable(conn, name="muzej", muzej, append=T, row.names=FALSE)

  }, finally = {
    dbDisconnect(conn) 
    
  })
}

pravice <- function(){
  # Uporabimo tryCatch,(da se povežemo in bazo in odvežemo)
  # da prisilimo prekinitev povezave v primeru napake
  tryCatch({
    # Vzpostavimo povezavo
    conn <- dbConnect(drv, dbname = db, host = host,#drv=s čim se povezujemo
                      user = user, password = password)
    
    dbSendQuery(conn, build_sql("GRANT CONNECT ON DATABASE sem2019_katjam TO jonag WITH GRANT OPTION", con=conn))
    
    dbSendQuery(conn, build_sql("GRANT ALL ON SCHEMA public TO jonag WITH GRANT OPTION", con=conn))
  }, finally = {
    # Na koncu nujno prekinemo povezavo z bazo,
    # saj preveč odprtih povezav ne smemo imeti
    dbDisconnect(conn) #PREKINEMO POVEZAVO
    # Koda v finally bloku se izvede, preden program konča z napako
  })
}

pravice()
delete_table()
ustvari_tabele()
insert_data()

