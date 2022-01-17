library(tidyverse)
library(here)
library(DBI)
library(odbc)
library(lubridate)

conn <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "dbprod02.izsler.it",Database = "IZSLER", Port = 1433)


sqlViro <- "SELECT
  dbo.Conferimenti.Numero,
  dbo.Conferimenti.Riferimenti,
  dbo_Anag_Finalita_Confer.Descrizione,
  dbo.Anag_TipoConf.Descrizione,
  dbo_Operatori_di_sistema_ConfMatr.Descr_Completa,
  dbo.Conferimenti.Data_Prelievo,
  dbo.Conferimenti.Data,
  dbo.Conferimenti.Data_Accettazione,
  convert (SMALLDATETIME, dbo.Conferimenti.Data_Primo_RDP_Completo_Firmato),
  dbo.RDP_Date_Emissione.Data_RDP,
  dbo.Anag_Supergruppo_Specie.Descrizione,
  dbo.Anag_Gruppo_Specie.Descrizione,
  dbo.Anag_Specie.Descrizione,
  dbo.Anag_Materiali.Descrizione,
  dbo.Conferimenti_Campioni.Campione,
  dbo.Conferimenti_Campioni.Identificazione,
  dbo.Conferimenti.Allevix_Proprietario,
  dbo.Anag_Referenti.Ragione_Sociale,
  dbo_Anag_Referenti_Prop.Ragione_Sociale,
  dbo_Anag_Referenti_DestRdP.Ragione_Sociale,
  dbo.Anag_Regioni.Descrizione,
  dbo.Anag_Comuni.Provincia,
  dbo.Anag_Comuni.Descrizione ,
  dbo.Anag_Organo_Prelevatore.Descrizione,
  dbo.Anag_Tipo_Prel.Descrizione,
  dbo.Conferimenti_Movimentazione.Data_Invio,
  dbo.Conferimenti_Movimentazione.Data_Presa_in_Carico,
  dbo_Anag_Reparti_Mitt.Descrizione,
  dbo_Anag_Reparti_ConfAcc.Descrizione,
  dbo.Anag_Finalita.Descrizione,
  dbo.Anag_Gruppo_Prove.Descrizione,
  dbo.Anag_Prove.Descrizione,
  dbo.Anag_Tecniche.Descrizione,
  dbo.Anag_Metodi_di_Prova.Descrizione,
  dbo.Nomenclatore.Chiave,
  dbo.Anag_Reparti.Descrizione,
  dbo.Anag_Laboratori.Descrizione,
  dbo_Anag_Boolean_InStampa.Descrizione2,
  dbo.Esami_Aggregati.Codice,
  dbo.Esami_Aggregati.Data_Inizio_Analisi,
  dbo.Esami_Aggregati.Data_Fine_Analisi,
  dbo.Risultati_Analisi_Note.Nota,
  dbo.Risultati_Analisi_Note.UdmNote,
  dbo.Nomenclatore_Range.Valore,
  dbo.Nomenclatore_Range.ModEspr,
  dbo.Nomenclatore_Range.ModEspr2,
  dbo.Risultati_Analisi.Segno,
  dbo.Risultati_Analisi.Valore,
  dbo.Anag_UDM.Descrizione,
  dbo.Risultati_Analisi.Decimali_Significativi,
  dbo.Anag_Esiti.Descrizione,
  dbo_Anag_Boolean_Incert.Descrizione2,
  dbo_Anag_Tipo_Dett_Diagnostica.Descrizione,
  dbo_Anag_Dettagli_Diagnostica.Dettaglio,
  dbo.Risultati_Analisi_Dettagli.Testo_Dettaglio
FROM
{ oj dbo.Anag_Finalita  dbo_Anag_Finalita_Confer INNER JOIN dbo.Conferimenti_Finalita ON ( dbo.Conferimenti_Finalita.Finalita=dbo_Anag_Finalita_Confer.Codice )
   INNER JOIN dbo.Conferimenti ON ( dbo.Conferimenti.Anno=dbo.Conferimenti_Finalita.Anno and dbo.Conferimenti.Numero=dbo.Conferimenti_Finalita.Numero )
   INNER JOIN dbo.Anag_Comuni ON ( dbo.Anag_Comuni.Codice=dbo.Conferimenti.Luogo_Prelievo )
   LEFT OUTER JOIN dbo.Anag_Regioni ON ( dbo.Anag_Regioni.Codice=dbo.Anag_Comuni.Regione )
   INNER JOIN dbo.Anag_Referenti ON ( dbo.Conferimenti.Conferente=dbo.Anag_Referenti.Codice )
   LEFT OUTER JOIN dbo.Esami_Aggregati ON ( dbo.Conferimenti.Anno=dbo.Esami_Aggregati.Anno_Conferimento and dbo.Conferimenti.Numero=dbo.Esami_Aggregati.Numero_Conferimento )
   LEFT OUTER JOIN dbo.Nomenclatore_MP ON ( dbo.Esami_Aggregati.Nomenclatore=dbo.Nomenclatore_MP.Codice )
   LEFT OUTER JOIN dbo.Anag_Metodi_di_Prova ON ( dbo.Nomenclatore_MP.MP=dbo.Anag_Metodi_di_Prova.Codice )
   LEFT OUTER JOIN dbo.Nomenclatore_Settori ON ( dbo.Nomenclatore_MP.Nomenclatore_Settore=dbo.Nomenclatore_Settori.Codice )
   LEFT OUTER JOIN dbo.Nomenclatore ON ( dbo.Nomenclatore_Settori.Codice_Nomenclatore=dbo.Nomenclatore.Chiave )
   LEFT OUTER JOIN dbo.Anag_Prove ON ( dbo.Nomenclatore.Codice_Prova=dbo.Anag_Prove.Codice )
   LEFT OUTER JOIN dbo.Anag_Tecniche ON ( dbo.Nomenclatore.Codice_Tecnica=dbo.Anag_Tecniche.Codice )
   LEFT OUTER JOIN dbo.Anag_Gruppo_Prove ON ( dbo.Nomenclatore.Codice_Gruppo=dbo.Anag_Gruppo_Prove.Codice )
   LEFT OUTER JOIN dbo.Programmazione_Finalita ON ( dbo.Esami_Aggregati.Anno_Conferimento=dbo.Programmazione_Finalita.Anno_Conferimento and dbo.Esami_Aggregati.Numero_Conferimento=dbo.Programmazione_Finalita.Numero_Conferimento and dbo.Esami_Aggregati.Codice=dbo.Programmazione_Finalita.Codice )
   LEFT OUTER JOIN dbo.Anag_Finalita ON ( dbo.Programmazione_Finalita.Finalita=dbo.Anag_Finalita.Codice )
   LEFT OUTER JOIN dbo.Laboratori_Reparto ON ( dbo.Esami_Aggregati.RepLab_analisi=dbo.Laboratori_Reparto.Chiave )
   LEFT OUTER JOIN dbo.Anag_Reparti ON ( dbo.Laboratori_Reparto.Reparto=dbo.Anag_Reparti.Codice )
   LEFT OUTER JOIN dbo.Anag_Laboratori ON ( dbo.Laboratori_Reparto.Laboratorio=dbo.Anag_Laboratori.Codice )
   INNER JOIN dbo.Indice_Campioni_Esaminati ON ( dbo.Esami_Aggregati.Anno_Conferimento=dbo.Indice_Campioni_Esaminati.Anno_Conferimento and dbo.Esami_Aggregati.Numero_Conferimento=dbo.Indice_Campioni_Esaminati.Numero_Conferimento and dbo.Esami_Aggregati.Codice=dbo.Indice_Campioni_Esaminati.Codice )
   LEFT OUTER JOIN dbo.Risultati_Analisi ON ( dbo.Indice_Campioni_Esaminati.Anno_Conferimento=dbo.Risultati_Analisi.Anno_Conferimento and dbo.Indice_Campioni_Esaminati.Numero_Conferimento=dbo.Risultati_Analisi.Numero_Conferimento and dbo.Indice_Campioni_Esaminati.Codice=dbo.Risultati_Analisi.Codice and dbo.Indice_Campioni_Esaminati.Numero_Campione=dbo.Risultati_Analisi.Numero_Campione )
   LEFT OUTER JOIN dbo.Nomenclatore_Range ON ( dbo.Risultati_Analisi.Range=dbo.Nomenclatore_Range.Codice )
   LEFT OUTER JOIN dbo.Anag_Esiti ON ( dbo.Risultati_Analisi.Esito=dbo.Anag_Esiti.Codice )
   LEFT OUTER JOIN dbo.Nomenclatore_UDM ON ( dbo.Risultati_Analisi.UDM=dbo.Nomenclatore_UDM.Codice )
   LEFT OUTER JOIN dbo.Anag_UDM ON ( dbo.Nomenclatore_UDM.Codice_UDM=dbo.Anag_UDM.Codice )
   LEFT OUTER JOIN dbo.Risultati_Analisi_Note ON ( dbo.Risultati_Analisi.Anno_Conferimento=dbo.Risultati_Analisi_Note.Anno_Conferimento and dbo.Risultati_Analisi.Numero_Conferimento=dbo.Risultati_Analisi_Note.Numero_Conferimento and dbo.Risultati_Analisi.Codice=dbo.Risultati_Analisi_Note.Codice and dbo.Risultati_Analisi.Numero_Campione=dbo.Risultati_Analisi_Note.Numero_Campione )
   LEFT OUTER JOIN dbo.Anag_Boolean  dbo_Anag_Boolean_Incert ON ( dbo.Risultati_Analisi.Incertezza=dbo_Anag_Boolean_Incert.Codice )
   LEFT OUTER JOIN dbo.Risultati_Analisi_Dettagli ON ( dbo.Indice_Campioni_Esaminati.Anno_Conferimento=dbo.Risultati_Analisi_Dettagli.Anno_Conferimento and dbo.Indice_Campioni_Esaminati.Numero_Conferimento=dbo.Risultati_Analisi_Dettagli.Numero_Conferimento and dbo.Indice_Campioni_Esaminati.Numero_Campione=dbo.Risultati_Analisi_Dettagli.Numero_Campione and dbo.Indice_Campioni_Esaminati.Codice=dbo.Risultati_Analisi_Dettagli.Codice_Programmazione )
   LEFT OUTER JOIN dbo.Anag_Dettagli  dbo_Anag_Dettagli_Diagnostica ON ( dbo.Risultati_Analisi_Dettagli.Codice_Dettaglio=dbo_Anag_Dettagli_Diagnostica.Codice )
   LEFT OUTER JOIN dbo.Anag_Tipo_Dett  dbo_Anag_Tipo_Dett_Diagnostica ON ( dbo.Risultati_Analisi_Dettagli.Tipo_Dettaglio=dbo_Anag_Tipo_Dett_Diagnostica.Codice )
   LEFT OUTER JOIN dbo.Conferimenti_Campioni ON ( dbo.Conferimenti_Campioni.Anno=dbo.Indice_Campioni_Esaminati.Anno_Conferimento and dbo.Conferimenti_Campioni.Numero=dbo.Indice_Campioni_Esaminati.Numero_Conferimento and dbo.Conferimenti_Campioni.Campione=dbo.Indice_Campioni_Esaminati.Numero_Campione )
   INNER JOIN dbo.Anag_Boolean  dbo_Anag_Boolean_InStampa ON ( dbo_Anag_Boolean_InStampa.Codice=dbo.Esami_Aggregati.InStampa )
   INNER JOIN dbo.Anag_Tipo_Prel ON ( dbo.Conferimenti.Tipo_Prelievo=dbo.Anag_Tipo_Prel.Codice )
   INNER JOIN dbo.Anag_Referenti  dbo_Anag_Referenti_DestRdP ON ( dbo.Conferimenti.Dest_Rapporto_Prova=dbo_Anag_Referenti_DestRdP.Codice )
   INNER JOIN dbo.Laboratori_Reparto  dbo_Laboratori_Reparto_ConfProp ON ( dbo.Conferimenti.RepLab=dbo_Laboratori_Reparto_ConfProp.Chiave )
   INNER JOIN dbo.Anag_Reparti  dbo_Anag_Reparti_ConfProp ON ( dbo_Laboratori_Reparto_ConfProp.Reparto=dbo_Anag_Reparti_ConfProp.Codice )
   INNER JOIN dbo.Laboratori_Reparto  dbo_Laboratori_Reparto_ConfAcc ON ( dbo.Conferimenti.RepLab_Conferente=dbo_Laboratori_Reparto_ConfAcc.Chiave )
   INNER JOIN dbo.Anag_Reparti  dbo_Anag_Reparti_ConfAcc ON ( dbo_Laboratori_Reparto_ConfAcc.Reparto=dbo_Anag_Reparti_ConfAcc.Codice )
   LEFT OUTER JOIN dbo.Anag_Referenti  dbo_Anag_Referenti_Prop ON ( dbo_Anag_Referenti_Prop.Codice=dbo.Conferimenti.Proprietario )
   INNER JOIN dbo.Anag_TipoConf ON ( dbo.Anag_TipoConf.Codice=dbo.Conferimenti.Tipo )
   LEFT OUTER JOIN dbo.Anag_Materiali ON ( dbo.Anag_Materiali.Codice=dbo.Conferimenti.Codice_Materiale )
   LEFT OUTER JOIN dbo.Anag_Specie ON ( dbo.Anag_Specie.Codice=dbo.Conferimenti.Codice_Specie )
   LEFT OUTER JOIN dbo.Anag_Gruppo_Specie ON ( dbo.Anag_Specie.Cod_Darc1=dbo.Anag_Gruppo_Specie.Codice )
   LEFT OUTER JOIN dbo.Anag_Supergruppo_Specie ON ( dbo.Anag_Gruppo_Specie.Cod_Supergruppo=dbo.Anag_Supergruppo_Specie.Codice )
   LEFT OUTER JOIN dbo.Anag_Organo_Prelevatore ON ( dbo.Conferimenti.OrganoPrelevatore=dbo.Anag_Organo_Prelevatore.Codice )
   INNER JOIN dbo.Operatori_di_sistema  dbo_Operatori_di_sistema_ConfMatr ON ( dbo.Conferimenti.Matr_Ins=dbo_Operatori_di_sistema_ConfMatr.Ident_Operatore )
   LEFT OUTER JOIN dbo.RDP_Date_Emissione ON ( dbo.RDP_Date_Emissione.Anno=dbo.Conferimenti.Anno and dbo.RDP_Date_Emissione.Numero=dbo.Conferimenti.Numero )
   LEFT OUTER JOIN dbo.Conferimenti_Movimentazione ON ( dbo.Conferimenti.Anno=dbo.Conferimenti_Movimentazione.Anno_Conferimento  AND  dbo.Conferimenti_Movimentazione.Numero_Conferimento=dbo.Conferimenti.Numero )
   LEFT OUTER JOIN dbo.Laboratori_Reparto  dbo_Laboratori_Reparto_Mitt ON ( dbo.Conferimenti_Movimentazione.RepLab_che_invia=dbo_Laboratori_Reparto_Mitt.Chiave )
   LEFT OUTER JOIN dbo.Anag_Reparti  dbo_Anag_Reparti_Mitt ON ( dbo_Laboratori_Reparto_Mitt.Reparto=dbo_Anag_Reparti_Mitt.Codice )
  }
WHERE
  dbo.Esami_Aggregati.Esame_Altro_Ente = 0
  AND  dbo.Esami_Aggregati.Esame_Altro_Ente = 0
  AND  (
  dbo_Anag_Reparti_ConfProp.Descrizione  =  'Reparto Virologia'
  AND  {fn year(dbo.Conferimenti.Data_Accettazione)}  >=  2020
  )
"


viro <- conn%>% tbl(sql(sqlViro)) %>% as_tibble()