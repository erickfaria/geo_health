download.dbc <- function(ftp_path,
                         dest_files_path = getwd(),
                         estados = NULL, anos = NULL){
  require(RCurl)
  require(tidyverse)
  
  url <- ftp_path
  #userpwd <- "yourUser:yourPass"
  #recupera o nome dos arquivos em ftp_path
  filenames <- getURL(url, userpwd = NULL,
                      ftp.use.epsv = FALSE,dirlistonly = TRUE)
  filenames <- unlist(strsplit(filenames,"\n"))
  filenames <- data.frame(filename=filenames)
  filenames <- data.frame(lapply(filenames, as.character), stringsAsFactors=FALSE)
  
  #filtra os estados desejados se estes forem especificados
  if(!is.null(estados)){
    filenames <- filenames %>%
      filter(str_detect(filename, paste(estados,sep="",collapse = '|')))
  }
  #filtra os anos desejados se estes forem especificados
  if(!is.null(anos)){
    filenames <- filenames %>%
      filter(str_detect(filename, paste(anos,sep="",collapse = '|')))
  }
  
  #faz download dos arquivos selecionados e salva os arquivos em dest_files_path
  for (filename in filenames$filename) {
    download.file(paste(url, filename, sep = ""), paste(dest_files_path, "/", filename, sep = ""))
  }
  
  #retorna um data.frame com os nomes e caminho dos arquivos
  return(data.frame(file_name=filenames$filename,file_path=dest_files_path))
}

#============[INTERAÇÂO DSS]============
library(dataiku)
library(dplyr)
#===========================================

# #Caminho para função "download.dbc"
path.script = "https://github.com/bigdata-icict/ETL-Dataiku-DSS/download.dbc.r"

# #Caminho FTP Datasus
ftp_path = "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/NOV/DNRES/"

#============[EDITAR]============
#Local para salvamento dos arquivos .dbc
path.data = "path_dbc_files"
#================================

#============[OPCIONAL]============
#filtros de estado e/ou anos
estados = c("MG","RJ") #outro exemplo: c("RJ")
anos = 2021 #outro exemplo: 2000:2014
#==================================

#Carrega função "download.dbc"
if(!exists("download.dbc", mode="function")) source(path.script)

# Computa retorno da receita: nome dos arquivos .dbc baixados
datasus_files_list <- download.dbc(ftp_path = ftp_path,
                                       dest_files_path = path.data,
                                       estados = estados,
                                       anos = anos)

# #============[INTERAÇÂO DSS]============ -> Alternativa: salvar data.frame em arquivo .csv 
# Retornando resultados para dataset do DSS
dkuWriteDataset(datasus_files_list,"datasus",schema = TRUE)
# #===========================================