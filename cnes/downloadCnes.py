import zipfile
import datetime
import pandas as pd
import numpy as np
from ftplib import FTP

year = datetime.date.today().year
last_month = datetime.date.today().month - 1
month = '{:02d}'.format(last_month)

cnes = f'BASE_DE_DADOS_CNES_{year}{month}.zip'

ftp = FTP(f'ftp.datasus.gov.br') # Faz a conexão com o FTP do datasus
ftp.login() # Login sem necessidade de usuário e senha
ftp.cwd('cnes') #Diretório para os arquivos do CNES

# Download dos dados
with open(f'{cnes}', 'wb') as fp:
  ftp.retrbinary(f'RETR {cnes}', fp.write)

ftp.quit() # Fecha a conexão com o FTP

with zipfile.ZipFile(f'/content/{cnes}', 'r') as zip_ref:
    zip_ref.extractall('/content/')

cnes = pd.read_csv(f'tbCargaHorariaSus{year}{month}.csv', sep=';',
                usecols=[0,2,6], names=['num_unidade','cod_cbo','qtd_ch_ambulatorial'], header=0, encoding='latin-1',
                dtype={'num_unidade':str,'cod_cbo':str})

# Remove as linhas que todos os casos estão duplicados
cnes.drop_duplicates(inplace=True)

# Filtra os casos Missing de Carga Horária Ambulatorial
cnes.fillna({'qtd_ch_ambulatorial':0}, inplace=True)

# Transforma a Variável de Carga Horária Ambulatorial em Integer
cnes['qtd_ch_ambulatorial'] = cnes['qtd_ch_ambulatorial'].astype('int64')

# Transforma códigos que possuem letra em número
cnes.cod_cbo.replace({
'1312C1':131220,'2231A1':223158,'2231A2':223159,'2231F3':225203,
'2231F4':225122,'2231F5':225290,'2231F6':225121,'2231F7':225130,
'223116':225130,'2231F8':223165,'2231F9':223166,'2231G1':223167,
'2232B1':223293,'2235C1':223565,'2235C2':223565,'2235C3':223565,
'2241E1':224140,'2236I1':223665,'3135D1':313510,'3135D2':324190,
'3222B3':515135,'3222E1':322245,'3222E2':322250,'3222E3':322255,
'3224F1':322425,'3224F2':322430,'3522G1':515125,'3522G2':515130,
'5151H1':516220,'5151F1':515140,'5152A1':515290,'1999A1':999999,
'225190':225340,'1999A2':999999}, inplace=True)

cnes['cod_cbo'] = cnes['cod_cbo'].astype('int64')

cnes.fillna({'cod_cbo':999999}, inplace=True)

# Nomeia as labels
cnes['cod_cbo_label'] = cnes.cod_cbo.map({
131205:'Diretor de serviços de saúde',
131210:'Gerente de serviços de saúde',
131215:'Tecnólogo em gestão hospitalar',
131220:'Gerontólogo',
142710:'Tecnólogo em sistemas biomédicos',
203005:'Pesquisador em Biologia Ambiental',
203010:'Pesquisador em Biologia Animal',
203015:'Pesquisador em Biologia de microorganismos e parasitas',
203020:'Pesquisador em Biologia Humana',
203025:'Pesquisador em Biologia Vegetal',
203305:'Pesquisador de clínica médica',
203310:'Pesquisador de medicina básica',
203315:'Pesquisador em medicina veterinária',
203320:'Pesquisador em saúde coletiva',
213150:'Físico (medicina)',
221105:'Biólogo',
221205:'Biomédico',
223101:'Médico acupunturista',
223102:'Médico alergista e imunologista',
223103:'Médico anatomopatologista',
223104:'Médico anestesiologista',
223105:'Médico angiologista',
223106:'Médico cardiologista',
223107:'Médico cirurgião cardiovascular',
223108:'Médico cirurgião de cabeça e pescoço',
223109:'Médico cirurgião do aparelho digestivo',
223110:'Médico cirurgião geral',
223111:'Médico cirurgião pediátrico',
223112:'Médico cirurgião plástico',
223113:'Médico cirurgião torácico',
223114:'Médico citopatologista',
223115:'Médico clínico',
223116:'Médico de família e comunidade',
223117:'Médico dermatologista',
223118:'Médico do trabalho',
223119:'Médico em eletroencefalografia',
223120:'Médico em endoscopia',
223121:'Médico em medicina de tráfego',
223122:'Médico em medicina intensiva',
223123:'Médico em medicina nuclear',
223124:'Médico em radiologia e diagnóstico por imagem',
223125:'Médico endocrinologista e metabologista',
223126:'Médico fisiatra',
223127:'Médico foniatra',
223128:'Médico gastroenterologista',
223129:'Médico generalista',
223130:'Médico geneticista',
223131:'Médico geriatra',
223132:'Médico ginecologista e obstetra',
223133:'Médico hematologista',
223134:'Médico hemoterapeuta',
223135:'Médico homeopata',
223136:'Médico infectologista',
223137:'Médico legista',
223138:'Médico mastologista',
223139:'Médico nefrologista',
223140:'Médico neurocirurgião',
223141:'Médico neurofisiologista clínico',
223142:'Médico neurologista',
223143:'Médico nutrologista',
223144:'Médico oftalmologista',
223145:'Médico oncologista clínico',
223146:'Médico ortopedista e traumatologista',
223147:'Médico otorrinolaringologista',
223148:'Médico patologista',
223149:'Médico pediatra',
223150:'Médico perito',
223151:'Médico pneumologista',
223152:'Médico coloproctologista',
223153:'Médico psiquiatra',
223154:'Médico radioterapeuta',
223155:'Médico reumatologista',
223156:'Médico sanitarista',
223157:'Médico urologista',
223158:'Médico broncoesofalogista',
223159:'Médico hansenologista',
223160:'Médico em cirurgia vascular',
223161:'Médico cancerologista pediátrico',
223162:'Médico da estratégia de saúde da família',
223163:'Médicocancerologista clínico',
223164:'Médico da estratégia de saúde da família',
223165:'Médico em medicina preventiva e social',
223166:'Médico residente',
223167:'Médico cardiologista intervencionista',
223204:'Cirurgião dentista - auditor',
223208:'Cirurgião dentista - clínico geral',
223212:'Cirurgião dentista - endodontista',
223216:'Cirurgião dentista - epidemiologista',
223220:'Cirurgião dentista - estomatologista',
223224:'Cirurgião dentista - implantodontista',
223228:'Cirurgião dentista - odontogeriatra',
223232:'Cirurgião dentista - odontologista legal',
223236:'Cirurgião dentista - odontopediatra',
223240:'Cirurgião dentista - ortopedista e ortodontista',
223244:'Cirurgião dentista - patologista bucal',
223248:'Cirurgião dentista - periodontista',
223252:'Cirurgião dentista - protesiólogo bucomaxilofacial',
223256:'Cirurgião dentista - protesista',
223260:'Cirurgião dentista - radiologista',
223264:'Cirurgião dentista - reabilitador oral',
223268:'Cirurgião dentista - traumatologista bucomaxilofacial',
223272:'Cirurgião dentista de saúde coletiva',
223276:'Cirurgião-dentista - odontologia do trabalho ',
223280:'Cirurgião-dentista - dentística ',
223284:'Cirurgião-dentista - disfunção temporomandibular e dor orofacial ',
223288:'Cirurgião-dentista - odontologia para pacientes com necessidades especiais ',
223293:'Cirurgião-dentista da Estratégia de Saúde da Família',
223305:'Médico veterinário',
223310:'Zootecnista',
223405:'Farmacêutico',
223410:'Farmacêutico bioquímico',
223415:'Farmacêutico analista clínico ',
223420:'Farmacêutico de alimentos ',
223425:'Farmacêutico práticas integrativas e complementares ',
223430:'Farmacêutico em saúde pública ',
223435:'Farmacêutico industrial ',
223440:'Farmacêutico toxicologista ',
223445:'Farmacêutico hospitalar e clínico ', 
223505:'Enfermeiro',
223510:'Enfermeiro auditor',
223515:'Enfermeiro de bordo',
223520:'Enfermeiro de centro cirúrgico',
223525:'Enfermeiro de terapia intensiva',
223530:'Enfermeiro do trabalho',
223535:'Enfermeiro nefrologista',
223540:'Enfermeiro neonatologista',
223545:'Enfermeiro obstétrico',
223550:'Enfermeiro psiquiátrico',
223555:'Enfermeiro puericultor e pediátrico',
223560:'Enfermeiro sanitarista',
223565:'Enfermeiro da Estratégia de Saúde da Família',
223570:'Perfusionista',
223605:'Fisioterapeuta geral',
223625:'Fisioterapeuta respiratória',
223630:'Fisioterapeuta neurofuncional',
223635:'Fisioterapeuta traumato-ortopédica funcional',
223640:'Fisioterapeuta osteopata',
223645:'Fisioterapeuta quiropraxistas',
223650:'Fisioterapeuta acupunturista',
223655:'Fisioterapeuta esportivo',
223660:'Fisioterapeuta do trabalho',
223665:'Técnico em orientação e mobilidade de cegos e deficientes visuais ',
223705:'Dietista',
223710:'Nutricionista',
223810:'Fonoaudiólogo',
223815:'Fonoaudiólogo educacional',
223820:'Fonoaudiólogo em audiologia ',
223825:'Fonoaudiólogo em disfagia ',
223830:'Fonoaudiólogo em linguagem ',
223835:'Fonoaudiólogo em motricidade orofacial ', 
223840:'Fonoaudiólogo em saúde coletiva ',
223845:'Fonoaudiólogo em voz ',
223905:'Terapeuta ocupacional',
223910:'Ortoptista',
223915:'Musicoterapeuta(Antigo)',
224105:'Avaliador Físico',
224110:'Ludomotricista',
224115:'Preparador de atleta',
224120:'Preparador físico',
224125:'Técnico de desporto individual e coletivo (exceto futebol)',
224130:'Técnico de laboratório e fiscalização desportiva',
224135:'Treinador profissional de futebol',
224140:'Profissional da educação física na saúde ',
225103:'Médico infectologista',
225105:'Médico acupunturista',
225106:'Médico legista',
225109:'Médico nefrologista',
225110:'Médico alergista e imunologista',
225112:'Médico neurologista',
225115:'Médico angiologista',
225118:'Médico nutrologista',
225120:'Médico cardiologista',
225121:'Médico oncologista clínico',
225122:'Médico cancerologista pediátrico',
225124:'Médico pediatra',
225125:'Médico clínico',
225127:'Médico pneumologista',
225130:'Médico de família e comunidade',
225133:'Médico psiquiatra',
225135:'Médico dermatologista',
225136:'Médico reumatologista',
225139:'Médico sanitarista',
225140:'Médico do trabalho',
225142:'Médico da estratégia de saúde da família',
225145:'Médico em medicina de tráfego',
225148:'Médico anatomopatologista',
225150:'Médico em medicina intensiva',
225151:'Médico anestesiologista',
225155:'Médico endocrinologista e metabologista',
225160:'Médico fisiatra',
225165:'Médico gastroenterologista',
225170:'Médico generalista',
225175:'Médico geneticista',
225180:'Médico geriatra',
225185:'Médico hematologista',
225195:'Médico homeopata',
225203:'Médico em cirurgia vascular',
225210:'Médico cirurgião cardiovascular',
225215:'Médico cirurgião de cabeça e pescoço',
225220:'Médico cirurgião do aparelho digestivo',
225225:'Médico cirurgião geral',
225230:'Médico cirurgião pediátrico',
225235:'Médico cirurgião plástico',
225240:'Médico cirurgião torácico',
225245:'Médico foniatra',
225250:'Médico ginecologista e obstetra',
225255:'Médico mastologista',
225260:'Médico neurocirurgião',
225265:'Médico oftalmologista',
225270:'Médico ortopedista e traumatologista',
225275:'Médico otorrinolaringologista',
225280:'Médico coloproctologista',
225285:'Médico urologista',
225290:'Médico cancerologista cirúrgico',
225295:'Médico cirurgião da mão',
225305:'Médico citopatologista',
225310:'Médico em endoscopia',
225315:'Médico em medicina nuclear',
225320:'Médico em radiologia e diagnóstico por imagem',
225325:'Médico patologista',
225330:'Médico radioterapeuta',
225335:'Médico patologista clínico - medicina laboratorial',
225340:'Médico hemoterapeuta',
225345:'Médico hiperbarista',
225350:'Médico neurofisiologista clínico',
226105:'Quiropraxista ',
226110:'Osteopata ',
226305:'Musicoterapeuta ',
226310:'Arteterapeuta ',
226315:'Equoterapeuta ',
226320:'Naturólogo ',
231315:'Professor de educação física no ensino fundamental',
232110:'Professor de biologia no ensino médio',
232120:'Professor de educação física no ensino médio',
233135:'Professor de técnicas de enfermagem',
234405:'Professor de ciências biológicas no ensino superior',
234410:'Professor de educação física no ensino superior',
234415:'Professor de enfermagem do ensino superior',
234420:'Professor de farmácia e bioquímica',
234425:'Professor de fisioterapia',
234430:'Professor de fonoaudiologia',
234435:'Professor de medicina',
234440:'Professor de medicina veterinária',
234445:'Professor de nutrição',
234450:'Professor de odontologia',
234455:'Professor de terapia ocupacional',
234460:'Professor de zootecnia do ensino superior',
251505:'Psicólogo educacional ',
251510:'Psicólogo clínico',
251515:'Psicólogo do esporte',
251520:'Psicólogo hospitalar',
251525:'Psicólogo jurídico',
251530:'Psicólogo social',
251535:'Psicólogo do trânsito',
251540:'Psicólogo do trabalho',
251545:'Neuropsicólogo',
251550:'Psicanalista',
251555:'Psicólogo acupunturista',
251605:'Assistente social ',
322105:'Técnico em acupuntura',
322110:'Podólogo',
322115:'Técnico em quiropraxia',
322120:'Massoterapeuta',
322125:'Terapeuta holístico',
322130:'Esteticista',
322135:'Doula ',
322205:'Técnico de enfermagem',
322210:'Técnico de enfermagem de terapia intensiva',
322215:'Técnico de enfermagem do trabalho',
322220:'Técnico de enfermagem psiquiátrica',
322225:'Instrumentador cirúrgico',
322230:'Auxiliar de enfermagem',
322235:'Auxiliar de enfermagem do trabalho',
322240:'Auxiliar de saúde (navegação marítima) ',
322245:'Técnico de enfermagem da Estratégia de Saúde da Família',
322250:'Auxiliar de enfermagem da Estratégia de Saúde da Família',
322255:'(Antigo) Perfusionista ',
322305:'Técnico em óptica e optometria',
322405:'Técnico em saúde bucal',
322410:'Protético dentário',
322415:'Auxiliar em saúde bucal',
322420:'Auxiliar de Prótese Dentária',
322425:'Técnico em saúde bucal da Estratégia de Saúde da Família',
322430:'Auxiliar em saúde bucal da Estratégia de Saúde da Família',
322505:'Técnico de ortopedia',
322605:'Técnico de imobilização ortopédica',
324105:'Técnico em métodos eletrográficos em encefalografia',
324110:'Técnico em métodos gráficos em cardiologia',
324115:'Técnico em radiologia e imagenologia',
324120:'Tecnólogo em radiologia',
324125:'Tecnólogo oftálmico ',
324190:'Técnico em equipamento médico hospitalar',
324205:'Técnico em patologia clínica',
324210:'Auxiliar técnico em patologia clínica',
325105:'Auxiliar técnico em laboratório de farmácia',
325110:'Técnico em laboratório de farmácia',
325115:'Técnico em farmácia',
325210:'Técnico em nutrição e dietética',
325305:'Técnico em biotecnologia',
325310:'Técnico em imunobiológicos',
351605:'Técnico em segurança do trabalho ',
351610:'Técnico em higiene ocupacional ',
352210:'Agente de saúde pública',
422110:'Recepcionista de consultório médico ou dentário',
422115:'Recepcionista de seguro de saúde',
515105:'Agente comunitário de saúde',
515110:'Atendente de enfermagem',
515115:'Parteira leiga',
515120:'Visitador sanitário',
515125:'Agente de saúde indígena',
515130:'Agente indígena de saneamento',
515135:'Socorrista (exceto médicos e enfermeiros)',
515140:'Agente de combate a endemias',
515205:'Auxiliar de banco de sangue',
515210:'Auxiliar de farmácia de manipulação',
515215:'Auxiliar de laboratório de análises clínicas',
515220:'Auxiliar de laboratório de imunobiológicos',
515225:'Auxiliar de produção farmacêutica',
515290:'Microscopista',
516210:'Cuidador de idosos',
516220:'Cuidador em saúde',
521130:'Atendente de farmácia – balconista',
810305:'Mestre de produção farmacêutica',
915305:'Técnico em manutenção de equipamentos e instrumentos médico-hospitalares'})

# Define as ocupações da saúde pelo CBO ANTIGO.
# *+*Altera o código da categoria "outros" de 45 para 99.

cnes['cod_cbo_saude'] = np.select([
(cnes['cod_cbo'] >= 223101) & (cnes['cod_cbo'] <= 223199),
(cnes['cod_cbo'] >= 223201) & (cnes['cod_cbo'] <= 223299),
(cnes['cod_cbo'] >= 223301) & (cnes['cod_cbo'] <= 223399),
(cnes['cod_cbo'] >= 223401) & (cnes['cod_cbo'] <= 223499),
(cnes['cod_cbo'] >= 223501) & (cnes['cod_cbo'] <= 223599),
(cnes['cod_cbo'] == 223605),
(cnes['cod_cbo'] >= 223701) & (cnes['cod_cbo'] <= 223799),
(cnes['cod_cbo'] == 223610),
(cnes['cod_cbo'] == 223620),
(cnes['cod_cbo'] >= 224101) & (cnes['cod_cbo'] <= 224199),
(cnes['cod_cbo'] == 221105),
(cnes['cod_cbo'] == 221205),
(cnes['cod_cbo'] >= 251501) & (cnes['cod_cbo'] <= 251599),
(cnes['cod_cbo'] >= 251601) & (cnes['cod_cbo'] <= 251699),
(cnes['cod_cbo'] == 324120),
(cnes['cod_cbo'] == 131215),
(cnes['cod_cbo'] == 142710),
(cnes['cod_cbo'] >= 203005) & (cnes['cod_cbo'] <= 203025),
(cnes['cod_cbo'] >= 203305) & (cnes['cod_cbo'] <= 203320),
(cnes['cod_cbo'] == 231315),
(cnes['cod_cbo'] == 232110),
(cnes['cod_cbo'] == 232120),
(cnes['cod_cbo'] == 233135),
(cnes['cod_cbo'] >= 234405) & (cnes['cod_cbo'] <= 234460),
(cnes['cod_cbo'] == 213150),
(cnes['cod_cbo'] >= 131205) & (cnes['cod_cbo'] <= 131220),
(cnes['cod_cbo'] >= 322205) & (cnes['cod_cbo'] <= 322245),
(cnes['cod_cbo'] == 322245),
(cnes['cod_cbo'] >= 322230) & (cnes['cod_cbo'] <= 322240),
(cnes['cod_cbo'] == 322250),
(cnes['cod_cbo'] == 515110),
(cnes['cod_cbo'] == 322405),
(cnes['cod_cbo'] == 322425),
(cnes['cod_cbo'] == 322415),
(cnes['cod_cbo'] == 322430),
(cnes['cod_cbo'] == 322410),
(cnes['cod_cbo'] == 322420),
(cnes['cod_cbo'] >= 322101) & (cnes['cod_cbo'] <= 322199),
(cnes['cod_cbo'] >= 322301) & (cnes['cod_cbo'] <= 322399),
(cnes['cod_cbo'] >= 322501) & (cnes['cod_cbo'] <= 322599),
(cnes['cod_cbo'] >= 322601) & (cnes['cod_cbo'] <= 322699),
(cnes['cod_cbo'] >= 324105) & (cnes['cod_cbo'] <= 324115),
(cnes['cod_cbo'] >= 324201) & (cnes['cod_cbo'] <= 324299),
(cnes['cod_cbo'] >= 325101) & (cnes['cod_cbo'] <= 325199),
(cnes['cod_cbo'] >= 325301) & (cnes['cod_cbo'] <= 325399),
(cnes['cod_cbo'] >= 351601) & (cnes['cod_cbo'] <= 351699),
(cnes['cod_cbo'] == 915305),
(cnes['cod_cbo'] == 515105),
(cnes['cod_cbo'] == 352210),
(cnes['cod_cbo'] >= 515115) & (cnes['cod_cbo'] <= 515135),
(cnes['cod_cbo'] >= 515201) & (cnes['cod_cbo'] <= 515299),
(cnes['cod_cbo'] == 516210),
(cnes['cod_cbo'] == 516220),
(cnes['cod_cbo'] == 422110),
(cnes['cod_cbo'] == 422115),
(cnes['cod_cbo'] == 521130),
(cnes['cod_cbo'] == 810305),
(cnes['cod_cbo'] == 223615),
(cnes['cod_cbo'] >= 225103) & (cnes['cod_cbo'] <= 225195),
(cnes['cod_cbo'] >= 225203) & (cnes['cod_cbo'] <= 225295),
(cnes['cod_cbo'] >= 225305) & (cnes['cod_cbo'] <= 225350),
(cnes['cod_cbo'] >= 223204) & (cnes['cod_cbo'] <= 223293),
(cnes['cod_cbo'] >= 223305) & (cnes['cod_cbo'] <= 223310),
(cnes['cod_cbo'] >= 223405) & (cnes['cod_cbo'] <= 223410),
(cnes['cod_cbo'] >= 223505) & (cnes['cod_cbo'] <= 223570),
(cnes['cod_cbo'] == 223605),
(cnes['cod_cbo'] >= 223625) & (cnes['cod_cbo'] <= 223665),
(cnes['cod_cbo'] >= 223705) & (cnes['cod_cbo'] <= 223710),
(cnes['cod_cbo'] >= 223810) & (cnes['cod_cbo'] <= 223845),
(cnes['cod_cbo'] == 223905),
(cnes['cod_cbo'] >= 224105) & (cnes['cod_cbo'] <= 224140),
(cnes['cod_cbo'] == 221105),
(cnes['cod_cbo'] == 221205),
(cnes['cod_cbo'] >= 251505) & (cnes['cod_cbo'] <= 251555),
(cnes['cod_cbo'] == 251605),
(cnes['cod_cbo'] == 324120),
(cnes['cod_cbo'] == 131215),
(cnes['cod_cbo'] == 142710),
(cnes['cod_cbo'] >= 203005) & (cnes['cod_cbo'] <= 203025),
(cnes['cod_cbo'] >= 203305) & (cnes['cod_cbo'] <= 203320),
(cnes['cod_cbo'] == 231315),
(cnes['cod_cbo'] == 232110),
(cnes['cod_cbo'] == 232120),
(cnes['cod_cbo'] == 233135),
(cnes['cod_cbo'] >= 234405) & (cnes['cod_cbo'] <= 234460),
(cnes['cod_cbo'] == 213150),
(cnes['cod_cbo'] >= 131205) & (cnes['cod_cbo'] <= 131210),
(cnes['cod_cbo'] >= 322205) & (cnes['cod_cbo'] <= 322225),
(cnes['cod_cbo'] == 322245),
(cnes['cod_cbo'] >= 322230) & (cnes['cod_cbo'] <= 322240),
(cnes['cod_cbo'] == 322250),
(cnes['cod_cbo'] == 322255),
(cnes['cod_cbo'] == 515110),
(cnes['cod_cbo'] == 322405),
(cnes['cod_cbo'] == 322425),
(cnes['cod_cbo'] == 322415),
(cnes['cod_cbo'] == 322430),
(cnes['cod_cbo'] == 322410),
(cnes['cod_cbo'] == 322420),
(cnes['cod_cbo'] >= 322105) & (cnes['cod_cbo'] <= 322135),
(cnes['cod_cbo'] == 322305),
(cnes['cod_cbo'] == 322505),
(cnes['cod_cbo'] == 322605),
(cnes['cod_cbo'] >= 324105) & (cnes['cod_cbo'] <= 324115),
(cnes['cod_cbo'] >= 324205) & (cnes['cod_cbo'] <= 324220),
(cnes['cod_cbo'] >= 325105) & (cnes['cod_cbo'] <= 325115),
(cnes['cod_cbo'] >= 325305) & (cnes['cod_cbo'] <= 325310),
(cnes['cod_cbo'] >= 351605) & (cnes['cod_cbo'] <= 351610),
(cnes['cod_cbo'] == 915305),
(cnes['cod_cbo'] == 515105),
(cnes['cod_cbo'] == 352210),
(cnes['cod_cbo'] >= 515115) & (cnes['cod_cbo'] <= 515135),
(cnes['cod_cbo'] >= 515205) & (cnes['cod_cbo'] <= 515225),
(cnes['cod_cbo'] == 516210),
(cnes['cod_cbo'] == 516220),
(cnes['cod_cbo'] == 422110),
(cnes['cod_cbo'] == 422115),
(cnes['cod_cbo'] == 521130),
(cnes['cod_cbo'] == 810305),
(cnes['cod_cbo'] >= 226100) & (cnes['cod_cbo'] <= 226199),
(cnes['cod_cbo'] >= 226300) & (cnes['cod_cbo'] <= 226399),
(cnes['cod_cbo'] == 223915),
(cnes['cod_cbo'] == 515140),
(cnes['cod_cbo'] == 325210),
(cnes['cod_cbo'] == 223910),
(cnes['cod_cbo'] == 324125),
(cnes['cod_cbo'] == 131220)], 
[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,17,17,17,17,17,
17,17,18,19,20,20,21,21,22,23,23,24,24,25,25,26,27,28,29,
30,31,32,33,34,35,36,37,37,38,39,40,41,42,43,44,49,1,1,1,
2,3,4,5,6,6,7,8,9,10,11,12,13,14,15,16,16,17,17,17,17,17,
17,17,18,19,20,20,21,21,5,22,23,23,24,24,25,25,26,27,28,
29,30,31,32,33,34,35,36,37,37,38,39,40,41,42,43,44,45,46,
46,47,48,49,50,51], default=99)

#  Nomeia as categorias de CBO saúde.
cnes['cod_cbo_saude_label'] = cnes.cod_cbo_saude.map({
1:'Médicos',
2:'Cirurgiões-dentistas',
3:'Veterinários',
4:'Farmacêuticos',
5:'Enfermeiros',
6:'Fisioterapeuta',
7:'Nutricionistas',
8:'Fonoaudiólogos',
9:'Terapeutas Ocupacionais',
10:'Profissionais da Educação Física',
11:'Biólogos',
12:'Biomédicos',
13:'Psicólogos e psicanalistas',
14:'Assistentes sociais',
15:'Tecnólogos em Radiologia',
16:'Tecnólogos em gestão hospitalar e sistemas biomédicos',
17:'Pesquisadores e professores das ciências biológicas e das ciências da saúde',
18:'Físico médico',
19:'Diretores e gerentes de serviços de sáude',
20:'Técnicos de enfermagem',
21:'Auxiliares de enfermagem',
22:'Atendente de enfermagem',
23:'Técnicos em saúde bucal',
24:'Auxiliares de saúde bucal',
25:'Protéticos dentários e auxiliares de prótese dentária',
26:'Tecnólogos e Técnicos em terapias alternativas e estéticas',
27:'Técnicos em óptica e optometria',
28:'Técnicos em próteses ortopédicas',
29:'Técnicos de imobilização ortopédica',
30:'Técnicos em equipamentos médicos e odontológicos',
31:'Técnicos e auxiliares técnicos em patologia clínica',
32:'Técnicos em manipulação farmacêutica',
33:'Técnicos de apoio à biotecnologia',
34:'Técnicos em segurança no trabalho',
35:'Técnico em manutenção de equipamentos e instrumentos médico-hospitalares',
36:'Agentes Comunitários de Saúde',
37:'Trabalhadores em serviços de promoção e apoio à saúde (exceto ACS, ACE e Atendente de Enfermagem)',
38:'Auxiliares de laboratório da saúde',
39:'Cuidadores de idosos',
40:'Cuidadores em saúde',
41:'Recepcionistas de consultório médico ou dentário',
42:'Recepcionistas de seguro de saúde',
43:'Atendentes de farmácia – balconista',
44:'Mestres de Produção Farmacêutica',
45:'Osteopatas e quiropraxistas',
46:'Profissionais das terapias criativas, equoterápicas e naturológicas',
47:'Agentes de Combate a Endemias',
48:'Técnicos em nutrição e dietética',
49:'Ortoptistas',
50:'Tecnólogos oftálmicos',
51:'Gerontólogos',
99:'Outros - não saúde'})

# Cria varável dummy de CBO Saúde
cnes['ind_cbo_saude'] = np.select([
(cnes['cod_cbo_saude'] == 99)],[0],default=1)

# Define a Label da Dummy de CBO Saúde.
cnes['ind_cbo_saude_label'] = cnes.ind_cbo_saude.map({
0:'Ocupação Não Saúde',
1:'Ocupação Saúde'})

# Cria dummy para ocupações da ABS
cnes['ind_cbo_abs'] = np.select([
(cnes['cod_cbo_saude'] == 1),
(cnes['cod_cbo_saude'] == 2),
(cnes['cod_cbo_saude'] == 5),
(cnes['cod_cbo_saude'] == 20),
(cnes['cod_cbo_saude'] == 21),
(cnes['cod_cbo_saude'] == 36),
(cnes['cod_cbo_saude'] == 23),
(cnes['cod_cbo_saude'] == 24)],
[1,1,1,1,1,1,1,1], default=0)

# Define a Label da Dummy de Ocupações da ABS.
cnes['ind_cbo_abs_label'] = cnes.ind_cbo_abs.map({
0:'Ocupação não ABS',
1:'Ocupação ABS'})

# Cria dummy para ocupações de Médicos
cnes['ind_cbo_medico'] = np.select([
(cnes['cod_cbo_saude'] == 1)], [1], default=0)

# Define a Label da Dummy de Ocupações de Médicos.
cnes['ind_cbo_medico_label'] = cnes.ind_cbo_medico.map({
0:'Não Médico',
1:'Médico'})

# Cria variável de especialidades médicas.
cnes['cod_cbo_especialidades'] = cnes.cod_cbo.replace({
225130:29,223116:29,223162:29,223164:29,225142:29,223115:16,225125:16,223129:16,225170:16,223102:2,225110:2,
223105:4,225115:4,223106:6,225120:6,223145:5,223161:5,225122:5,225290:5,223163:5,225121:5,223117:18,225135:18,
223159:18,223125:19,225155:19,223128:21,225165:21,223131:23,225180:23,223136:27,225103:27,223122:34,225150:34,
223139:38,225109:38,223141:38,225350:38,223142:40,225112:40,223149:47,225124:47,223151:48,225127:48,223158:48,
223153:49,225133:49,223119:49,223155:52,225136:52,223107:7,225210:7,223167:7,223108:9,225215:9,225295:8,223109:10,
225220:10,223110:11,225225:11,223111:12,225230:12,225190:25,223112:13,225235:13,223113:14,225240:14,223160:15,
225203:15,223152:17,225280:17,223132:24,225250:24,223138:28,225255:28,223140:39,225260:39,223144:42,225265:42,
223146:43,225270:43,223147:44,225275:44,223127:44,225245:44,223157:53,225285:53,223104:3,225151:3,223133:25,
223134:25,225185:25,225340:25,223120:20,225310:20,223123:36,225315:36,223143:41,225118:41,223148:45,223103:45,
223114:45,225325:45,225148:45,225305:45,223124:50,225320:50,223154:51,225330:51,225335:46,223101:1,
225105:1,223135:26,225195:26,223130:22,225175:22,223137:35,223150:35,225106:35,223118:30,225140:30,223156:37,
225139:37,223165:37,223121:31,225145:31,223126:54,225160:54,223166:54,225345:54})

# Cria a Label da variável de especialidades médicas
cnes['cod_cbo_especialidades_label'] = cnes.cod_cbo_especialidades.map({
1:'Acupuntura',
2:'Alergia e Imunologia',
3:'Anestesiologia',
4:'Angiologia',
5:'Cancerologia',
6:'Cardiologia',
7:'Cirurgia Cardiovascular',
8:'Cirurgia da Mão',
9:'Cirurgia de Cabeça e Pescoço',
10:'Cirurgia do Aparelho Digestivo',
11:'Cirurgia Geral',
12:'Cirurgia Pediátrica',
13:'Cirurgia Plástica',
14:'Cirurgia Torácica',
15:'Cirurgia Vascular',
16:'Clínica médica',
17:'Coloproctologia',
18:'Dermatologia',
19:'Endocrinologia e Metabologia',
20:'Endoscopia',
21:'Gastroenterologia',
22:'Genética Médica',
23:'Geriatria',
24:'Ginecologia e Obstetrícia',
25:'Hematologia e Hemoterapia',
26:'Homeopatia',
27:'Infectologia',
28:'Mastologia',
29:'Medicina de Família e Comunidade',
30:'Medicina do Trabalho',
31:'Medicina do Tráfego',
34:'Medicina Intensiva',
35:'Medicina Legal e Perícia Médica',
36:'Medicina Nuclear',
37:'Medicina Preventiva e social',
38:'Nefrologia',
39:'Neurocirurgia',
40:'Neurologia',
41:'Nutrologia',
42:'Oftalmologia',
43:'Ortopedia e Traumatologia',
44:'Otorrinoloaringologia',
45:'Patologia',
46:'Patologia clínica-medicina laboratorial',
47:'Pediatria',
48:'Pneumologia',
49:'Psiquiatria',
50:'Radiologia e Diagnóstico por Imagem',
51:'Radioterapia',
52:'Reumatologia',
53:'Urologia',
54:'Outras'})

# Cria agregação de grupo de especialidades médicas
cnes['cod_cbo_grupo_especialidades'] = cnes.cod_cbo.replace({
225130:1,223116:1, 223162:1,223164:1,225142:1,223115:1,225125:1,223129:1,225170:1,223102:2,225110:2,
223105:2,225115:2,223106:2,225120:2,223145:2,223161:2,225122:2,225290:2,223163:2,225121:2,223117:2,
225135:2,223159:2,223125:2,225155:2,223128:2,225165:2,223131:2,225180:2,223136:2,225103:2,223122:2,
225150:2,223139:2,225109:2,223141:2,225350:2,223142:2,225112:2,223149:2,225124:2,223151:2,225127:2,
223158:2,223153:2,225133:2,223119:2,223155:2,225136:2,223107:3,223167:3,223108:3,223109:3,223110:3,
223111:3,223112:3,223113:3,223160:3,223152:3,223132:3,223138:3,223140:3,223144:3,223146:3,223147:3,
223127:3,223157:3,225210:3,225215:3,225295:3,225220:3,225225:3,225230:3,225235:3,225240:3,225203:3,
225280:3,225250:3,225255:3,225260:3,225265:3,225270:3,225275:3,225285:3,223104:4,223133:4,223134:4,
223120:4,223123:4,223143:4,223148:4,223103:4,223114:4,223124:4,223154:4,225151:4,225185:4,225340:4,
225310:4,225315:4,225118:4,225325:4,225148:4,225305:4,225320:4,225330:4,225335:4,223101:5,223135:5,
223130:5,223137:5,223150:5,223118:5,223156:5,223165:5,223121:5,223126:5,223166:5,225105:5,225195:5,
225175:5,225106:5,225140:5,225139:5,225145:5,225160:5,225345:5,225245:3,225190:4})

# Cria a Label da variável de grupo de especialidades médicas
cnes['cod_cbo_grupo_especialidades_label'] = cnes.cod_cbo_grupo_especialidades.map({
1:'Atenção Primária',
2:'Especialidades Clínicas',
3:'Especialidades Cirúrgicas',
4:'Especialidades de Medicina Diagnóstica e Terapêutica',
5:'Outras especialidades'})

# Cria as categorias da Variável Carga Horária Total
cnes['cod_ch_ambulatorial'] = np.select ([
    (cnes['qtd_ch_ambulatorial'] >= 1) & (cnes['qtd_ch_ambulatorial'] <= 19),
    (cnes['qtd_ch_ambulatorial'] == 20),
    (cnes['qtd_ch_ambulatorial'] >= 21) & (cnes['qtd_ch_ambulatorial'] <= 29),
    (cnes['qtd_ch_ambulatorial'] == 30),
    (cnes['qtd_ch_ambulatorial'] >= 31) & (cnes['qtd_ch_ambulatorial'] <= 39),
    (cnes['qtd_ch_ambulatorial'] == 40),
    (cnes['qtd_ch_ambulatorial'] >= 41) & (cnes['qtd_ch_ambulatorial'] <= 80)], 
    [1,2,3,4,5,6,7], default=8)

cnes['cod_ch_total_label'] = cnes.cod_ch_ambulatorial.map({
1:'1 a 19 horas',
2:'20 horas',
3:'21 a 29 horas',
4:'30 horas',
5:'31 a 39 horas',
6:'40 horas',
7:'Mais de 40 horas',
8:'Sem informação ou inconsistente'})

cnes.to_parquet(f'/content/cnes_ch_{year}{month}.parquet.gzip',
              compression='gzip', index=False)