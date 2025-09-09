import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import hashlib
import io
from datetime import datetime
import re
from collections import defaultdict
import xml.dom.minidom as minidom
import os
import zipfile
import time
import pytz

# ================================================================================
# DADOS INTERNOS
# ================================================================================


def carregar_erros_ans():
    """
    Carrega a planilha UNIFICADA de erros da ANS que agora está embutida no código.
    O código do termo agora é a concatenação 'codigoErro-identificadorCampo'.
    """
    csv_data = """codigoErroUnico,descricaoErro
00-5001,Motivo Glosa: MENSAGEM ELETRÔNICA FORA DO PADRÃO TISS - Crítica ANS: [TISS 1.04 MODIFICADA] Erro de estrutura em relação ao XSD. Entende-se como erro de estrutura a informação de conteúdo no XML enviado pelas operadoras não previsto nos campos do XSD que define as terminologias da TUSS ou a falta de campos obrigatórios previstos neste mesmo XSD.
00-5002,"Motivo Glosa: NÃO FOI POSSÍVEL VALIDAR O ARQUIVO XML - Crítica ANS: .Caso o formato de compactação do arquivo enviado seja diferente de ZIP ou o método de compressão seja  diferente de DEFLATE
.Caso a operadora envie um arquivo ZTE que não possua nenhum arquivo a ser descompactado"
00-5014,"Motivo Glosa: CÓDIGO HASH INVÁLIDO. MENSAGEM PODE ESTAR CORROMPIDA. - Crítica ANS: O código HASH não está de acordo com o conteúdo do arquivo, o arquivo pode ter sido alterado após ser gerado ou está corrompido."
00-5016,Motivo Glosa: SEM NENHUMA OCORRENCIA DE MOVIMENTO NA COMPETENCIA PARA ENVIO A ANS - Crítica ANS: Este código informa a ANS que a operadora não teve nenhum movimento em determinada competencia. Este código é enviado pela operadora para a ANS.
00-5017,Motivo Glosa: ARQUIVO PROCESSADO PELA ANS - Crítica ANS: Este código no arquivo de retorno indica que o arquivo foi processado pela ANS
00-5023,"Motivo Glosa: COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - Crítica ANS: Caso a competência informada no nome do arquivo não esteja aberta para envio de dados e a operadora não possua uma exceção da competência cadastrada, ou a exceção da competência cadastrada possua uma data fim de envio menor que a data de recepção do arquivo, o arquivo será rejeitado "
00-5044,Motivo Glosa: JÁ EXISTEM INFORMAÇÕES NA ANS PARA A COMPETÊNCIA INFORMADA - Crítica ANS: Este erro ocorre quando a operadora envia um arquivo sem movimento (5016) e já existem lançamentos para aquela competência na base de dados da ANS
00-5045,Motivo Glosa: COMPETÊNCIA ANTERIOR NÃO ENVIADA - Crítica ANS: A competência anterior a que está sendo enviada não contem nenhum registro ativo na base de dados da ANS. Esta verificação só é realizada a partir da competência 02/2016
00-5046,"Motivo Glosa: COMPETÊNCIA INVÁLIDA - Crítica ANS: .Arquivos que forem descompactados devem possuir no seu nome a mesma competência informada no nome do seu arquivo ZTE de origem
.A competência informada no nome do arquivo deve ser maior ou igual ao ano e mês da data de registro da operadora.
.A competencia informada no nome do arquivo não existe no sistema de recepção da ANS.
.A competência expressa no nome do arquivo deve ser igual a competencia registrada internamente no cabeçalho deste mesmo arquivo"
01-,Motivo Glosa:  - Crítica ANS: 
02-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
03-5024,"Motivo Glosa: OPERADORA INATIVA NA COMPETÊNCIA DOS DADOS - Crítica ANS: Caso a operadora já esteja descredenciada na base de dados de operadoras da ANS, a competência dos dados enviados deve ser menor ou igual ao mês/ano da data de descredenciamento"
04-5025,"Motivo Glosa: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: Deve ser uma data positiva, menor ou igual à data atual e preenchida no formato AAAA–MM–DD"
05-5026,"Motivo Glosa: HORA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: Deve ser preenchida no formato HH:MM:SS. Se a data de registro da transação for igual à data atual, a hora de registro da transação deve ser menor ou igual à hora atual"
06-5027,"Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: .Deve ser um número de registro existente na base de dados de operadoras da ANS
.O número de registro da operadora informado no nome do arquivo enviado, deverá ser equivalente à operadora que enviou o arquivo via PTA
.O número de registro da operadora que constar no cabeçalho do arquivo deve ser equivalente ao número de registro informado no nome do arquivo enviado e equivalente à operadora que enviou o arquivo via PTA"
07-,Motivo Glosa:  - Crítica ANS: 
08-,Motivo Glosa:  - Crítica ANS: 
09-,Motivo Glosa:  - Crítica ANS: 
010-5028,"Motivo Glosa: VERSÃO DO PADRÃO INVÁLIDA - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchida quando a origem da guia for igual a 1 (Rede Contratada, referenciada ou credenciada), 2 (Rede Própria – Cooperados) ou 3 (Rede Própria – Demais prestadores).
Caso a guia de atendimento possua mais de um lançamento ativo, a operadora tentar incluir um novo lançamento ou tentar alterar um de seus lançamentos e a versão TISS do Prestador informado no lançamento for diferente do que consta no primeiro lançamento ativo da guia de atendimento, o lançamento será rejeitado."
011-,Motivo Glosa:  - Crítica ANS: 
012-1202,Motivo Glosa: NÚMERO DO CNES INVÁLIDO - Crítica ANS: Deve ser um código de CNES existente no Ministério da Saúde ou igual a '999999'
012-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
012-5063,Motivo Glosa: PAR CNPJ x CNES NAO ENCONTRADO NA BASE DO CNES - Crítica ANS: [TISS 1.04 NOVA] O par CNPJ do estabelecimento X CNES do prestador ou CNPJ da mantenedora X CNES do prestador deve existir na base de dados do CNES. 
012-5064,"Motivo Glosa: TIPO DE ESTABELECIMENTO NO CNES NÃO É APTO PARA INTERNAÇÃO - Crítica ANS: [TISS 1.04 NOVA] Quando o tipo de guia for igual a 3 – Internação, o CNES for diferente de ‘9999999’ e o tipo de prestador for pessoa jurídica, o tipo do estabelecimento do prestador, conforme registro na base do CNES, deve ser apto a realizar internações (HOSPITAL GERAL; HOSPITAL ESPECIALIZADO; UNIDADE MISTA; PRONTO SOCORRO GERAL; PRONTO SOCORRO ESPECIALIZADO; CLINICA/CENTRO DE ESPECIALIDADE; CENTRO DE PARTO NORMAL - ISOLADO; HOSPITAL/DIA - ISOLADO; SERVICO DE ATENCAO DOMICILIAR ISOLADO(HOME CARE))."
013-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Verifica se o identificador do executante é válido (1–CNPJ ou 2–CPF).
Em guias de resumo de internação deve ser obrigatoriamente igual a 1-CNPJ."
014-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: O CPF/CNPJ deve ser existente na base da Receita Federal
014-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
014-5065,"Motivo Glosa: TIPO DE ATIVIDADE ECONOMICA DO CNPJ NÃO É APTO PARA INTERNAÇÃO - Crítica ANS: [TISS 1.04 NOVA] Quando o tipo de guia for igual a 3 – Internação e o tipo de prestador for pessoa jurídica, o tipo de atividade econômica (CNAE) do prestador, conforme registro na base de pessoas jurídicas da Receita Federal, deve ser apto a realizar internações."
015-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
015-5030,Motivo Glosa: CÓDIGO DO MUNÍCIPIO INVÁLIDO - Crítica ANS: Deve ser um código de município válido na base de dados de municípios do IBGE
016-1002,Motivo Glosa: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - Crítica ANS: Quando informado seu conteúdo deve ser válido pelo cálculo do digito verificador
016-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
016-5086,"Motivo Glosa: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - Crítica ANS: [TISS 1.04 NOVA] Quando o número do CNS for encontrado na base de cadeia de CNS do SIB; e existir na base de cadeia de CNS do SIB apenas uma combinação das informações do beneficiário (CPF, sexo e data de nascimento) as informações de CPF (quando informado), sexo e data de nascimento devem ser iguais às informadas no SIB."
017-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
017-5077,Motivo Glosa: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - Crítica ANS: [TISS 1.04 NOVA] O sexo relacionado ao CPF informado deve ser igual ao registrado na base de dados da Receita Federal.
018-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Quando informada, deve ser uma data positiva, menor ou igual à data de realização do procedimento e preenchida no formato AAAA–MM–DD."
018-5078,Motivo Glosa: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - Crítica ANS: [TISS 1.04 NOVA] A data de nascimento relacionada ao CPF informado deve ser igual ao registrado na base de dados da Receita Federal.
019-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
019-5030,Motivo Glosa: CÓDIGO DO MUNÍCIPIO INVÁLIDO - Crítica ANS: Deve ser um código de município válido na base de dados de municípios do IBGE
020-1024,Motivo Glosa: PLANO NÃO EXISTENTE - Crítica ANS: Deve ser um número de registro de plano válido na base de dados de planos da operadora que realizou o envio do arquivo
020-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
021-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
022-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
023-1307,"Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Caso a operadora solicite a alteração ou exclusão de um lançamento de uma guia que não conste na base de dados ou não esteja ativo.
Quando a origem da guia for igual a 1 (rede contratada), 2 (rede própria-cooperados) ou 3 (rede própria-demais prestadores), o campo deve ser preenchido com o número da guia no prestador. Quando a origem da guia for igual a 4 (reembolso) ou 5 (Prestador eventual), o campo deve ser preenchido com uma sequência de 20 zeros nas operação de “Inclusão"" e ""Alteração”."
023-1308,Motivo Glosa: GUIA JÁ APRESENTADA - Crítica ANS: Inclusão de guia (chave completa) que já existe na base de dados da ANS
023-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
023-5073,"Motivo Glosa: O PRIMEIRO LANCAMENTO DA GUIA SÓ PODE SER EXCLUIDO SE ELE FOR O ÚNICO - Crítica ANS: [TISS 1.04 NOVA] Caso seja enviado uma exclusão do primeiro lançamento de uma guia e já existam outros lançamentos desta guia no banco de dados, este lançamento será rejeitado."
024-1307,"Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Quando a origem da guia for igual a 1 (rede contratada), 2 (rede própria-cooperados) ou 3 (rede própria-demais prestadores) o campo deve ser preenchido com o código atribuído pela operadora. 
Caso a operadora não possua um código ou a origem da guia for igual a 4(reembolso) ou 5 (Prestador eventual), ela deverá informar uma sequência de 20 zeros nas operações de “Inclusão"" ou ""Alteração”."
024-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
025-1307,"Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Quando a origem da guia for igual a 4(reembolso) o campo deve ser preenchido com o número de identificação de reembolso atribuído pela operadora e deve ser diferente de uma sequência de 20 zeros.
Quando a origem da guia – origemEventoAtencao for igual a 5 (Prestador Eventual) o campo identificação de reembolso deve ser preenchido com o número que identifica o pagamento a prestador não pertencente à rede da operadora e dever ser diferente de uma sequência de 20 números 0."
025-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
026-1307,"Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchido quando o tipo de guia for igual 3(internação) ou 5(honorários) e a origem da guia for igual a  1(rede contratada), 2(rede própria-cooperados) ou 3(rede própria-demais prestadores) nas transações de inclusão ou alteração.
Este número não pode ser informado nas guias odontológicas (GTO) nem nas guias de consultas.
O número da guia de solicitação, quando informado, não deve ser preenchido com zeros."
027-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Deve ser uma data positiva e preenchida no formato AAAA–MM–DD
028-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Quando informada, deve ser uma data positiva e preenchida no formato AAAA–MM–DD"
029-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Deve ser uma data positiva, menor que a data atual, menor ou igual à data de registro da transação e preenchida no formato AAAA–MM–DD"
030-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: [TISS 1.04 MODIFICADA] Quando informada, deve ser uma data positiva, menor que a data atual, menor ou igual à data de registro da transação e preenchida no formato AAAA–MM–DD."
031-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser uma data positiva, menor que a data atual, menor ou igual à data de registro da transação, maior ou igual à data de início do faturamento, maior ou igual a data de realização e preenchida no formato AAAA–MM–DD.
Esta data é de preenchimento obrigatório em guias de resumo de internação."
032-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Deve ser uma data positiva, menor que a data atual, menor ou igual à data de registro da transação e preenchida no formato AAAA–MM–DD"
033-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Deve ser uma data positiva, menor que a data atual e preenchida no formato AAAA–MM–DD"
034-1603,"Motivo Glosa: TIPO DE CONSULTA INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchido quando tipo de guia for igual a 1 (consulta) e a origem da guia for igual a 1 (rede contratada), 2 (rede própria-cooperados) ou 3 (rede própria-demais prestadores);   
- OU -
tipo de guia for igual a 2 (SP/SADT), o tipo de atendimento for igual a 4 e a origem da guia for igual a 1 (rede contratada), 2 (rede própria-cooperados) ou 3 (rede própria-demais prestadores)."
035-1213,"Motivo Glosa: CBO (ESPECIALIDADE) INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchido quando tipo de guia for igual a 1 (consulta); ou 2 (SP/SPDAT) e o tipo de atendimento for igual a 4 (Consulta); ou 4 (Tratamento Odontológico) e o tipo de atendimento for igual a 4 (Consulta); e a origem da guia for uma das seguintes opções: 1 (Rede Contratada, referenciada ou credenciada); ou 2 (Rede Própria - Cooperados); ou 3 (Rede Própria - Demais prestadores).
Não deve ser preenchido quando o tipo de guia for igual a 3 (Internação); ou 5 (Honorários).
O CBO do executante não pode ser igual a 999999."
035-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
036-5032,"Motivo Glosa: INDICADOR DE RECÉM-NATO INVÁLIDO - Crítica ANS: Deve ser preenchido quando o tipo de guia for igual a 1(consulta), 2(SP/SADT) ou 3(internação)"
037-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser um código válido na TUSS.
Deve ser preenchido quando o tipo de guia for igual a 1-Consulta, 2-SP/SADT ou 3-Resumo de Internação e a origem da guia for igual a 1 - Rede Contratada, referenciada ou credenciada, 2 - Rede Própria - Cooperados ou 3 - Rede Própria - Demais prestadores.
Não deve ser preenchido quando os tipos de guia forem iguais a 4 - Tratamento Odontológico ou 5 - Honorários"
038-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
038-5031,"Motivo Glosa: CARÁTER DE ATENDIMENTO INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchido quando o tipo de guia for igual 2 (SP/SADT) ou 3 (internação) e a origem da guia for uma das seguintes opções: 1 (Rede Contratada, referenciada ou credenciada); ou 2 (Rede Própria - Cooperados); ou 3 (Rede Própria - Demais prestadores).
Não deve ser preenchido quando o tipo de guia for igual a 1 (Consulta), 4 (Tratamento Odontológico) ou 5 (Honorários)."
039-1506,"Motivo Glosa: TIPO DE INTERNAÇÃO INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchido quando o tipo de guia for igual a 3 (internação) e a origem da guia for igual a 1 (rede contratada), 2 (rede própria-cooperados) ou 3 (rede própria-demais prestadores).
Não deve ser preenchido quando o tipo de guia for igual a 1 (Consulta), 2 (SP/SADT), 4 (Tratamento Odontológico) ou 5 (Honorários)."
039-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
040-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser um código válido na TUSS.
Deve ser preenchido quando o tipo de guia for igual 3-Resumo de Internação e a origem da guia for igual a 1 - Rede Contratada, referenciada ou credenciada, 2 - Rede Própria - Cooperados ou 3 - Rede Própria - Demais prestadores."
041-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Quando informado deve ser um código de CID válido na base de dados de CID e estar vigente
041-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44
No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID de primeiro diagnóstico seja informado, este deve ser igual ao que já estava na base de dados para esta guia
Este campo só pode ser informado em guias de resumo de internação"
041-5067,"Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: [TISS 1.04 NOVA] Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44.
Este campo só pode ser informado em guias de resumo de internação."
041-5075,"Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
042-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Quando informado deve ser um código de CID válido na base de dados de CID e estar vigente
042-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44
No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID de segundo diagnóstico seja informado, este deve ser igual ao que já estava na base de dados para esta guia
Este campo só pode ser informado em guias de resumo de internação"
042-5067,"Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: [TISS 1.04 NOVA] Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44.
Este campo só pode ser informado em guias de resumo de internação."
042-5075,"Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
043-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Quando informado deve ser um código de CID válido na base de dados de CID e estar vigente
043-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44
No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID de terceiro diagnóstico seja informado, este deve ser igual ao que já estava na base de dados para esta guia
Este campo só pode ser informado em guias de resumo de internação"
043-5067,"Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: [TISS 1.04 NOVA] Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44.
Este campo só pode ser informado em guias de resumo de internação."
043-5075,"Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
044-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Quando informado deve ser um código de CID válido na base de dados de CID e estar vigente
044-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44
No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID de quarto  diagnóstico seja informado, este deve ser igual ao que já estava na base de dados para esta guia
Este campo só pode ser informado em guias de resumo de internação"
044-5067,"Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: [TISS 1.04 NOVA] Não deve haver repetição do conteúdo do CID nos campos 41,42,43 e 44.
Este campo só pode ser informado em guias de resumo de internação."
044-5075,"Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) caso o CID seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
045-1602,"Motivo Glosa: TIPO DE ATENDIMENTO INVÁLIDO OU NÃO INFORMADO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchido quando o tipo de guia for igual 2 (SP/SADT) e a origem da guia for igual a 1 (rede contratada), 2 (rede própria-cooperados) ou 3 (rede própria-demais prestadores).
Na inclusão do primeiro lançamento não poderá haver as seguintes opções para o tipo de atendimento: 05,06,07,11,14,15,16,17,18,19,20,21 ou 22. Quando a versão enviada pelo prestador for menor que 23, não poderá ser informado código de tipo de atendimento maior que 22.
Caso a operadora tente incluir um lançamento ou alterar o único lançamento ativo de uma guia de atendimento presente na base de dados e o tipo de atendimento informado no lançamento for diferente do que consta no primeiro lançamento ativo da guia de atendimento, o lançamento será rejeitado."
045-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
046-1713,"Motivo Glosa: FATURAMENTO INVÁLIDO - Crítica ANS: Deve ser preenchido quando o tipo de guia for igual 3(internação) ou 4(GTO) e a origem da guia for igual a 1(rede contratada), 2(rede própria-cooperados) ou 3(rede própria-demais prestadores) "
046-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
047-1304,Motivo Glosa: COBRANÇA EM GUIA INDEVIDA - Crítica ANS: Só pode ser preenchido quando o tipo de guia for igual a 3(internação)
048-1304,Motivo Glosa: COBRANÇA EM GUIA INDEVIDA - Crítica ANS: Só pode ser preenchido quando o tipo de guia for igual a 3(internação)
049-5033,"Motivo Glosa: MOTIVO DE ENCERRAMENTO INVÁLIDO - Crítica ANS: Deve ser preenchido quando o tipo de guia for igual a 3(internação) e a origem da guia for igual a 1(rede contratada), 2(rede própria-cooperados) ou 3(rede própria-demais prestadores).
Deve ser um código válido na base de dados de termos da tabela TUSS, vinculado a tabela TUSS 39"
050-1705,"Motivo Glosa: VALOR APRESENTADO A MAIOR - Crítica ANS: .Caso seja enviado um lançamento de inclusão para uma guia que já exista no banco de dados da ANS, o valor total informado da guia enviado no arquivo não pode ser maior que o valor total informado existente no banco de dados.
.Caso seja enviado um lançamento de alteração  e já houver mais de um lançamento da guia no banco de dados,  o valor total informado no arquivo enviado não pode ser maior que o valor que está no banco de dados da ANS."
050-1706,"Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: .Caso seja enviado um lançamento de inclusão para uma guia que já exista no banco de dados da ANS, o valor total informado da lançamento enviado no arquivo não pode ser menor que o valor total informado existente no banco de dados.
.Caso seja enviado um lançamento de alteração  e já houver mais de um lançamento da guia no banco de dados,  o valor total informado no lançamento enviado não pode ser menor que o valor que está no banco de dados da ANS.
.Caso seja enviado um lançamento de inclusão e já exista um lançamento ativo desta mesma guia na base de dados da ANS, o valor total informado no lançamento deve ser igual ou maior que o valor informado nos itens deste lançamento."
050-5042,"Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: [TISS 1.04 MODIFICADA] Se o 'tipoRegistro' for igual a Inclusão e não exista lançamentos na base de dados para a mesma guia ou o 'tipoRegistro' for igual a Alteração e exista apenas um lançamento do tipo Inclusão na base de dados para a mesma guia, o 'valorTotalInformado' deve ser igual à soma do 'valorInformado' dos procedimentos/itens assistenciais informados no arquivo e já existentes na base de dados em outros lançamentos ativos da mesma guia."
050-5083,Motivo Glosa: SOMA DOS VALORES DOS MODELOS DE REMUNERAÇÃO DIFERENTE DO VALOR INFORMADO DA GUIA - Crítica ANS: [TISS 1.04 NOVA] A soma de todos os valores das formas de remuneração deve ser igual ao valor total informado da guia.
051-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor processado da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
052-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: .O valor do total pago de procedimento enviado no lançamento somado ao valor total pago de procedimentos já existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago de procedimentos deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
052-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de procedimentos da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
053-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: .O valor do total pago de diárias enviado no lançamento somado ao valor total pago de diárias já existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago de diárias deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
053-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de diárias da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
054-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago de taxas enviado no lançamento somado ao valor total pago de taxas já existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago de taxas deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
054-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de taxas da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
055-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago de materiais enviado no lançamento somado ao valor total pago de materiais já existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago de materiais deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
055-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de materiais da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
056-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago de OPME enviado no lançamento somado ao valor total pago de OPME já existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago de OPME deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
056-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de OPME da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
057-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago de medicamentos enviado no lançamento somado ao valor total pago de medicamentos já existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago de medicamentos deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
057-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de medicamentos da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
058-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago de glosa da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
059-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago enviado no lançamento somado ao valor total pago na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
059-5034,"Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: .O valor total pago deve ser igual a soma do 'valorTotalPagoProcedimentos', 'valorTotalDiarias', 'valorTotalTaxas', 'valorTotalMateriais', 'valorTotalOPME', 'valorTotalMedicamentos', informados no mesmo lançamento
.O valor total pago da guia deve ser maior ou igual a zero nas operações de Inclusão ou Alteração"
059-5042,"Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: [TISS 1.04 MODIFICADA] Se o 'tipoRegistro' for igual a Inclusão ou Alteração, o 'valorPagoGuia' informado no arquivo deve ser igual à soma do 'valorPagoProc' dos procedimentos/itens assistenciais informados no arquivo e já existentes na base de dados de procedimentos para o mesmo lançamento da guia. "
060-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago diretamente aos fornecedores enviado no lançamento somado ao valor total pago diretamente aos fornecedores existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago aos fornecedores deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
060-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago diretamente ao fornecedor deve ser maior ou igual a zero
060-5042,"Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: [TISS 1.04 MODIFICADA] Se o 'tipoRegistro' for igual a Inclusão ou Alteração, o 'valorPagoFornecedores' informado no arquivo deve ser igual à soma do 'valorPagoFornecedor' dos procedimentos/itens assistenciais informados no arquivo e já existentes na base de dados de procedimentos para o mesmo lançamento da guia. "
061-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor do total pago em tabela própria enviado no lançamento somado ao valor total pago em tabela própria existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor total pago em tabela própria deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
061-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago em tabela própria deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
062-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] A informação da Declaração de Nascido Vivo só deve constar em guias de Resumo de Internação.
062-5066,Motivo Glosa: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE. - Crítica ANS: [TISS 1.04 NOVA] Não pode haver repetição do mesmo número nas ocorrências deste campo.
062-5068,"Motivo Glosa: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) ou alteração, caso o numero da declaração de nascido vivo seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
063-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] A Declaração de Óbito só deve constar em guias de resumo de internação.
063-5034,"Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: [TISS 1.04 MODIFICADA] Deve ser preenchida quando o tipo de guia for igual a 3 (Resumo de Internação, e a origem da guia for uma das seguintes opções: 1 (Rede Contratada, referenciada ou credenciada), 2 (Rede Própria - Cooperados) ou 3 (Rede Própria - Demais prestadores), e quando o ‘motivoSaida’ for igual a 41 (Óbito com declaração de óbito fornecida pelo médico assistente), 63 (Alta da mãe/puérpera e óbito do recém-nascido), 65 (Óbito da gestante e do concepto), 66 (Óbito da mãe/puérpera e alta do recém-nascido)  ou 67 (Óbito da mãe/puérpera e permanência do recém-nascido)."
063-5066,Motivo Glosa: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE. - Crítica ANS: [TISS 1.04 NOVA] Não pode haver repetição do mesmo número nas ocorrências deste campo.
063-5068,"Motivo Glosa: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) ou alteração caso o número da declaração de óbito seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
064-1801,"Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] A partir do segundo lançamento de uma guia, o par informado nos campos 64 e 65 (Código do Grupo) ou nos campos 64 e 66 (Código do Procedimento) já devem existir no primeiro lançamento."
064-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: A) Deve ser um código válido na TUSS
B) Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado pois o sistema irá remover estes caracteres, fazendo com que o elemento fique vazio"
064-5035,Motivo Glosa: CÓDIGO DA TABELA DE REFERÊNCIA NÃO INFORMADO - Crítica ANS: Deve ser um código válido na base de dados de termos da tabela TUSS 87
064-5053,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Não deve haver repetição do campo tabela de referência e código do grupo ou item assistencial. Quando for guia odontológica, a regra verificará também código do dente, face e região da boca, sendo considerado duplicidade apenas se estes campos forem iguais."
065-1801,"Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: A partir do segundo lançamento de uma guia, o par informado nos campos 64 e 65 já devem existir no primeiro lançamento"
065-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: A) Deve ser um código válido na TUSS
B) Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado pois o sistema irá remover estes caracteres, fazendo com que o elemento fique vazio"
065-5036,Motivo Glosa: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - Crítica ANS: Deve ser um código válido na base de termos da tabela TUSS 63
065-5053,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Não deve haver repetição do campo tabela de referência e código do grupo ou item assistencial. Quando for guia odontológica, a regra verificará também código do dente, face e região da boca, sendo considerado duplicidade apenas se estes campos forem iguais."
066-1801,"Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: A) Quando não for código próprio da operadora ou pacote (indicados pelos códigos 00, 90 ou 98 no campo 64), deve ser um código existente na  TUSS, conforme código da tabela de referência informada.
B) A partir do segundo lançamento de uma guia, o par informado nos campos 64 e 66 já devem existir no primeiro lançamento."
066-2601,Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO. - Crítica ANS: A forma de envio do procedimento informado deve ser consolidado (informação na tabela TUSS 64)
067-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: .Deve ser um código válido na TUSS
.No lançamento de inclusão ou alteração o código do dente deve ser o mesmo para o item/procedimento já informado em lançamento anterior da mesma guia"
067-5069,"Motivo Glosa: CÓDIGO DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] Deve ser um código válido na TUSS.
No lançamento de inclusão ou alteração o código do dente deve ser o mesmo para o item/procedimento já informado em lançamento anterior da mesma guia."
068-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: .Deve ser um código válido na TUSS
.No lançamento de inclusão ou alteração a identificação da região da boca deve ser a mesma para o item/procedimento já informado em lançamento anterior da mesma guia"
068-5070,"Motivo Glosa: CÓDIGO DA REGIÃO DA BOCA DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] Deve ser um código válido na TUSS.
No lançamento de inclusão ou alteração a identificação da região da boca deve ser a mesma para o item/procedimento já informado em lançamento anterior da mesma guia."
069-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: No lançamento de inclusão ou alteração a identificação da face do dente deve ser a mesma para o item/procedimento já informado em lançamento anterior da mesma guia
069-5039,"Motivo Glosa: CÓDIGO DA FACE DO DENTE INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Não pode ser informado quando o tipo de guia for diferente de 4 (GTO). 
Quando preenchido, o campo poderá ser composto por até 5 códigos de termos concatenados. Dentre esses 5 códigos, não poderá haver repetição de código e os códigos devem ser válidos na tabela TUSS 32."
069-5071,Motivo Glosa: CÓDIGO DA FACE DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão ou alteração a identificação da face do dente deve ser a mesma para o item/procedimento já informado em lançamento anterior da mesma guia.
070-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: A quantidade informada de procedimentos deve ser maior que zero
071-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: O valor informado no lançamento de inclusão ou alteração deve ser igual ao já existente em outro lançamento da mesma guia para o mesmo procedimento/item
071-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor informado do item deve ser maior ou igual a zero nas operações de Inclusão ou Alteração
071-5072,Motivo Glosa: VALOR INFORMADO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: [TISS 1.04 NOVA] O valor informado no lançamento de inclusão ou alteração deve ser igual ao já existente em outro lançamento da mesma guia para o mesmo procedimento/item.
072-1806,"Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: Se o valor pago do procedimento for maior que zero, a quantidade de procedimentos paga deve ser maior que zero"
072-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: A quantidade paga de itens deve ser maior ou igual a zero
073-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor pago do procedimento enviado no lançamento somado ao valor pago do procedimento existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor pago do procedimento deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
073-5034,"Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: .O valor pago do item deve ser maior ou igual a zero
.Se a quantidade paga do item for maior que zero, o valor pago do item deve ser maior que zero"
074-1740,"Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: O valor pago ao fornecedor enviado no lançamento somado ao valor pago ao fornecedor existente na base de dados da ANS não deve ser negativo. Caso isso ocorra, é sinal que a operadora está enviando um valor pago negativo (estorno) maior que o valor que já foi pago pela guia.
.Quando houver um lançamento de exclusão, o valor pago diretamente ao fornecedor deve permanecer positivo nos lançamentos restantes na base para a mesma guia."
074-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor pago do item diretamente ao fornecedor deve ser maior ou igual a zero
075-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: O CNPJ deve ser existente na base da Receita Federal
076-,Motivo Glosa:  - Crítica ANS: 
077-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: A) Deve ser um código válido na TUSS
B) Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado pois o sistema irá remover estes caracteres, fazendo com que o elemento fique vazio"
077-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Não deve haver repetição dos campos tabela de referencia e código do item dentro do pacote
078-1801,"Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Quando não for código próprio da operadora ou pacote (indicados pelos códigos 00, 90 ou 98 no campo 77), deve ser um código existente na  TUSS, conforme código da tabela de referência informada.
A partir do segundo lançamento de uma guia, o procedimento informado no campo 78 já deve existir no primeiro lançamento."
078-2601,Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO. - Crítica ANS: A forma de envio do procedimento informado deve ser consolidado (informação na tabela TUSS 64)
078-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Não deve haver repetição dos campos tabela de referencia e código do item dentro do pacote
079-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: A quantidade informada de procedimentos no pacote deve ser maior que zero
080-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Deve ser uma data positiva, menor que a data atual e preenchida no formato AAAA–MM–DD"
080-5025,Motivo Glosa: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: O mês/ano da data de processamento deve ser igual ao mês/ano da competência do arquivo nos casos de transação de inclusão
081-5027,"Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: .O registro deve corresponder a uma operadora existente na base de dados de operadoras e deve ser diferente da operadora que realizou o envio do arquivo.
.Caso já exista um lançamento da mesma guia na base de dados, o registro da operadora intermediária deve ser igual ao registro da operadora informado no lançamento anterior."
081-5062,"Motivo Glosa: REGISTRO ANS DA OPERADORA INTERMEDIÁRIA NÃO INFORMADO - Crítica ANS: O campo ""Tipo de atendimento por operadora intermediária"" foi informado e o campo ""Registro ANS da operadora intermediária"" não está preenchido."
082-5050,"Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Se já existir um lançamento no banco de dados da ANS para a mesma guia, o identificador de contratação por valor pré-estabelecido deve ser o mesmo enviado anteriormente"
082-5052,"Motivo Glosa: IDENTIFICADOR INEXISTENTE - Crítica ANS: Quando o tipo de guia for igual a internação (3), a identificacao do valor preestabelecido deve existir na base de dados dos valores preestabelecidos para o mesmo prestador ou para a operadora intermediária informado no lançamento e estar ativo.
Quando o tipo de guia for diferente de internação, a identificacao do valor preestabelecido deve existir na base de dados dos valores preestabelecidos para o mesmo prestador ou para a operadora intermediária informado no lançamento, na competência equivalente ao mês e ano da data de realizacao informada no lançamento e estar ativo."
083-1307,"Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Quando o tipo da guia for igual a 2 (SP/SADT) ou 4 (GTO) e a origem da guia for igual a 1(rede contratada), 2(rede própria-cooperados) ou 3(rede própria-demais prestadores), o número da guia principal de SP/SADT ou da guia principal de tratamento odontológico deve ser preenchido com o número da guia atribuído pelo prestador. Quando a origem da guia for igual a 4(reembolso) e o número da guia principal de SP/SADT for informado no arquivo, seu valor deve ser preenchido com uma sequência de 20 zeros. 
O número da guia principal de SADT só deve ser informado em uma guia de SP/SADT ou guia de tratamento odontológico.
Não deve ser preenchido quando o tipo de guia for igual a 1 – Consulta, 3 – Internação ou 5 – Honorários."
084-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total de coparticipação deve ser maior ou igual a zero
084-5050,"Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: O valor total de coparticipação deve ser igual ao somatório do valor de coparticipação dos itens (campo 085) nas guias de consulta, SP/SADT de beneficiário não internado e guia de tratamento odontológico. No caso da guia de resumo de internação, honorário profissional e da guia de SP/SADT de beneficiário internado, deve-se preencher apenas o valor total de coparticipação sem indicar valor de coparticipação nos itens.
No caso de um lançamento de alteração, o valor total de coparticipação deve ser igual ao valor dos itens que estão no arquivo para alteração mais o valor dos itens de coparticipação que por ventura já existam no banco de dados para o mesmo lançamento. A crítica na alteração obedece a mesma lógica da transação de inclusão quanto ao tipo de guia que está sendo enviado."
085-5034,"Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: A) O valor de coparticipação no item deve ser maior ou igual a zero
B) Caso o valor de coparticipação do item seja nulo (tag vazia) o lançamento será rejeitado."
085-5050,"Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: o valor de coparticipação do item deve ser igual a zero nas guias de resumo de internação, honorário profissional e SP/SADT de beneficiário internado. Nas outras situações ele deve ser maior ou igual a zero."
086-,Motivo Glosa:  - Crítica ANS: 
088-1202,Motivo Glosa: NÚMERO DO CNES INVÁLIDO - Crítica ANS: Deve ser um código de CNES existente no Ministério da Saúde ou igual a '999999'
089-,Motivo Glosa:  - Crítica ANS: 
090-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: O CNPJ deve ser existente na base da Receita Federal
091-5030,Motivo Glosa: CÓDIGO DO MUNÍCIPIO INVÁLIDO - Crítica ANS: Deve ser um código de município válido na base de dados de municípios do IBGE
092-5027,Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: O registro deve corresponder a uma operadora existente na base de dados de operadoras e deve ser diferente da operadora que realizou o envio do arquivo
093-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
093-5053,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: [TISS 1.04 MODIFICADA] Não deve haver repetição do identificador do valor pré-estabelecido para outra contratação informada à ANS.
Quando o campo Operadora Intermediária for informado a chave será:
  -Número do registro da operadora na ANS que realizou o envio do arquivo;
  -Número do registro da operadora na ANS intermediária;
  -Competência da Cobertura Contratada;
  -Número de identificação do valor preestabelecido.
Quando o campo Operadora Intermediária não for informado a chave será:
  -Número do registro da operadora na ANS que realizou o envio do arquivo;
  -Número do CNES do prestador recebedor;
  -Indicador de identificador do prestador;
  -Número do CPF/CNPJ do prestador;
  -Código do município do prestador;
  -Competência da Cobertura Contratada;
  -Número de identificação do valor preestabelecido."
093-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Acontece quando a operadora envia um lançamento de alteração ou exclusão de contratação por valor pré-estabelecido e o registro não é encontrado no banco de dados da ANS
093-5059,Motivo Glosa: EXCLUSÃO INVÁLIDA - EXISTEM LANÇAMENTOS VINCULADOS A ESTA FORMA DE CONTRATAÇÃO - Crítica ANS: Não é possível excluir um registro de identificação de valor pré-estabelecido enquanto houver lançamentos vinculados a ele
094-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor preestabelecido deve ser maior que zero
095-,Motivo Glosa:  - Crítica ANS: 
096-1002,Motivo Glosa: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - Crítica ANS: Quando informado seu conteúdo deve ser válido pelo cálculo do digito verificador
096-5086,"Motivo Glosa: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - Crítica ANS: [TISS 1.04 NOVA] Quando o número do CNS for encontrado na base de cadeia de CNS do SIB; e existir na base de cadeia de CNS do SIB apenas uma combinação das informações do beneficiário (CPF, sexo e data de nascimento) as informações de CPF (quando informado), sexo e data de nascimento devem ser iguais às informadas no SIB."
097-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
097-5077,Motivo Glosa: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - Crítica ANS: [TISS 1.04 NOVA] O sexo relacionado ao CPF informado deve ser igual ao registrado na base de dados da Receita Federal.
098-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: A data deve ser preenchida no formato AAAA–MM–DD, ser menor que a data atual e menor ou igual a data de registro da transacao (campo 004)"
098-5078,Motivo Glosa: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - Crítica ANS: [TISS 1.04 NOVA] A data de nascimento relacionada ao CPF informado deve ser igual ao registrado na base de dados da Receita Federal.
099-5030,Motivo Glosa: CÓDIGO DO MUNÍCIPIO INVÁLIDO - Crítica ANS: Deve ser um código de município válido na base de dados de municípios do IBGE
0100-1024,Motivo Glosa: PLANO NÃO EXISTENTE - Crítica ANS: Deve ser um número de registro de plano válido na base de dados de planos da operadora que realizou o envio do arquivo
0101-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 MODIFICADA] Se neste campo for informado apenas espaços em branco, caractere de quebra de linha ou de tabulação, o lançamento será rejeitado."
0101-5053,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: [TISS 1.04 MODIFICADA] Não deve haver repetição de identificador de fornecimento direto de materiais e medicamentos.
Chave para identificação de um fornecimento direto:
-Número do registro da operadora na ANS que realizou o enviou do arquivo;
-Número de identificação do fornecimento direto;
-Data do fornecimento"
0101-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Acontece quando a operadora envia um lançamento de alteração ou exclusão de fornecimento de materiais e medicamentos e o registro não é encontrado no banco de dados da ANS
0102-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: A data deve ser preenchida no formato AAAA–MM–DD, ser menor que a data atual e seu ano e mês devem ser equivalentes ao ano e mês contidos no campo competencia dos dados (campo 003)"
0102-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - Crítica ANS: O ano e mês da data de fornecimento informada no arquivo devem corresponder a uma competência vigente na base de dados de competências na data em que o arquivo foi recepcionado na base de dados do PTA.
0103-1706,Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: O valor total dos itens fornecidos deve ser maior ou igual a soma do valor fornecido dos procedimentos/itens assistenciais informados no arquivo e já existentes na base de dados.
0103-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total dos itens fornecidos deve ser maior ou igual a zero
0103-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS:  O valor total dos itens fornecidos deve ser igual a soma do valor fornecido dos procedimentos/itens assistenciais (campo 110)
0104-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total dos itens fornecidos com código em tabela própria  deve ser maior ou igual a zero
0105-1706,"Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: No caso de um lançamento de alteração de fornecimento direto, o valor total de coparticipação deve ser maior ou igual a soma do valor de coparticipação dos itens informados no lançamento e deve ser igual ao somatório do valor de coparticipação dos itens alterados mais o valor dos outros itens que por ventura já existam para o mesmo lançamento"
0105-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total da coparticipação deve ser maior ou igual a zero
0105-5050,"Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: No caso de um lançamento de inclusão de fornecimento direto, o valor total de coparticipação deve ser igual a soma do valor de coparticipação dos itens "
0106-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
0106-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Não deve haver repetição da tabela de referência em conjunto com o código do grupo ou item assistencial no fornecimento direto de materiais e medicamentos
0106-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: [TISS 1.04 MODIFICADA] Se o 'tipoRegistro' for igual a Alteração o 'códigoTabela' já deve existir na base de dados para o mesmo lançamento em alteração. 
0107-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Deve ser um código válido na TUSS
0107-5036,Motivo Glosa: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - Crítica ANS: Deve ser um código válido na base de termos da tabela TUSS 63
0107-5053,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: [TISS 1.04 MODIFICADA] Não deve haver repetição da tabela de referência em conjunto com o código do grupo ou item assistencial no fornecimento direto de materiais e medicamentos. 
Chave para identificação de um item de fornecimento direto:
-Número do registro da operadora na ANS que realizou o enviou do arquivo;
-Número de identificação do fornecimento direto;
-Data do fornecimento;
-Código da tabela TUSS do procedimento;
-Código do procedimento ou grupo de procedimento, de acordo com o informado no arquivo"
0107-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: [TISS 1.04 MODIFICADA] Se o 'tipoRegistro' for igual a Alteração o 'grupoProcedimento' já deve existir na base de dados para o mesmo lançamento em alteração. 
0108-1801,"Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Quando não for código próprio da operadora ou pacote (indicados pelos códigos 00, 90 ou 98 no campo 106) deve ser um código existente na  TUSS"
0108-2601,Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO. - Crítica ANS: A forma de envio do item informado deve ser consolidado (informação na tabela TUSS 64)
0108-5053,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: [TISS 1.04 MODIFICADA] Não deve haver repetição da tabela de referência em conjunto com o código do item assistencial no fornecimento direto de materiais e medicamentos.
Chave para identificação de um item de fornecimento direto:
-Número do registro da operadora na ANS que realizou o enviou do arquivo;
-Número de identificação do fornecimento direto;
-Data do fornecimento;
-Código da tabela TUSS do procedimento;
-Código do procedimento ou grupo de procedimento, de acordo com o informado no arquivo"
0108-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: [TISS 1.04 MODIFICADA] Se o 'tipoRegistro' for igual a Alteração o 'codigoProcedimento' já deve existir na base de dados para o mesmo lançamento em alteração. 
0109-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: A quantidade de itens fornecidos deve ser maior que zero
0110-5040,Motivo Glosa: VALOR DEVE SER MAIOR QUE ZERO - Crítica ANS: O valor deve ser maior que zero
0111-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor da coparticipação deve ser maior ou igual a zero
0112-,Motivo Glosa:  - Crítica ANS: 
0113-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Deve ser menor que a data atual e seu ano e mês devem ser equivalentes ao ano e mês contidos no campo de competência dos dados (campo 003)
0113-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - Crítica ANS: O ano e mês da data de processamento informada no arquivo devem corresponder a uma competência vigente na base de dados de competências na data em que o arquivo foi recepcionado na base de dados do PTA.
0114-,Motivo Glosa:  - Crítica ANS: 
0115-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: O CPF/CNPJ deve ser existente na base da Receita Federal
0115-5055,"Motivo Glosa: IDENTIFICADOR JÁ INFORMADO NA COMPETÊNCIA - Crítica ANS: Acontece quando é enviado um lançamento de outras formas de remuneração que já conste na base de dados da ANS
Chave utilizada nesta pesquisa:
-Número do registro da operadora na ANS;
-Tipo de identificação do recebedor;
-Número do CPF/CNPJ do recebedor;
-Data do processamento da outra remuneração;"
0115-5056,"Motivo Glosa: IDENTIFICADOR NÃO INFORMADO NA COMPETÊNCIA - Crítica ANS: Acontece quando é enviado um lançamento de alteração ou exclusão de outras formas de remuneração que não conste na base de dados da ANS.
Chave utilizada nesta pesquisa:
-Número do registro da operadora na ANS;
-Tipo de identificação do recebedor;
-Número do CPF/CNPJ do recebedor;
-Data do processamento da outra remuneração;"
0116-5040,Motivo Glosa: VALOR DEVE SER MAIOR QUE ZERO - Crítica ANS: O valor total informado deve ser maior que zero
0117-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total da glosa deve  ser maior ou igual a zero
0118-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: O valor total pago deve  ser maior ou igual a zero
0119-1323,"Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: A competência da cobertura contratada, preenchida no formato AAAAMM, deve ser menor que o mês/ano da data atual e seu ano e mês deve ser equivalente ao ano e mês contido no campo competenciado dos dados (campo 003)"
0119-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - Crítica ANS: A competência não deve ser diferente das competências vigentes na base de dados de competências do TISS
0119-5045,Motivo Glosa: COMPETÊNCIA ANTERIOR NÃO ENVIADA - Crítica ANS: Acontece quando há uma lacuna (falta de informação) entre a competência enviada e a última incorporada ao banco de dados da ANS
0120-5061,"Motivo Glosa: TIPO DE ATENDIMENTO OPERADORA INTERMEDIÁRIA NÃO INFORMADO - Crítica ANS: Caso o primeiro lançamento da guia seja de competência posterior a dezembro de 2017 e o campo 081 que identifica a operadora intermediária tenha sido informado, o tipo de atendimento por operadora intermediária deve ser informado."
0121-1206,"Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] No lançamento de inclusão (do segundo em diante para a mesma guia) ou alteração caso o número do CPF seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
0121-5076,Motivo Glosa: CPF NÃO ENCONTRADO NA RECEITA FEDERAL - Crítica ANS: [TISS 1.04 NOVA] Deve ser de um CPF válido (ter 11 caracteres) e existir na base de dados da Receita Federal.
0122-5079,Motivo Glosa: MODELO DE REMUNERAÇÃO EM DUPLICIDADE. - Crítica ANS: [TISS 1.04 NOVA] No lançamento de Inclusão ou Alteração não deve haver repetição do mesmo código no campo modelo de remuneração.
0122-5080,"Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO INFORMADO - Crítica ANS: [TISS 1.04 NOVA] Deve ser preenchido quando a origem da guia for igual a 1 (Rede Contratada, referenciada ou credenciada), 2 (Rede Própria - Cooperados) ou 3 (Rede Própria - Demais prestadores)."
0122-5081,Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REEMBOLSO/PRESTADOR EVENTUAL - Crítica ANS: [TISS 1.04 NOVA] Não deve ser preenchido se a origem da guia foi 4 (Reembolso) ou 5 (Prestador eventual).
0122-5082,Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REDE PROPRIA COM MESMO CNPJ - Crítica ANS: [TISS 1.04 NOVA] Não deve ser preenchido nos atendimentos em rede própria de mesmo CNPJ.
0123-5074,"Motivo Glosa: REGIME DE ATENDIMENTO INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] Deve ser preenchido no lançamento de inclusão ou alteração, se o tipo de guia for igual a 1 (Consulta) ou 2 (SP/SADT); e a origem da guia for uma das seguintes opções: 1 (Rede Contratada, referenciada ou credenciada), 2 (Rede Própria - Cooperados) ou 3 (Rede Própria - Demais prestadores); e a versão enviada pelo prestador à operadora é igual ou maior a 023 (versão 4.00.00). 
Não deve ser preenchido se a versão enviada pelo prestador à operadora for menor que 023 ou tipo de guia for igual a 3 (Internação), 4 (Tratamento Odontológico) ou 5 (Honorários).
No lançamento de inclusão (do segundo em diante para a mesma guia) ou alteração, deve ser igual ao que já estava na base de dados para esta guia."
0124-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] Não deve ser preenchido quando a versão enviada pelo prestador à operadora for menor que 023 ou tipo de guia for igual a 3 (Internação), 4 (Tratamento Odontológico) ou 5 (Honorários).
Caso exista um ou mais lançamentos ativos na base de dados, a operadora tentar incluir um novo lançamento para a guia de atendimento ou tentar alterar um de seus lançamentos e o código de saúde ocupacional informado no lançamento for diferente do que consta no primeiro lançamento ativo da guia de atendimento, o lançamento será rejeitado."
0125-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] Caso exista um ou mais lançamentos ativos na base de dados, a operadora tentar incluir um novo lançamento para a guia de atendimento ou tentar alterar um de seus lançamentos e a Unidade de Medida informada no lançamento for diferente do que consta no primeiro lançamento ativo da guia de atendimento, o lançamento será rejeitado."
0125-5084,"Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: [TISS 1.04 NOVA] Deve ser preenchido quando o item fornecido possuir unidade de medida. Não se aplica às tabelas TUSS 22, 63, 90 e 98."
0125-5085,"Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de Inclusão ou Alteração quando o elemento ‘unidadeMedida’for informado no registro de um procedimento/item assistencial, deve ser de um tipo válido (Tabela 60 da TUSS - Terminologia de unidade de medida). É obrigatório para itens da TUSS de Medicamentos (Tabela 20)."
0126-5076,Motivo Glosa: CPF NÃO ENCONTRADO NA RECEITA FEDERAL - Crítica ANS: [TISS 1.04 NOVA] Deve ser de um CPF válido (ter 11 caracteres) e existir na base de dados da Receita Federal.
0127-5084,"Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: [TISS 1.04 NOVA] Deve ser preenchido quando o item fornecido possuir unidade de medida. Não se aplica à tabelas TUSS 22, 63, 90 e 98."
0127-5085,"Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de Inclusão ou Alteração quando o elemento ‘unidadeMedida’for informado no registro de um procedimento/item assistencial, deve ser de um tipo válido (Tabela 60 da TUSS - Terminologia de unidade de medida). É obrigatório para itens da TUSS de Medicamentos (Tabela 20)."
0128-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: [TISS 1.04 NOVA] Deve ser maior ou igual a zero.
0128-5050,"Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] No lançamento de Inclusão/Alteração, caso já exista no mínimo um lançamento ativo na base de dados para a mesma guia, o valor informado do modelo de remuneração deve ser igual ao já existente na base de dados nos outros lançamentos da mesma guia, para o mesmo modelo de remuneração."
0129-5029,"Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: [TISS 1.04 NOVA] Caso exista um ou mais lançamentos ativos na base de dados, a operadora tentar incluir um novo lançamento para a guia de atendimento ou tentar alterar um de seus lançamentos e a Unidade de Medida (no elemento ‘detalhePacote’) informada for diferente do que consta no primeiro lançamento ativo da guia de atendimento, o lançamento será rejeitado."
0129-5084,Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: [TISS 1.04 NOVA] Deve ser preenchido quando o item fornecido possuir unidade de medida. Não se aplica à tabela TUSS 22.
0129-5085,"Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: [TISS 1.04 NOVA] No lançamento de Inclusão ou Alteração quando o elemento ‘unidadeMedida’for informado no registro de um procedimento/item assistencial, deve ser de um tipo válido (Tabela 60 da TUSS - Terminologia de unidade de medida). É obrigatório para itens da TUSS de Medicamentos (Tabela 20)."

"""
    data_file = io.StringIO(csv_data)
    df = pd.read_csv(data_file)
    return df

# ================================================================================
# FUNÇÕES DE PARSE
# ================================================================================


@st.cache_data
def parse_xte(file):
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}
    all_data = []

    cabecalho_info = {}
    cabecalho = root.find('.//ans:cabecalho', namespaces=ns)
    if cabecalho is not None:
        identificacao = cabecalho.find(
            'ans:identificacaoTransacao', namespaces=ns)
        if identificacao is not None:
            cabecalho_info['tipoTransacao'] = identificacao.findtext(
                'ans:tipoTransacao', default='', namespaces=ns)
            cabecalho_info['numeroLote'] = identificacao.findtext(
                'ans:numeroLote', default='', namespaces=ns)
            cabecalho_info['competenciaLote'] = identificacao.findtext(
                'ans:competenciaLote', default='', namespaces=ns)
            cabecalho_info['dataRegistroTransacao'] = identificacao.findtext(
                'ans:dataRegistroTransacao', default='', namespaces=ns)
            cabecalho_info['horaRegistroTransacao'] = identificacao.findtext(
                'ans:horaRegistroTransacao', default='', namespaces=ns)
        cabecalho_info['registroANS'] = cabecalho.findtext(
            'ans:registroANS', default='', namespaces=ns)
        cabecalho_info['versaoPadrao'] = cabecalho.findtext(
            'ans:versaoPadrao', default='', namespaces=ns)

    for guia in root.findall(".//ans:guiaMonitoramento", namespaces=ns):
        guia_data = {}
        guia_data.update(cabecalho_info)

        for elem in guia.iter():
            tag_full = elem.tag.split('}')[-1]
            if 'data' in tag_full.lower() and elem.text:
                try:
                    date_obj = datetime.strptime(elem.text, '%Y-%m-%d')
                    guia_data[tag_full] = date_obj.strftime('%d/%m/%Y')
                except ValueError:
                    guia_data[tag_full] = elem.text
            else:
                guia_data[tag_full] = elem.text if elem.text else None

        procedimentos = guia.findall(".//ans:procedimentos", namespaces=ns)
        if procedimentos:
            for proc in procedimentos:
                proc_data = guia_data.copy()
                proc_data['codigoProcedimento'] = (proc.findtext(
                    'ans:identProcedimento/ans:Procedimento/ans:codigoProcedimento', namespaces=ns) or '').strip()
                proc_data['grupoProcedimento'] = (proc.findtext(
                    'ans:identProcedimento/ans:Procedimento/ans:grupoProcedimento', namespaces=ns) or '').strip()
                proc_data['valorInformado'] = (proc.findtext(
                    'ans:valorInformado', namespaces=ns) or '').strip()
                proc_data['valorPagoProc'] = (proc.findtext(
                    'ans:valorPagoProc', namespaces=ns) or '').strip()
                campos_procedimento = ['quantidadeInformada', 'quantidadePaga',
                                       'valorPagoFornecedor', 'valorCoParticipacao', 'unidadeMedida']
                for campo in campos_procedimento:
                    proc_data[campo] = (proc.findtext(
                        f'ans:{campo}', namespaces=ns) or '').strip()
                proc_data['codigoTabela'] = (proc.findtext(
                    'ans:identProcedimento/ans:codigoTabela', namespaces=ns) or '').strip()
                proc_data['registroANSOperadoraIntermediaria'] = (proc.findtext(
                    'ans:registroANSOperadoraIntermediaria', namespaces=ns) or '').strip()
                proc_data['tipoAtendimentoOperadoraIntermediaria'] = (proc.findtext(
                    'ans:tipoAtendimentoOperadoraIntermediaria', namespaces=ns) or '').strip()
                all_data.append(proc_data)
        else:
            all_data.append(guia_data)

    df = pd.DataFrame(all_data)
    df['Nome da Origem'] = file.name

    date_columns = [col for col in df.columns if 'data' in col.lower()]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(
                df[col], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        except Exception:
            pass

    if 'dataRealizacao' in df.columns and 'dataNascimento' in df.columns:
        def calcular_idade(row):
            try:
                data_realizacao = datetime.strptime(
                    row['dataRealizacao'], '%d/%m/%Y')
                data_nascimento = datetime.strptime(
                    row['dataNascimento'], '%d/%m/%Y')
                return (data_realizacao - data_nascimento).days // 365
            except Exception:
                return None
        df['Idade_na_Realização'] = df.apply(calcular_idade, axis=1)

    df.rename(columns={
        'valorInformado': 'valorInformado_proc',
        'valorPagoFornecedor': 'valorPagoFornecedor_proc',
        'dataRegistroTransacao': 'dataRegistroTransacao_cabecalho',
        'horaRegistroTransacao': 'horaRegistroTransacao_cabecalho',
        'registroANS': 'registroANS_cabecalho',
        'versaoPadrao': 'versaoPadrao_cabecalho'
    }, inplace=True)

    colunas_finais = [
        'Nome da Origem', 'tipoRegistro', 'versaoTISSPrestador', 'formaEnvio', 'tipoTransacao',
        'numeroLote', 'competenciaLote', 'dataRegistroTransacao_cabecalho', 'horaRegistroTransacao_cabecalho',
        'registroANS_cabecalho', 'versaoPadrao_cabecalho', 'CNES', 'identificadorExecutante',
        'codigoCNPJ_CPF', 'municipioExecutante', 'registroANSOperadoraIntermediaria',
        'tipoAtendimentoOperadoraIntermediaria', 'numeroCartaoNacionalSaude', 'cpfBeneficiario',
        'sexo', 'dataNascimento', 'municipioResidencia', 'numeroRegistroPlano',
        'tipoEventoAtencao', 'origemEventoAtencao', 'numeroGuia_prestador', 'numeroGuia_operadora',
        'identificacaoReembolso', 'formaRemuneracao', 'valorRemuneracao', 'guiaSolicitacaoInternacao',
        'dataSolicitacao', 'numeroGuiaSPSADTPrincipal', 'dataAutorizacao', 'dataRealizacao',
        'dataFimPeriodo', 'dataInicialFaturamento', 'dataProtocoloCobranca', 'dataPagamento', 'dataProcessamentoGuia',
        'tipoConsulta', 'cboExecutante', 'indicacaoRecemNato', 'indicacaoAcidente',
        'caraterAtendimento', 'tipoInternacao', 'regimeInternacao', 'tipoAtendimento',
        'regimeAtendimento', 'tipoFaturamento', 'diariasAcompanhante', 'diariasUTI', 'motivoSaida',
        'valorTotalInformado', 'valorProcessado', 'valorTotalPagoProcedimentos', 'valorTotalDiarias',
        'valorTotalTaxas', 'valorTotalMateriais', 'valorTotalOPME', 'valorTotalMedicamentos',
        'valorGlosaGuia', 'valorPagoGuia', 'valorPagoFornecedores', 'valorTotalTabelaPropria',
        'valorTotalCoParticipacao', 'declaracaoNascido', 'declaracaoObito', 'codigoTabela',
        'grupoProcedimento', 'codigoProcedimento', 'quantidadeInformada', 'valorInformado', 'valorInformado_proc',
        'valorPagoFornecedor', 'quantidadePaga', 'unidadeMedida', 'valorCoParticipacao', 'valorPagoProc', 'valorPagoFornecedor_proc',
        'Idade_na_Realização', 'diagnosticoCID'
    ]

    for col in colunas_finais:
        if col not in df.columns:
            df[col] = None

    df = df[colunas_finais]
    return df, content, tree


@st.cache_data
def parse_xtr(file):
    """
    Analisa um arquivo .XTR. Para erros de item, agora extrai o código do 
    procedimento associado para permitir um cruzamento preciso. Para erros de guia,
    os campos de procedimento ficam vazios.
    """
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}
    all_errors = []

    caminho_das_guias = './/ans:resumoProcessamento/ans:registrosRejeitados/ans:guiaMonitoramento'

    for guia_rejeitada in root.findall(caminho_das_guias, namespaces=ns):
        guia_info_base = {
            'nomeArquivo_xtr': file.name,
            'numeroGuiaOperadora_xtr': guia_rejeitada.findtext('ans:numeroGuiaOperadora', default='', namespaces=ns).strip()
        }

        # 1. Processa erros de GUIA
        # Estes erros não têm um procedimento específico associado.
        erros_guia_nodes = guia_rejeitada.findall(
            'ans:errosGuia', namespaces=ns)
        for erro_node in erros_guia_nodes:
            codigo_erro = erro_node.findtext(
                'ans:codigoErro', default='', namespaces=ns).strip()
            id_campo = erro_node.findtext(
                'ans:identificadorCampo', default='', namespaces=ns).strip()
            if codigo_erro and id_campo:
                linha = guia_info_base.copy()
                linha['tipoErro'] = 'Guia'
                linha['codigoErroUnico'] = f"Guia_{id_campo}-{codigo_erro}"
                linha['proc_codigoTabela_xtr'] = None
                linha['proc_codigoProcedimento_xtr'] = None
                all_errors.append(linha)

        # 2. Processa erros de ITEM
        # Estes erros estão ligados a um procedimento específico.
        erros_itens_nodes = guia_rejeitada.findall(
            'ans:errosItensGuia', namespaces=ns)
        for item_erro_block in erros_itens_nodes:
            # Extrai a identificação do procedimento para este bloco de erros
            ident_proc = item_erro_block.find('ans:identProcedimento', ns)
            codigo_tabela = ''
            codigo_procedimento = ''
            if ident_proc is not None:
                codigo_tabela = ident_proc.findtext(
                    'ans:codigoTabela', default='', namespaces=ns)
                proc_node = ident_proc.find('ans:Procedimento', ns)
                if proc_node is not None:
                    codigo_procedimento = proc_node.findtext(
                        'ans:codigoProcedimento', default='', namespaces=ns)

            # Itera sobre os erros deste procedimento
            for relacao_erro in item_erro_block.findall('ans:relacaoErros', ns):
                codigo_erro = relacao_erro.findtext(
                    'ans:codigoErro', default='', namespaces=ns).strip()
                id_campo = relacao_erro.findtext(
                    'ans:identificadorCampo', default='', namespaces=ns).strip()
                if codigo_erro and id_campo:
                    linha = guia_info_base.copy()
                    linha['tipoErro'] = 'Item'
                    linha['codigoErroUnico'] = f"Item_{id_campo}-{codigo_erro}"
                    linha['proc_codigoTabela_xtr'] = codigo_tabela
                    linha['proc_codigoProcedimento_xtr'] = codigo_procedimento
                    all_errors.append(linha)

    df_errors = pd.DataFrame(all_errors)
    return df_errors


@st.cache_data
def parse_xtr_para_relatorio_wide(file):
    """
    Analisa um arquivo .XTR.
    VERSÃO CORRIGIDA: Se uma guia tiver erros de guia E erros de procedimento,
    cria uma linha SEPARADA para os erros de guia e, em seguida, linhas
    separadas para cada erro de procedimento.
    """
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}

    all_rows_data = []
    caminho_das_guias = './/ans:resumoProcessamento/ans:registrosRejeitados/ans:guiaMonitoramento'

    for guia_rejeitada in root.findall(caminho_das_guias, namespaces=ns):
        contratado = guia_rejeitada.find('ans:contratadoExecutante', ns)

        # 1. Coleta os dados da guia que serão comuns a todas as linhas
        dados_comuns_guia = {
            'Nome da Origem': file.name,
            'tipoRegistroANS': guia_rejeitada.findtext('ans:tipoRegistro', default='', namespaces=ns),
            'registroANS': root.findtext('.//ans:registroANS', default='', namespaces=ns),
            'CNES': contratado.findtext('ans:CNES', default='', namespaces=ns) if contratado is not None else '',
            'CNPJ_CPF': contratado.findtext('ans:codigoCNPJ_CPF', default='', namespaces=ns) if contratado is not None else '',
            'numeroGuiaPrestador': guia_rejeitada.findtext('ans:numeroGuiaPrestador', default='', namespaces=ns),
            'numeroGuiaOperadora': guia_rejeitada.findtext('ans:numeroGuiaOperadora', default='', namespaces=ns),
            'identificadorReembolso': guia_rejeitada.findtext('ans:identificadorReembolso', default='', namespaces=ns),
            'dataProcessamento': guia_rejeitada.findtext('ans:dataProcessamento', default='', namespaces=ns),
        }

        # 2. Captura os erros de guia e de item separadamente
        erros_de_guia_nodes = guia_rejeitada.findall('ans:errosGuia', ns)
        itens_com_erro_nodes = guia_rejeitada.findall('ans:errosItensGuia', ns)

        # 3. LÓGICA DE GERAÇÃO DE LINHAS SEPARADAS

        # Se houver erros no nível da GUIA, cria uma linha para eles
        if erros_de_guia_nodes:
            linha_erro_guia = dados_comuns_guia.copy()

            # Adiciona os erros de guia a esta linha
            lista_erros_guia = []
            for erro_guia in erros_de_guia_nodes:
                erro_info = {
                    'guia_identificadorCampo': erro_guia.findtext('ans:identificadorCampo', default='', namespaces=ns),
                    'guia_codigoErro': erro_guia.findtext('ans:codigoErro', default='', namespaces=ns)
                }
                lista_erros_guia.append(erro_info)

            linha_erro_guia['lista_erros_guia'] = lista_erros_guia

            # Garante que os campos de procedimento e seus erros estejam vazios nesta linha
            linha_erro_guia['lista_erros_proc'] = []
            linha_erro_guia['proc_codigoTabela'] = None
            linha_erro_guia['proc_codigoProcedimento'] = None

            all_rows_data.append(linha_erro_guia)

        # Se houver erros no nível do ITEM/PROCEDIMENTO, cria linhas para eles
        if itens_com_erro_nodes:
            for item_erro_block in itens_com_erro_nodes:
                linha_erro_item = dados_comuns_guia.copy()

                # Garante que a lista de erros de guia esteja vazia nesta linha
                linha_erro_item['lista_erros_guia'] = []

                # Extrai os dados específicos deste procedimento
                ident_proc = item_erro_block.find('ans:identProcedimento', ns)
                if ident_proc is not None:
                    linha_erro_item['proc_codigoTabela'] = ident_proc.findtext(
                        'ans:codigoTabela', default='', namespaces=ns)
                    linha_erro_item['proc_codigoProcedimento'] = ident_proc.findtext(
                        'ans:Procedimento/ans:codigoProcedimento', default='', namespaces=ns)

                # Extrai os erros específicos deste procedimento
                lista_erros_proc = []
                todas_relacoes_erro = item_erro_block.findall(
                    'ans:relacaoErros', ns)
                for relacao_erro in todas_relacoes_erro:
                    erro_info = {
                        'proc_identificadorCampo': relacao_erro.findtext('ans:identificadorCampo', default='', namespaces=ns),
                        'proc_codigoErro': relacao_erro.findtext('ans:codigoErro', default='', namespaces=ns)
                    }
                    lista_erros_proc.append(erro_info)

                linha_erro_item['lista_erros_proc'] = lista_erros_proc
                all_rows_data.append(linha_erro_item)

    return pd.DataFrame(all_rows_data)


def converter_xtr_para_xlsx(uploaded_xtr_files):
    """
    Converte arquivos XTR em uma planilha Excel.
    VERSÃO FINAL: Expande múltiplos erros de GUIA (amarelo) e múltiplos erros
    de PROCEDIMENTO (azul) em colunas dinâmicas.
    """
    if not uploaded_xtr_files:
        return None, None

    df_list = [parse_xtr_para_relatorio_wide(f) for f in uploaded_xtr_files]
    df_raw = pd.concat(df_list, ignore_index=True)

    if df_raw.empty:
        return None, None

    # --- LÓGICA DE EXPANSÃO DOS ERROS DE GUIA E PROCEDIMENTO ---
    df_final = df_raw.copy()

    # Expande erros de GUIA
    erros_guia_expandidos = df_final['lista_erros_guia'].apply(pd.Series)
    max_erros_guia = len(erros_guia_expandidos.columns)
    for i in range(max_erros_guia):
        col_erros = erros_guia_expandidos[i].apply(pd.Series)
        df_final[f'identificadorCampo_guia.{i+1}'] = col_erros.get(
            'guia_identificadorCampo', '')
        df_final[f'codigoErro_guia.{i+1}'] = col_erros.get(
            'guia_codigoErro', '')

    # Expande erros de PROCEDIMENTO
    erros_proc_expandidos = df_final['lista_erros_proc'].apply(pd.Series)
    max_erros_proc = len(erros_proc_expandidos.columns)
    for i in range(max_erros_proc):
        col_erros = erros_proc_expandidos[i].apply(pd.Series)
        df_final[f'identificadorCampo_proc.{i+1}'] = col_erros.get(
            'proc_identificadorCampo', '')
        df_final[f'codigoErro_proc.{i+1}'] = col_erros.get(
            'proc_codigoErro', '')

    # Limpa colunas de lista intermediárias
    df_final = df_final.drop(columns=['lista_erros_guia', 'lista_erros_proc'])

    # --- MONTAGEM DA ORDEM FINAL DAS COLUNAS ---
    colunas_base = [
        'Nome da Origem', 'tipoRegistroANS', 'registroANS', 'CNES', 'CNPJ_CPF',
        'numeroGuiaPrestador', 'numeroGuiaOperadora', 'identificadorReembolso', 'dataProcessamento'
    ]

    colunas_erros_guia = []
    for i in range(1, max_erros_guia + 1):
        colunas_erros_guia.append(f'identificadorCampo_guia.{i}')
        colunas_erros_guia.append(f'codigoErro_guia.{i}')

    colunas_proc_base = ['proc_codigoTabela', 'proc_codigoProcedimento']

    colunas_erros_proc = []
    for i in range(1, max_erros_proc + 1):
        colunas_erros_proc.append(f'identificadorCampo_proc.{i}')
        colunas_erros_proc.append(f'codigoErro_proc.{i}')

    final_column_order = colunas_base + colunas_erros_guia + \
        colunas_proc_base + colunas_erros_proc

    for col in final_column_order:
        if col not in df_final.columns:
            df_final[col] = ''
    df_final = df_final[final_column_order]
    df_final.fillna('', inplace=True)

    # --- GERAÇÃO DO EXCEL FORMATADO ---
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df_final.to_excel(writer, sheet_name='Modelo XTR',
                          index=False, header=False, startrow=3)

        workbook = writer.book
        worksheet = writer.sheets['Modelo XTR']

        # Definição de Formatos
        main_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
        error_guide_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'fg_color': '#FFFF00'})
        error_proc_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'fg_color': '#DAEEF3'})
        sub_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        yellow_data_format = workbook.add_format(
            {'border': 1, 'valign': 'top', 'fg_color': '#FFFF00', 'text_wrap': True})
        blue_data_format = workbook.add_format(
            {'border': 1, 'valign': 'top', 'fg_color': '#DAEEF3', 'text_wrap': True})
        default_data_format = workbook.add_format(
            {'border': 1, 'valign': 'top', 'text_wrap': True})

        # --- LÓGICA DE CABEÇALHOS DINÂMICOS ---
        for i in range(len(colunas_base)):
            worksheet.merge_range(
                1, i, 2, i, colunas_base[i], main_header_format)

        start_col_guia_err = len(colunas_base)
        end_col_guia_err = start_col_guia_err + len(colunas_erros_guia) - 1

        start_col_proc = end_col_guia_err + 1
        end_col_proc = start_col_proc + \
            len(colunas_proc_base) + len(colunas_erros_proc) - 1

        if max_erros_guia > 0:
            worksheet.merge_range(
                1, start_col_guia_err, 1, end_col_guia_err, 'ERRO GUIA', error_guide_header_format)
            col_idx = start_col_guia_err
            for i in range(1, max_erros_guia + 1):
                worksheet.write(
                    2, col_idx, f'identificadorCampo.{i}', sub_header_format)
                worksheet.write(
                    2, col_idx + 1, f'codigoErro.{i}', sub_header_format)
                col_idx += 2

        if max_erros_proc > 0 or df_final['proc_codigoTabela'].notna().any():
            worksheet.merge_range(
                1, start_col_proc, 1, end_col_proc, 'ERRO PROCEDIMENTO', error_proc_header_format)
            worksheet.write(2, start_col_proc,
                            'codigoTabela', sub_header_format)
            worksheet.write(2, start_col_proc + 1,
                            'codigoProcedimento', sub_header_format)
            col_idx = start_col_proc + 2
            for i in range(1, max_erros_proc + 1):
                worksheet.write(
                    2, col_idx, f'identificadorCampo.{i}', sub_header_format)
                worksheet.write(
                    2, col_idx + 1, f'codigoErro.{i}', sub_header_format)
                col_idx += 2

        # Formatação das Células de Dados
        for row_num in range(len(df_final)):
            for col_num in range(len(df_final.columns)):
                cell_value = df_final.iloc[row_num, col_num]
                if start_col_guia_err <= col_num <= end_col_guia_err:
                    worksheet.write(row_num + 3, col_num,
                                    cell_value, yellow_data_format)
                elif start_col_proc <= col_num <= end_col_proc:
                    worksheet.write(row_num + 3, col_num,
                                    cell_value, blue_data_format)
                else:
                    worksheet.write(row_num + 3, col_num,
                                    cell_value, default_data_format)

        worksheet.set_column('A:A', 30)
        worksheet.set_column(1, len(df_final.columns) - 1, 22)
        worksheet.freeze_panes(3, 0)

    return excel_buffer.getvalue(), len(df_final)


def converter_xtr_para_xlsx(uploaded_xtr_files):
    """
    Converte arquivos XTR em uma planilha Excel.
    VERSÃO FINAL CORRIGIDA: Expande múltiplos erros de GUIA (amarelo) e múltiplos erros
    de PROCEDIMENTO (azul) em colunas dinâmicas.
    """
    if not uploaded_xtr_files:
        return None, None

    df_list = [parse_xtr_para_relatorio_wide(f) for f in uploaded_xtr_files]
    df_raw = pd.concat(df_list, ignore_index=True)

    if df_raw.empty:
        return None, None

    df_final = df_raw.copy()

    # --- LÓGICA DE EXPANSÃO CORRIGIDA ---

    # Expande erros de GUIA de forma robusta
    max_erros_guia = 0
    if 'lista_erros_guia' in df_final.columns:
        rows_guia_errors = []
        # Itera sobre a coluna que contém as listas de erros
        for error_list in df_final['lista_erros_guia']:
            flat_errors = {}
            if isinstance(error_list, list):
                # Atualiza o número máximo de erros encontrados
                if len(error_list) > max_erros_guia:
                    max_erros_guia = len(error_list)
                # Cria um dicionário com as colunas numeradas
                for i, error_dict in enumerate(error_list):
                    flat_errors[f'identificadorCampo_guia.{i+1}'] = error_dict.get(
                        'guia_identificadorCampo', '')
                    flat_errors[f'codigoErro_guia.{i+1}'] = error_dict.get(
                        'guia_codigoErro', '')
            rows_guia_errors.append(flat_errors)

        if rows_guia_errors:
            # Cria um DataFrame com os erros expandidos e junta ao principal
            df_guia_errors_flat = pd.DataFrame(
                rows_guia_errors, index=df_final.index)
            df_final = pd.concat([df_final, df_guia_errors_flat], axis=1)

    # Expande erros de PROCEDIMENTO de forma robusta
    max_erros_proc = 0
    if 'lista_erros_proc' in df_final.columns:
        rows_proc_errors = []
        for error_list in df_final['lista_erros_proc']:
            flat_errors = {}
            if isinstance(error_list, list):
                if len(error_list) > max_erros_proc:
                    max_erros_proc = len(error_list)
                for i, error_dict in enumerate(error_list):
                    flat_errors[f'identificadorCampo_proc.{i+1}'] = error_dict.get(
                        'proc_identificadorCampo', '')
                    flat_errors[f'codigoErro_proc.{i+1}'] = error_dict.get(
                        'proc_codigoErro', '')
            rows_proc_errors.append(flat_errors)

        if rows_proc_errors:
            df_proc_errors_flat = pd.DataFrame(
                rows_proc_errors, index=df_final.index)
            df_final = pd.concat([df_final, df_proc_errors_flat], axis=1)

    # Limpa colunas de lista intermediárias
    if 'lista_erros_guia' in df_final.columns:
        df_final = df_final.drop(columns=['lista_erros_guia'])
    if 'lista_erros_proc' in df_final.columns:
        df_final = df_final.drop(columns=['lista_erros_proc'])

    # --- MONTAGEM DA ORDEM FINAL DAS COLUNAS ---
    colunas_base = [
        'Nome da Origem', 'tipoRegistroANS', 'registroANS', 'CNES', 'CNPJ_CPF',
        'numeroGuiaPrestador', 'numeroGuiaOperadora', 'identificadorReembolso', 'dataProcessamento'
    ]

    colunas_erros_guia = []
    for i in range(1, max_erros_guia + 1):
        colunas_erros_guia.append(f'identificadorCampo_guia.{i}')
        colunas_erros_guia.append(f'codigoErro_guia.{i}')

    colunas_proc_base = ['proc_codigoTabela', 'proc_codigoProcedimento']

    colunas_erros_proc = []
    for i in range(1, max_erros_proc + 1):
        colunas_erros_proc.append(f'identificadorCampo_proc.{i}')
        colunas_erros_proc.append(f'codigoErro_proc.{i}')

    final_column_order = colunas_base + colunas_erros_guia + \
        colunas_proc_base + colunas_erros_proc

    for col in final_column_order:
        if col not in df_final.columns:
            df_final[col] = ''
    df_final = df_final[final_column_order]
    df_final.fillna('', inplace=True)

    # --- GERAÇÃO DO EXCEL FORMATADO ---
    # (O restante do código para gerar o Excel com xlsxwriter permanece o mesmo da versão anterior)
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df_final.to_excel(writer, sheet_name='Modelo XTR',
                          index=False, header=False, startrow=3)

        workbook = writer.book
        worksheet = writer.sheets['Modelo XTR']

        main_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
        error_guide_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'fg_color': '#FFFF00'})
        error_proc_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'fg_color': '#DAEEF3'})
        sub_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        yellow_data_format = workbook.add_format(
            {'border': 1, 'valign': 'top', 'fg_color': '#FFFF00', 'text_wrap': True})
        blue_data_format = workbook.add_format(
            {'border': 1, 'valign': 'top', 'fg_color': '#DAEEF3', 'text_wrap': True})
        default_data_format = workbook.add_format(
            {'border': 1, 'valign': 'top', 'text_wrap': True})

        for i in range(len(colunas_base)):
            worksheet.merge_range(
                1, i, 2, i, colunas_base[i], main_header_format)

        start_col_guia_err = len(colunas_base)
        end_col_guia_err = start_col_guia_err + len(colunas_erros_guia) - 1

        start_col_proc = end_col_guia_err + 1
        end_col_proc = start_col_proc + \
            len(colunas_proc_base) + len(colunas_erros_proc) - 1

        if max_erros_guia > 0:
            worksheet.merge_range(
                1, start_col_guia_err, 1, end_col_guia_err, 'ERRO GUIA', error_guide_header_format)
            col_idx = start_col_guia_err
            for i in range(1, max_erros_guia + 1):
                worksheet.write(
                    2, col_idx, f'identificadorCampo.{i}', sub_header_format)
                worksheet.write(
                    2, col_idx + 1, f'codigoErro.{i}', sub_header_format)
                col_idx += 2

        if max_erros_proc > 0 or df_final['proc_codigoTabela'].notna().any():
            worksheet.merge_range(
                1, start_col_proc, 1, end_col_proc, 'ERRO PROCEDIMENTO', error_proc_header_format)
            worksheet.write(2, start_col_proc,
                            'codigoTabela', sub_header_format)
            worksheet.write(2, start_col_proc + 1,
                            'codigoProcedimento', sub_header_format)
            col_idx = start_col_proc + 2
            for i in range(1, max_erros_proc + 1):
                worksheet.write(
                    2, col_idx, f'identificadorCampo.{i}', sub_header_format)
                worksheet.write(
                    2, col_idx + 1, f'codigoErro.{i}', sub_header_format)
                col_idx += 2

        for row_num in range(len(df_final)):
            for col_num in range(len(df_final.columns)):
                cell_value = df_final.iloc[row_num, col_num]
                if start_col_guia_err <= col_num <= end_col_guia_err:
                    worksheet.write(row_num + 3, col_num,
                                    cell_value, yellow_data_format)
                elif start_col_proc <= col_num <= end_col_proc and col_num >= start_col_proc:  # Added second condition for safety
                    worksheet.write(row_num + 3, col_num,
                                    cell_value, blue_data_format)
                else:
                    worksheet.write(row_num + 3, col_num,
                                    cell_value, default_data_format)

        worksheet.set_column('A:A', 30)
        worksheet.set_column(1, len(df_final.columns) - 1, 22)
        worksheet.freeze_panes(3, 0)

    return excel_buffer.getvalue(), len(df_final)


# ================================================================================
# INTERFACE GRÁFICA (STREAMLIT)
# ================================================================================

st.set_page_config(page_title="Analisador TISS", layout="wide")

st.markdown("""
    <style>
        section[data-testid="stSidebar"] .css-ng1t4o {
            background-color: #1e1e1e;
            color: white;
            font-weight: bold;
            font-size: 1.1rem;
        }
        section[data-testid="stSidebar"] label {
            color: white !important;
        }
    </style>
""", unsafe_allow_html=True)

st.sidebar.title("AM Consultoria | Ferramenta TISS")
st.sidebar.markdown("---")
st.sidebar.info("Versão de Demonstração")

st.title("Analisador de Erros TISS")

# -------------------------------------------------------------------------------
# PÁGINA ÚNICA: ANÁLISE DE ERROS
# -------------------------------------------------------------------------------
st.subheader("🔍 Análise de Erros do Retorno da ANS (XTR)")
st.markdown("""
Esta ferramenta cruza os dados de um arquivo de retorno (`.XTR`) com os dados originais (`.XTE`) para gerar um relatório de análise focado **apenas nas guias com erro**. A lista de erros da ANS já está integrada.

**Siga os passos:**
1.  Faça o upload do(s) arquivo(s) `.XTE` que você enviou. 
2.  Faça o upload do(s) arquivo(s) de retorno `.XTR` correspondente(s).
3.  Clique em "Analisar Erros" para gerar o relatório consolidado.
""")

col1, col2 = st.columns(2)
with col1:
    uploaded_xte_files = st.file_uploader(
        "1. Arquivos de Envio (.xte)", accept_multiple_files=True, type=["xte"])
with col2:
    uploaded_xtr_files = st.file_uploader(
        "2. Arquivos de Retorno (.xtr)", accept_multiple_files=True, type=["xtr"])

if st.button("Analisar Erros", type="primary"):
    if not uploaded_xte_files or not uploaded_xtr_files:
        st.warning(
            "Por favor, faça o upload dos arquivos XTE e XTR para a análise.")
    else:
        try:
            progress_bar = st.progress(0)
            st.markdown("---")

            # --- ESTRUTURA DE LOGS PERSISTENTES E DOWNLOADS ---
            log_container = st.container()
            log_leitura_xte = log_container.empty()
            download_xte_placeholder = log_container.empty()
            log_leitura_xtr = log_container.empty()
            download_xtr_placeholder = log_container.empty()
            status_placeholder = log_container.empty()

            # Passo 1: Consolidar XTE
            log_leitura_xte.info(
                "Passo 1/7: Consolidando arquivos de envio (.xte)... ⏳")
            df_xte_list = [parse_xte(f)[0] for f in uploaded_xte_files]
            df_xte_full = pd.concat(df_xte_list, ignore_index=True)
            progress_bar.progress(14)
            log_leitura_xte.success(
                f"Passo 1/7: Arquivos XTE consolidados! ✅ ({len(df_xte_full)} registros)")

            excel_buffer_xte = io.BytesIO()
            df_xte_full.to_excel(
                excel_buffer_xte, index=False, engine='xlsxwriter')
            download_xte_placeholder.download_button(
                label="⬇ Baixar Planilha Consolidada XTE",
                data=excel_buffer_xte.getvalue(),
                file_name="dados_consolidados_xte.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            time.sleep(1)

            # Passo 2: Consolidar XTR e Gerar Planilha Formatada
            log_leitura_xtr.info(
                "Passo 2/7: Consolidando e formatando arquivos de retorno (.xtr)... ⏳")

            excel_data_xtr, total_erros = converter_xtr_para_xlsx(
                uploaded_xtr_files)

            progress_bar.progress(28)
            log_leitura_xtr.success(
                f"Passo 2/7: Arquivos XTR consolidados e formatados! ✅ ({total_erros} erros reportados)")

            if excel_data_xtr is not None:
                timestamp = datetime.now(pytz.timezone(
                    'America/Sao_Paulo')).strftime("%Y%m%d_%H%M%S")
                filename_xtr = f"erros_XTR_formatado_{timestamp}.xlsx"

                download_xtr_placeholder.download_button(
                    label="⬇ Baixar Planilha de Erros XTR Formatada",
                    data=excel_data_xtr,
                    file_name=filename_xtr,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                download_xtr_placeholder.empty()
            time.sleep(1)

            # É necessário recarregar os dados do XTR para a proxima etapa
            df_xtr_list = [parse_xtr(f) for f in uploaded_xtr_files]
            df_xtr_full = pd.concat(df_xtr_list, ignore_index=True)

            # Passo 3: Carregar Padrão ANS
            status_placeholder.info(
                "Passo 3/7: Carregando padrão de erros da ANS... ⏳")
            df_ans_errors = carregar_erros_ans()
            df_ans_errors.rename(
                columns={'Código do Termo': 'codigoErroUnico', 'Termo': 'descricaoErro'}, inplace=True)
            progress_bar.progress(42)
            status_placeholder.success(
                "Passo 3/7: Padrão de erros carregado! ✅")
            time.sleep(1)

            # Passo 4: Preparar Nomes Base dos Arquivos
            status_placeholder.info(
                "Passo 4/7: Preparando nomes base dos arquivos para cruzamento... ⏳")
            df_xte_full['nomeArquivo_base'] = df_xte_full['Nome da Origem'].apply(
                lambda x: os.path.splitext(x)[0])
            if not df_xtr_full.empty:
                df_xtr_full['nomeArquivo_base'] = df_xtr_full['nomeArquivo_xtr'].apply(
                    lambda x: os.path.splitext(x)[0])
            progress_bar.progress(56)
            status_placeholder.success(
                "Passo 4/7: Nomes base preparados! ✅")
            time.sleep(1)

            # Passo 5: Separar e Agrupar Erros por Tipo (Guia vs. Item)
            status_placeholder.info(
                "Passo 5/7: Separando e agrupando erros por tipo... ⏳")

            if not df_xtr_full.empty:
                df_xtr_full['proc_codigoProcedimento_xtr'] = df_xtr_full['proc_codigoProcedimento_xtr'].astype(
                    str).fillna('')
                df_xte_full['codigoProcedimento'] = df_xte_full['codigoProcedimento'].astype(
                    str).fillna('')

                df_xte_full['chave_guia'] = df_xte_full['nomeArquivo_base'].astype(
                    str) + "_" + df_xte_full['numeroGuia_operadora'].astype(str)
                df_xte_full['chave_procedimento'] = df_xte_full['chave_guia'] + \
                    "_" + df_xte_full['codigoProcedimento']

                df_xtr_full['chave_guia'] = df_xtr_full['nomeArquivo_base'].astype(
                    str) + "_" + df_xtr_full['numeroGuiaOperadora_xtr'].astype(str)
                df_xtr_full['chave_procedimento'] = df_xtr_full['chave_guia'] + \
                    "_" + df_xtr_full['proc_codigoProcedimento_xtr']

                erros_guia = df_xtr_full[df_xtr_full['tipoErro'] == 'Guia']
                erros_item = df_xtr_full[df_xtr_full['tipoErro'] == 'Item']

                guia_erros_agrupados = erros_guia.groupby('chave_guia')['codigoErroUnico'].unique(
                ).apply(list).reset_index().rename(columns={'codigoErroUnico': 'lista_erros_guia'})
                item_erros_agrupados = erros_item.groupby('chave_procedimento')['codigoErroUnico'].unique(
                ).apply(list).reset_index().rename(columns={'codigoErroUnico': 'lista_erros_item'})
            else:  # Caso não haja erros, cria dataframes vazios para evitar erros posteriores
                guia_erros_agrupados = pd.DataFrame(
                    columns=['chave_guia', 'lista_erros_guia'])
                item_erros_agrupados = pd.DataFrame(
                    columns=['chave_procedimento', 'lista_erros_item'])

            progress_bar.progress(70)
            status_placeholder.success(
                "Passo 5/7: Erros agrupados com sucesso! ✅")
            time.sleep(1)

            # Passo 6: Juntar Dados em Duas Etapas
            status_placeholder.info(
                "Passo 6/7: Cruzando dados originais com erros de item e guia... ⏳")

            df_analise = pd.merge(
                df_xte_full, item_erros_agrupados, on='chave_procedimento', how='left')
            df_analise = pd.merge(
                df_analise, guia_erros_agrupados, on='chave_guia', how='left')

            df_analise = df_analise[df_analise['lista_erros_item'].notna(
            ) | df_analise['lista_erros_guia'].notna()].copy()

            # --- LINHA CORRIGIDA ---
            # Função auxiliar para combinar listas de forma segura, tratando valores NaN
            def safe_combine_lists(row):
                guia_errors = row['lista_erros_guia'] if isinstance(
                    row['lista_erros_guia'], list) else []
                item_errors = row['lista_erros_item'] if isinstance(
                    row['lista_erros_item'], list) else []
                return guia_errors + item_errors

            df_analise['lista_erros'] = df_analise.apply(
                safe_combine_lists, axis=1)
            # --- FIM DA CORREÇÃO ---

            progress_bar.progress(85)
            status_placeholder.success(
                "Passo 6/7: Guias com erro identificadas e cruzadas! ✅")
            time.sleep(1)

            # Passo 7: Montar Relatório Final com Colunas de Erro
            status_placeholder.info(
                "Passo 7/7: Montando o relatório final com colunas de erro... ⏳")

            error_map = pd.Series(
                df_ans_errors.descricaoErro.values, index=df_ans_errors.codigoErroUnico).to_dict()
            df_final = df_analise.copy()

            if not df_final.empty and 'lista_erros' in df_final.columns:
                unique_errors_in_batch = sorted(list(set(
                    e for lista in df_final['lista_erros'] if isinstance(lista, list) for e in lista)))

                for error_code_with_prefix in unique_errors_in_batch:
                    col_name = f"Erro_{error_code_with_prefix}"
                    original_error_code = error_code_with_prefix.split('_', 1)[
                        1]
                    description = error_map.get(
                        original_error_code, f"Descrição não encontrada para {original_error_code}")

                    df_final[col_name] = df_final['lista_erros'].apply(
                        lambda x: description if isinstance(x, list) and error_code_with_prefix in x else None)

            original_cols = [col for col in df_xte_full.columns if col not in [
                'chave_guia', 'chave_procedimento', 'nomeArquivo_base']]
            all_error_cols = [
                col for col in df_final.columns if col.startswith('Erro_')]
            guia_error_cols = sorted(
                [col for col in all_error_cols if col.startswith('Erro_Guia_')])
            item_error_cols = sorted(
                [col for col in all_error_cols if col.startswith('Erro_Item_')])
            ordered_error_cols = guia_error_cols + item_error_cols

            cols_to_drop = ['lista_erros_guia', 'lista_erros_item', 'lista_erros',
                            'chave_guia', 'chave_procedimento', 'nomeArquivo_base']
            df_final.drop(columns=[
                          col for col in cols_to_drop if col in df_final.columns], inplace=True, errors='ignore')

            final_order = original_cols + ordered_error_cols
            df_final = df_final[[
                col for col in final_order if col in df_final.columns]]

            progress_bar.progress(100)
            status_placeholder.success(
                "Passo 7/7: Relatório final organizado! ✅")
            time.sleep(1.5)

            log_container.empty()
            progress_bar.empty()

            if df_final.empty:
                st.error(
                    "Nenhuma correspondência de guia com erro encontrada. Verifique se os nomes dos arquivos XTE e XTR correspondem.")
            else:
                st.success(
                    f"🎉 Análise concluída! Foram encontradas {len(df_final)} linhas de procedimento em guias com erro.")
                st.markdown(
                    "Abaixo está uma **pré-visualização** do resultado:")
                st.dataframe(df_final)

                st.markdown("---")
                with st.spinner('Preparando sua planilha para download... ⏳'):
                    excel_buffer_final = io.BytesIO()
                    df_final.to_excel(excel_buffer_final,
                                      index=False, engine='xlsxwriter')
                    time.sleep(1)

                st.download_button(
                    label="⬇ Baixar Planilha de Análise Completa (.xlsx)",
                    data=excel_buffer_final.getvalue(),
                    file_name="analise_de_erros_TISS_wide.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

        except Exception as e:
            st.error(f"Ocorreu um erro durante a análise: {e}")
            st.error(
                "Verifique se os arquivos estão no formato correto e se correspondem.")

