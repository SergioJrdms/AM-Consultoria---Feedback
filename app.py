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
00-5001,"MENSAGEM ELETRÔNICA FORA DO PADRÃO TISS - O erro está no campo: Cabeçalho do Lote XTE"
00-5002,"NÃO FOI POSSÍVEL VALIDAR O ARQUIVO XML - O erro está no campo: Cabeçalho do Lote XTE"
00-5014,"CÓDIGO HASH INVÁLIDO. MENSAGEM PODE ESTAR CORROMPIDA. - O erro está no campo: Cabeçalho do Lote XTE"
00-5016,"SEM NENHUMA OCORRENCIA DE MOVIMENTO NA COMPETENCIA PARA ENVIO A ANS - O erro está no campo: Cabeçalho do Lote XTE"
00-5017,"ARQUIVO PROCESSADO PELA ANS - O erro está no campo: Cabeçalho do Lote XTE"
00-5023,"COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - O erro está no campo: Cabeçalho do Lote XTE"
00-5044,"JÁ EXISTEM INFORMAÇÕES NA ANS PARA A COMPETÊNCIA INFORMADA - O erro está no campo: Cabeçalho do Lote XTE"
00-5045,"COMPETÊNCIA ANTERIOR NÃO ENVIADA - O erro está no campo: Cabeçalho do Lote XTE"
00-5046,"COMPETÊNCIA INVÁLIDA - O erro está no campo: Cabeçalho do Lote XTE"
02-5029,"INDICADOR INVÁLIDO - O erro está no campo: Número do lote"
03-5024,"OPERADORA INATIVA NA COMPETÊNCIA DOS DADOS - O erro está no campo: Competência"
04-5025,"DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - O erro está no campo: Data de registro da transação"
05-5026,"HORA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - O erro está no campo: Hora de registro da transação"
06-5027,"REGISTRO ANS DA OPERADORA INVÁLIDO - O erro está no campo: Registro ANS"
010-5028,"VERSÃO DO PADRÃO INVÁLIDA - O erro está no campo: Padrão do Prestador"
012-1202,"NÚMERO DO CNES INVÁLIDO - O erro está no campo: CNES Executante"
012-5029,"INDICADOR INVÁLIDO - O erro está no campo: CNES Executante"
012-5063,"PAR CNPJ x CNES NAO ENCONTRADO NA BASE DO CNES - O erro está no campo: CNES Executante"
012-5064,"TIPO DE ESTABELECIMENTO NO CNES NÃO É APTO PARA INTERNAÇÃO - O erro está no campo: CNES Executante"
013-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tipo Docto - CNPJ ou CPF"
014-1206,"CPF / CNPJ INVÁLIDO - O erro está no campo: CNPJ/CPF Executante"
014-5029,"INDICADOR INVÁLIDO - O erro está no campo: CNPJ/CPF Executante"
014-5065,"TIPO DE ATIVIDADE ECONOMICA DO CNPJ NÃO É APTO PARA INTERNAÇÃO - O erro está no campo: CNPJ/CPF Executante"
015-5029,"INDICADOR INVÁLIDO - O erro está no campo: Município Executante"
015-5030,"CÓDIGO DO MUNÍCIPIO INVÁLIDO - O erro está no campo: Município Executante"
016-1002,"NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - O erro está no campo: CNS - Cartão Nacional de Saúde"
016-5029,"INDICADOR INVÁLIDO - O erro está no campo: CNS - Cartão Nacional de Saúde"
016-5086,"DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - O erro está no campo: CNS - Cartão Nacional de Saúde"
017-5029,"INDICADOR INVÁLIDO - O erro está no campo: Sexo do Beneficiário"
017-5077,"SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - O erro está no campo: Sexo do Beneficiário"
018-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Nascimento"
018-5078,"DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - O erro está no campo: Data Nascimento"
019-5029,"INDICADOR INVÁLIDO - O erro está no campo: Município do Beneficiário"
019-5030,"CÓDIGO DO MUNÍCIPIO INVÁLIDO - O erro está no campo: Município do Beneficiário"
020-1024,"PLANO NÃO EXISTENTE - O erro está no campo: Nr. Plano na ANS"
020-5029,"INDICADOR INVÁLIDO - O erro está no campo: Nr. Plano na ANS"
021-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tipo de Guia"
022-5029,"INDICADOR INVÁLIDO - O erro está no campo: Origem da guia"
023-1307,"NÚMERO DA GUIA INVÁLIDO - O erro está no campo: Nr. Guia no Prestador"
023-1308,"GUIA JÁ APRESENTADA - O erro está no campo: Nr. Guia no Prestador"
023-5029,"INDICADOR INVÁLIDO - O erro está no campo: Nr. Guia no Prestador"
023-5073,"O PRIMEIRO LANCAMENTO DA GUIA SÓ PODE SER EXCLUIDO SE ELE FOR O ÚNICO - O erro está no campo: Nr. Guia no Prestador"
024-1307,"NÚMERO DA GUIA INVÁLIDO - O erro está no campo: Nr. Guia na Operadora"
024-5029,"INDICADOR INVÁLIDO - O erro está no campo: Nr. Guia na Operadora"
025-1307,"NÚMERO DA GUIA INVÁLIDO - O erro está no campo: Nr. Reembolso ou Pagto Eventual"
025-5029,"INDICADOR INVÁLIDO - O erro está no campo: Nr. Reembolso ou Pagto Eventual"
026-1307,"NÚMERO DA GUIA INVÁLIDO - O erro está no campo: Nr. Guia de solicitação de Internação"
027-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Solicitação"
028-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Autorização"
029-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Realização ou Início do Atendimento"
030-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data de Início do Faturamento"
031-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Final de Atendimento ou do Faturamento"
032-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Protocolo Cobrança"
033-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Pagamento"
034-1603,"TIPO DE CONSULTA INVÁLIDO - O erro está no campo: Tipo de Consulta"
035-1213,"CBO (ESPECIALIDADE) INVÁLIDO - O erro está no campo: CBO do Executante"
035-5029,"INDICADOR INVÁLIDO - O erro está no campo: CBO do Executante"
036-5032,"ATENDIMENTO AO RECÉM-NATO - O erro está no campo: Atendimento ao recém-nato"
037-5029,"INDICADOR INVÁLIDO - O erro está no campo: Acidente ou Doença relacionada?"
038-5029,"INDICADOR INVÁLIDO - O erro está no campo: Caráter do Atendimento"
038-5031,"CARÁTER DE ATENDIMENTO INVÁLIDO - O erro está no campo: Caráter do Atendimento"
039-1506,"TIPO DE INTERNAÇÃO INVÁLIDO - O erro está no campo: Tipo de Internação"
039-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tipo de Internação"
040-5029,"INDICADOR INVÁLIDO - O erro está no campo: Regime de Internação"
041-1509,"CÓDIGO CID INVÁLIDO - O erro está no campo: Diagnóstico Principal"
041-5029,"INDICADOR INVÁLIDO - O erro está no campo: Diagnóstico Principal"
041-5067,"DIAGNÓSTICO EM DUPLICIDADE - O erro está no campo: Diagnóstico Principal"
041-5075,"CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Diagnóstico Principal"
042-1509,"CÓDIGO CID INVÁLIDO - O erro está no campo: Diagnóstico Secundário"
042-5029,"INDICADOR INVÁLIDO - O erro está no campo: Diagnóstico Secundário"
042-5067,"DIAGNÓSTICO EM DUPLICIDADE - O erro está no campo: Diagnóstico Secundário"
042-5075,"CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Diagnóstico Secundário"
043-1509,"CÓDIGO CID INVÁLIDO - O erro está no campo: Terceiro Diagnóstico"
043-5029,"INDICADOR INVÁLIDO - O erro está no campo: Terceiro Diagnóstico"
043-5067,"DIAGNÓSTICO EM DUPLICIDADE - O erro está no campo: Terceiro Diagnóstico"
043-5075,"CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Terceiro Diagnóstico"
044-1509,"CÓDIGO CID INVÁLIDO - O erro está no campo: Quarto Diagnóstico"
044-5029,"INDICADOR INVÁLIDO - O erro está no campo: Quarto Diagnóstico"
044-5067,"DIAGNÓSTICO EM DUPLICIDADE - O erro está no campo: Quarto Diagnóstico"
044-5075,"CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Quarto Diagnóstico"
045-1602,"TIPO DE ATENDIMENTO INVÁLIDO OU NÃO INFORMADO - O erro está no campo: Tipo de Atendimento"
045-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tipo de Atendimento"
046-1713,"FATURAMENTO INVÁLIDO - O erro está no campo: Tipo de Faturamento"
046-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tipo de Faturamento"
047-1304,"COBRANÇA EM GUIA INDEVIDA - O erro está no campo: Qtde. Diárias Acompanhante"
048-1304,"COBRANÇA EM GUIA INDEVIDA - O erro está no campo: Qtde Diárias de UTI"
049-5033,"MOTIVO DE ENCERRAMENTO INVÁLIDO - O erro está no campo: Motivo Encerramento"
050-1705,"VALOR APRESENTADO A MAIOR - O erro está no campo: Valor informado da Guia"
050-1706,"VALOR APRESENTADO A MENOR - O erro está no campo: Valor informado da Guia"
050-5042,"VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - O erro está no campo: Valor informado da Guia"
050-5083,"SOMA DOS VALORES DOS MODELOS DE REMUNERAÇÃO DIFERENTE DO VALOR INFORMADO DA GUIA - O erro está no campo: Valor informado da Guia"
051-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor processado da Guia"
052-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total pago Procedimentos"
052-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total pago Procedimentos"
053-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total pago Diárias"
053-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total pago Diárias"
054-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total pago Taxas e Aluguéis"
054-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total pago Taxas e Aluguéis"
055-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total Pago Materiais"
055-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago Materiais"
056-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total Pago OPME"
056-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago OPME"
057-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total Pago Medicamentos"
057-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago Medicamentos"
058-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Glosa"
059-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total Pago"
059-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago"
059-5042,"VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - O erro está no campo: Valor total Pago"
060-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total Pago aos Fornecedores"
060-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago aos Fornecedores"
060-5042,"VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - O erro está no campo: Valor total Pago aos Fornecedores"
061-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor total Pago em Tabela Própria"
061-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago em Tabela Própria"
062-5029,"INDICADOR INVÁLIDO - O erro está no campo: Declaração de Nascido Vivo"
062-5066,"NÚMERO DA DECLARAÇÃO EM DUPLICIDADE. - O erro está no campo: Declaração de Nascido Vivo"
062-5068,"DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Declaração de Nascido Vivo"
063-5029,"INDICADOR INVÁLIDO - O erro está no campo: Declaração de Óbito"
063-5034,"VALOR NÃO INFORMADO - O erro está no campo: Declaração de Óbito"
063-5066,"NÚMERO DA DECLARAÇÃO EM DUPLICIDADE. - O erro está no campo: Declaração de Óbito"
063-5068,"DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Declaração de Óbito"
064-1801,"PROCEDIMENTO INVÁLIDO - O erro está no campo: Tabela TUSS do Item"
064-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tabela TUSS do Item"
064-5035,"CÓDIGO DA TABELA DE REFERÊNCIA NÃO INFORMADO - O erro está no campo: Tabela TUSS do Item"
064-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Tabela TUSS do Item"
065-1801,"PROCEDIMENTO INVÁLIDO - O erro está no campo: Cód. Grupo TUSS"
065-5029,"INDICADOR INVÁLIDO - O erro está no campo: Cód. Grupo TUSS"
065-5036,"CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - O erro está no campo: Cód. Grupo TUSS"
065-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Cód. Grupo TUSS"
066-1801,"PROCEDIMENTO INVÁLIDO - O erro está no campo: Cód. Procedimento ou Item"
066-2601,"CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO. - O erro está no campo: Cód. Procedimento ou Item"
067-5029,"INDICADOR INVÁLIDO - O erro está no campo: Dente"
067-5069,"CÓDIGO DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Dente"
068-5029,"INDICADOR INVÁLIDO - O erro está no campo: Região da Boca"
068-5070,"CÓDIGO DA REGIÃO DA BOCA DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Região da Boca"
069-5029,"INDICADOR INVÁLIDO - O erro está no campo: Face do Dente"
069-5039,"CÓDIGO DA FACE DO DENTE INVÁLIDO - O erro está no campo: Face do Dente"
069-5071,"CÓDIGO DA FACE DO DENTE DIFERente DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Face do Dente"
070-1806,"QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - O erro está no campo: Qtde. Procedimento ou Item"
071-5029,"INDICADOR INVÁLIDO - O erro está no campo: Valor Informado do Item"
071-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor Informado do Item"
071-5072,"VALOR INFORMADO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - O erro está no campo: Valor Informado do Item"
072-1806,"QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - O erro está no campo: Qtde. Paga do Item"
072-5034,"VALOR NÃO INFORMADO - O erro está no campo: Qtde. Paga do Item"
073-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor Pago ao Prestador ou Reembolso"
073-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor Pago ao Prestador ou Reembolso"
074-1740,"ESTORNO DO VALOR DE PROCEDIMENTO PAGO - O erro está no campo: Valor Pago ao Fornecedor"
074-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor Pago ao Fornecedor"
075-1206,"CPF / CNPJ INVÁLIDO - O erro está no campo: CNPJ Fornecedor"
080-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Processamento"
080-5025,"DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - O erro está no campo: Data Processamento"
081-5027,"REGISTRO ANS DA OPERADORA INVÁLIDO - O erro está no campo: Registro ANS da OPS Intermediária"
081-5062,"REGISTRO ANS DA OPERADORA INTERMEDIÁRIA NÃO INFORMADO - O erro está no campo: Registro ANS da OPS Intermediária"
082-5050,"VALOR INFORMADO INVÁLIDO - O erro está no campo: Nr. Contrato Valor Pré"
082-5052,"IDENTIFICADOR INEXISTENTE - O erro está no campo: Nr. Contrato Valor Pré"
083-1307,"NÚMERO DA GUIA INVÁLIDO - O erro está no campo: Nr. Guia Principal SP/SADT ou Odonto"
084-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor Coparticipação"
084-5050,"VALOR INFORMADO INVÁLIDO - O erro está no campo: Valor Coparticipação"
085-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor Coparticipação Item"
085-5050,"VALOR INFORMADO INVÁLIDO - O erro está no campo: Valor Coparticipação Item"
088-1202,"NÚMERO DO CNES INVÁLIDO - O erro está no campo: CNES Executante"
090-1206,"CPF / CNPJ INVÁLIDO - O erro está no campo: CNPJ/CPF Executante"
091-5030,"CÓDIGO DO MUNÍCIPIO INVÁLIDO - O erro está no campo: Município do Executante"
092-5027,"REGISTRO ANS DA OPERADORA INVÁLIDO - O erro está no campo: Registro ANS da operadora intermediária"
093-5029,"INDICADOR INVÁLIDO - O erro está no campo: Nr. Contrato Valor Pré"
093-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Nr. Contrato Valor Pré"
093-5054,"IDENTIFICADOR NÃO ENCONTRADO - O erro está no campo: Nr. Contrato Valor Pré"
093-5059,"EXCLUSÃO INVÁLIDA - EXISTEM LANÇAMENTOS VINCULADOS A ESTA FORMA DE CONTRATAÇÃO - O erro está no campo: Nr. Contrato Valor Pré"
094-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor Cobertura na Competência"
096-1002,"NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - O erro está no campo: CNS - Cartão Nacional de Saúde"
096-5086,"DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - O erro está no campo: CNS - Cartão Nacional de Saúde"
097-5029,"INDICADOR INVÁLIDO - O erro está no campo: Sexo do beneficiário"
097-5077,"SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - O erro está no campo: Sexo do beneficiário"
098-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Nascimento"
098-5078,"DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - O erro está no campo: Data Nascimento"
099-5030,"CÓDIGO DO MUNÍCIPIO INVÁLIDO - O erro está no campo: Município de residência do beneficiário"
0100-1024,"PLANO NÃO EXISTENTE - O erro está no campo: Número de identificação do plano do beneficiário na ANS"
0101-5029,"INDICADOR INVÁLIDO - O erro está no campo: Identificador da operação de fornecimento de materiais e medicamentos"
0101-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Identificador da operação de fornecimento de materiais e medicamentos"
0101-5054,"IDENTIFICADOR NÃO ENCONTRADO - O erro está no campo: Identificador da operação de fornecimento de materiais e medicamentos"
0102-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Fornecimento"
0102-5023,"COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - O erro está no campo: Data Fornecimento"
0103-1706,"VALOR APRESENTADO A MENOR - O erro está no campo: Valor total dos itens fornecidos"
0103-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total dos itens fornecidos"
0103-5050,"VALOR INFORMADO INVÁLIDO - O erro está no campo: Valor total dos itens fornecidos"
0104-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total em tabela própria da operadora"
0105-1706,"VALOR APRESENTADO A MENOR - O erro está no campo: Valor total de co-participação"
0105-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total de co-participação"
0105-5050,"VALOR INFORMADO INVÁLIDO - O erro está no campo: Valor total de co-participação"
0106-5029,"INDICADOR INVÁLIDO - O erro está no campo: Tabela TUSS do Item Fornecido"
0106-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Tabela TUSS do Item Fornecido"
0106-5054,"IDENTIFICADOR NÃO ENCONTRADO - O erro está no campo: Tabela TUSS do Item Fornecido"
0107-5029,"INDICADOR INVÁLIDO - O erro está no campo: Código do grupo do procedimento ou item assistencial"
0107-5036,"CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - O erro está no campo: Código do grupo do procedimento ou item assistencial"
0107-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Código do grupo do procedimento ou item assistencial"
0107-5054,"IDENTIFICADOR NÃO ENCONTRADO - O erro está no campo: Código do grupo do procedimento ou item assistencial"
0108-1801,"PROCEDIMENTO INVÁLIDO - O erro está no campo: Código do item assistencial fornecido"
0108-2601,"CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO. - O erro está no campo: Código do item assistencial fornecido"
0108-5053,"IDENTIFICADOR JÁ INFORMADO - O erro está no campo: Código do item assistencial fornecido"
0108-5054,"IDENTIFICADOR NÃO ENCONTRADO - O erro está no campo: Código do item assistencial fornecido"
0109-1806,"QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - O erro está no campo: Quantidade informada de itens assistenciais"
0110-5040,"VALOR DEVE SER MAIOR QUE ZERO - O erro está no campo: Valor do item assistencial fornecido"
0111-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor de co-participação"
0113-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Data Processamento"
0113-5023,"COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - O erro está no campo: Data Processamento"
0115-1206,"CPF / CNPJ INVÁLIDO - O erro está no campo: CNPJ/CPF Executante/Recebedor"
0115-5055,"IDENTIFICADOR JÁ INFORMADO NA COMPETÊNCIA - O erro está no campo: CNPJ/CPF Executante/Recebedor"
0115-5056,"IDENTIFICADOR NÃO INFORMADO NA COMPETÊNCIA - O erro está no campo: CNPJ/CPF Executante/Recebedor"
0116-5040,"VALOR DEVE SER MAIOR QUE ZERO - O erro está no campo: Valor Total Informado"
0117-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total de Glosa"
0118-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor total Pago"
0119-1323,"DATA PREENCHIDA INCORRETAMENTE - O erro está no campo: Competência da cobertura contratada"
0119-5023,"COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS - O erro está no campo: Competência da cobertura contratada"
0119-5045,"COMPETÊNCIA ANTERIOR NÃO ENVIADA - O erro está no campo: Competência da cobertura contratada"
0120-5061,"TIPO DE ATENDIMENTO OPERADORA INTERMEDIÁRIA NÃO INFORMADO - O erro está no campo: Tipo de atendimento por OPS intermediária"
0121-1206,"CPF / CNPJ INVÁLIDO - O erro está no campo: CPF do Beneficiário"
0121-5076,"CPF NÃO ENCONTRADO NA RECEITA FEDERAL - O erro está no campo: CPF do Beneficiário"
0122-5079,"MODELO DE REMUNERAÇÃO EM DUPLICIDADE. - O erro está no campo: Modelo de remuneração"
0122-5080,"MODELO DE REMUNERAÇÃO NÃO INFORMADO - O erro está no campo: Modelo de remuneração"
0122-5081,"MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REEMBOLSO/PRESTADOR EVENTUAL - O erro está no campo: Modelo de remuneração"
0122-5082,"MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REDE PROPRIA COM MESMO CNPJ - O erro está no campo: Modelo de remuneração"
0123-5074,"REGIME DE ATENDIMENTO INVÁLIDO - O erro está no campo: Regime de atendimento"
0124-5029,"INDICADOR INVÁLIDO - O erro está no campo: Saúde ocupacional"
0125-5029,"INDICADOR INVÁLIDO - O erro está no campo: Unidade de medida"
0125-5084,"UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - O erro está no campo: Unidade de medida"
0125-5085,"UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - O erro está no campo: Unidade de medida"
0126-5076,"CPF NÃO ENCONTRADO NA RECEITA FEDERAL - O erro está no campo: CPF do Beneficiário"
0127-5084,"UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - O erro está no campo: Unidade de Medida"
0127-5085,"UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - O erro está no campo: Unidade de Medida"
0128-5034,"VALOR NÃO INFORMADO - O erro está no campo: Valor informado do modelo de remuneração"
0128-5050,"VALOR INFORMADO INVÁLIDO - O erro está no campo: Valor informado do modelo de remuneração"
0129-5029,"INDICADOR INVÁLIDO - O erro está no campo: Unidade de medida de itens assistenciais que compõem o pacote"
0129-5084,"UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - O erro está no campo: Unidade de medida de itens assistenciais que compõem o pacote"
0129-5085,"UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - O erro está no campo: Unidade de medida de itens assistenciais que compõem o pacote"
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
    Analisa um arquivo .XTR e cria um DataFrame "longo" (uma linha por erro),
    com uma coluna 'codigoErroUnico' que concatena o código do erro e o campo.
    """
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}
    all_errors = []

    caminho_das_guias = './/ans:resumoProcessamento/ans:registrosRejeitados/ans:guiaMonitoramento'

    for guia_rejeitada in root.findall(caminho_das_guias, namespaces=ns):
        guia_info = {
            'nomeArquivo_xtr': file.name,
            'numeroGuiaOperadora_xtr': guia_rejeitada.findtext('ans:numeroGuiaOperadora', default='', namespaces=ns).strip()
        }

        # Função interna para processar e concatenar um nó de erro
        def processar_erro_node(erro_node):
            codigo_erro = erro_node.findtext(
                'ans:codigoErro', default='', namespaces=ns).strip()
            id_campo = erro_node.findtext(
                'ans:identificadorCampo', default='', namespaces=ns).strip()
            if codigo_erro and id_campo:
                linha = guia_info.copy()
                linha['codigoErroUnico'] = f"{id_campo}-{codigo_erro}"
                all_errors.append(linha)

        # Processa erros de guia
        for erro in guia_rejeitada.findall('ans:errosGuia', namespaces=ns):
            processar_erro_node(erro)

        # Processa erros de item
        for item_erro in guia_rejeitada.findall('ans:errosItensGuia', namespaces=ns):
            for relacao_erro in item_erro.findall('ans:relacaoErros', namespaces=ns):
                processar_erro_node(relacao_erro)

    df_errors = pd.DataFrame(all_errors)
    return df_errors


@st.cache_data
def parse_xtr_para_relatorio_wide(file):
    """
    Analisa um arquivo .XTR e cria um DataFrame "largo" (uma linha por guia),
    capturando apenas o PRIMEIRO erro de guia e o PRIMEIRO erro de procedimento para replicar o modelo.
    """
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}

    all_guides_data = []

    caminho_das_guias = './/ans:resumoProcessamento/ans:registrosRejeitados/ans:guiaMonitoramento'

    for guia_rejeitada in root.findall(caminho_das_guias, namespaces=ns):
        contratado = guia_rejeitada.find('ans:contratadoExecutante', ns)

        guia_data = {
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

        # Captura apenas o PRIMEIRO erro de guia
        primeiro_erro_guia = guia_rejeitada.find('ans:errosGuia', ns)
        if primeiro_erro_guia is not None:
            guia_data['guia_identificadorCampo'] = primeiro_erro_guia.findtext(
                'ans:identificadorCampo', default='', namespaces=ns)
            guia_data['guia_codigoErro'] = primeiro_erro_guia.findtext(
                'ans:codigoErro', default='', namespaces=ns)

        # Captura dados apenas do PRIMEIRO erro de procedimento
        primeiro_item_erro = guia_rejeitada.find('ans:errosItensGuia', ns)
        if primeiro_item_erro is not None:
            ident_proc = primeiro_item_erro.find('ans:identProcedimento', ns)
            if ident_proc is not None:
                guia_data['proc_codigoTabela'] = ident_proc.findtext(
                    'ans:codigoTabela', default='', namespaces=ns)
                guia_data['proc_codigoProcedimento'] = ident_proc.findtext(
                    'ans:Procedimento/ans:codigoProcedimento', default='', namespaces=ns)

            primeira_relacao_erro = primeiro_item_erro.find(
                'ans:relacaoErros', ns)
            if primeira_relacao_erro is not None:
                guia_data['proc_identificadorCampo'] = primeira_relacao_erro.findtext(
                    'ans:identificadorCampo', default='', namespaces=ns)
                guia_data['proc_codigoErro'] = primeira_relacao_erro.findtext(
                    'ans:codigoErro', default='', namespaces=ns)

        all_guides_data.append(guia_data)

    return pd.DataFrame(all_guides_data)


def converter_xtr_para_xlsx(uploaded_xtr_files):
    """
    Converte arquivos XTR em uma planilha Excel IDÊNTICA ao modelo da imagem,
    com formatação precisa de cabeçalhos, colunas e cores.
    """
    if not uploaded_xtr_files:
        return None, None

    # PASSO 1: Usar o parser "wide" para obter uma linha por guia
    df_list = [parse_xtr_para_relatorio_wide(f) for f in uploaded_xtr_files]
    df_wide = pd.concat(df_list, ignore_index=True)

    if df_wide.empty:
        return None, None

    # PASSO 2: Renomear as colunas para os nomes dos sub-cabeçalhos do modelo
    df_wide = df_wide.rename(columns={
        'guia_identificadorCampo': 'identificadorCampo',
        'guia_codigoErro': 'codigoErro',
        'proc_codigoTabela': 'codigoTabela',
        'proc_codigoProcedimento': 'codigoProcedimento',
        'proc_identificadorCampo': 'identificadorCampo.1',
        'proc_codigoErro': 'codigoErro.1'
    })

    # PASSO 3: Garantir a ordem final e a existência de todas as colunas do modelo
    final_column_order = [
        'Nome da Origem', 'tipoRegistroANS', 'registroANS', 'CNES', 'CNPJ_CPF',
        'numeroGuiaPrestador', 'numeroGuiaOperadora', 'identificadorReembolso', 'dataProcessamento',
        'identificadorCampo', 'codigoErro',
        'codigoTabela', 'codigoProcedimento', 'identificadorCampo.1', 'codigoErro.1'
    ]
    for col in final_column_order:
        if col not in df_wide.columns:
            df_wide[col] = ''

    df_wide = df_wide[final_column_order]

    # --- INÍCIO DA CORREÇÃO ---
    # PASSO 3.5: Substituir todos os valores NaN por strings vazias para evitar o erro do XlsxWriter
    df_wide.fillna('', inplace=True)
    # --- FIM DA CORREÇÃO ---

    # PASSO 4: Gerar o Excel com formatação avançada e precisa
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        # Escreve o dataframe a partir da linha 4 (índice 3), sem o cabeçalho do Pandas
        df_wide.to_excel(writer, sheet_name='Modelo XTR',
                         index=False, header=False, startrow=3)

        workbook = writer.book
        worksheet = writer.sheets['Modelo XTR']

        # Definição de Formatos
        merged_header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#E0E0E0', 'border': 1})
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

        headers = df_wide.columns.values
        for i in range(9):
            worksheet.merge_range(1, i, 2, i, headers[i], main_header_format)

        worksheet.merge_range('J2:K2', 'ERRO GUIA', error_guide_header_format)
        worksheet.merge_range('L2:O2', 'ERRO PROCEDIMENTO',
                              error_proc_header_format)

        for i in range(9, 15):
            worksheet.write(2, i, headers[i], sub_header_format)

        # Formatação das Células de Dados
        for row_num in range(3, len(df_wide) + 3):
            for col_num in range(len(df_wide.columns)):
                cell_value = df_wide.iloc[row_num - 3, col_num]

                # Escolhe o formato com base na coluna
                if 9 <= col_num <= 10:
                    worksheet.write(row_num, col_num,
                                    cell_value, yellow_data_format)
                elif 11 <= col_num <= 14:
                    worksheet.write(row_num, col_num,
                                    cell_value, blue_data_format)
                else:
                    worksheet.write(row_num, col_num,
                                    cell_value, default_data_format)

        # Ajuste Final de Layout
        worksheet.set_column('A:A', 30)
        worksheet.set_column('B:I', 20)
        worksheet.set_column('J:K', 20)
        worksheet.set_column('L:O', 22)
        worksheet.freeze_panes(3, 0)

    return excel_buffer.getvalue(), len(df_wide)


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
                "Passo 1: Consolidando arquivos de envio (.xte)... ⏳")
            df_xte_list = [parse_xte(f)[0] for f in uploaded_xte_files]
            df_xte_full = pd.concat(df_xte_list, ignore_index=True)
            progress_bar.progress(14)
            log_leitura_xte.success(
                f"Passo 1: Arquivos XTE consolidados! ✅ ({len(df_xte_full)} registros)")

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
                "Passo 2: Consolidando e formatando arquivos de retorno (.xtr)... ⏳")

            excel_data_xtr, total_erros = converter_xtr_para_xlsx(
                uploaded_xtr_files)

            progress_bar.progress(28)
            log_leitura_xtr.success(
                f"Passo 2: Arquivos XTR consolidados e formatados! ✅ ({total_erros} erros reportados)")

            if excel_data_xtr is not None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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

            # É necessário recarregar os dados do XTR para a proxima etapa, pois a função de conversão os consome
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

            # Passo 4: Preparar Chaves de Cruzamento
            status_placeholder.info(
                "Passo 4/7: Preparando chaves de cruzamento nos dados... ⏳")
            df_xte_full['nomeArquivo_base'] = df_xte_full['Nome da Origem'].apply(
                lambda x: os.path.splitext(x)[0])
            df_xte_full['chave_cruzamento'] = df_xte_full['nomeArquivo_base'].astype(
                str) + "_" + df_xte_full['numeroGuia_operadora'].astype(str)
            if not df_xtr_full.empty:
                df_xtr_full['nomeArquivo_base'] = df_xtr_full['nomeArquivo_xtr'].apply(
                    lambda x: os.path.splitext(x)[0])
                df_xtr_full['chave_cruzamento'] = df_xtr_full['nomeArquivo_base'].astype(
                    str) + "_" + df_xtr_full['numeroGuiaOperadora_xtr'].astype(str)
            progress_bar.progress(56)
            status_placeholder.success(
                "Passo 4/7: Chaves de cruzamento preparadas! ✅")
            time.sleep(1)

            # Passo 5: Agrupar Erros por Guia
            status_placeholder.info(
                "Passo 5/7: Agrupando os erros por guia... ⏳")
            erros_agrupados = pd.DataFrame(columns=['chave_cruzamento'])
            if not df_xtr_full.empty:
                # Agrupa todos os códigos de erro únicos em uma lista para cada guia
                erros_agrupados = df_xtr_full.groupby('chave_cruzamento')[
                    'codigoErroUnico'].unique().apply(list).reset_index()
                erros_agrupados.rename(
                    columns={'codigoErroUnico': 'lista_erros'}, inplace=True)

            progress_bar.progress(70)
            status_placeholder.success(
                "Passo 5/7: Erros agrupados com sucesso! ✅")
            time.sleep(1)

            # Passo 6: Juntar Dados Originais com a Lista de Erros
            status_placeholder.info(
                "Passo 6/7: Cruzando dados originais com as listas de erros... ⏳")
            df_analise = pd.merge(
                df_xte_full, erros_agrupados, on='chave_cruzamento', how='inner')
            progress_bar.progress(85)
            status_placeholder.success(
                "Passo 6/7: Guias com erro identificadas e cruzadas! ✅")
            time.sleep(1)

            # Passo 7: Criar Colunas de Erro Dinamicamente e Organizar
            status_placeholder.info(
                "Passo 7/7: Montando o relatório final com colunas de erro... ⏳")

            error_map = pd.Series(
                df_ans_errors.descricaoErro.values, index=df_ans_errors.codigoErroUnico).to_dict()

            df_final = df_analise.copy()
            if not df_final.empty and 'lista_erros' in df_final.columns:
                unique_errors_in_batch = sorted(
                    list(set(e for lista in df_final['lista_erros'] for e in lista)))

                for error_code in unique_errors_in_batch:
                    col_name = f"Erro_{error_code}"
                    description = error_map.get(
                        error_code, f"Descrição não encontrada para {error_code}")
                    df_final[col_name] = df_final['lista_erros'].apply(
                        lambda x: description if error_code in x else None)

            original_cols = list(df_xte_full.columns.drop(
                ['nomeArquivo_base', 'chave_cruzamento']))
            error_cols = sorted(
                [col for col in df_final.columns if col.startswith('Erro_')])

            if 'lista_erros' in df_final.columns:
                df_final.drop(columns=['lista_erros'], inplace=True)

            df_final = df_final[original_cols + error_cols]

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
                    f"🎉 Análise concluída! Foram encontradas {len(df_final)} linhas de procedimento nas guias com erro.")
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
            st.error("Verifique se os arquivos estão no formato correto.")
