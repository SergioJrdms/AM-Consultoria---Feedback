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
016-1002,Motivo Glosa: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - Crítica ANS: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO
096-1002,Motivo Glosa: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - Crítica ANS: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO
020-1024,Motivo Glosa: PLANO NÃO EXISTENTE - Crítica ANS: PLANO NÃO EXISTENTE
0100-1024,Motivo Glosa: PLANO NÃO EXISTENTE - Crítica ANS: PLANO NÃO EXISTENTE
012-1202,Motivo Glosa: NÚMERO DO CNES INVÁLIDO - Crítica ANS: NÚMERO DO CNES INVÁLIDO
088-1202,Motivo Glosa: NÚMERO DO CNES INVÁLIDO - Crítica ANS: NÚMERO DO CNES INVÁLIDO
014-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: CPF / CNPJ INVÁLIDO
075-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: CPF / CNPJ INVÁLIDO
090-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: CPF / CNPJ INVÁLIDO
0115-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: CPF / CNPJ INVÁLIDO
0121-1206,Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: CPF / CNPJ INVÁLIDO
035-1213,Motivo Glosa: CBO (ESPECIALIDADE) INVÁLIDO - Crítica ANS: CBO (ESPECIALIDADE) INVÁLIDO
047-1304,Motivo Glosa: COBRANÇA EM GUIA INDEVIDA - Crítica ANS: COBRANÇA EM GUIA INDEVIDA
048-1304,Motivo Glosa: COBRANÇA EM GUIA INDEVIDA - Crítica ANS: COBRANÇA EM GUIA INDEVIDA
023-1307,Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: NÚMERO DA GUIA INVÁLIDO
024-1307,Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: NÚMERO DA GUIA INVÁLIDO
025-1307,Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: NÚMERO DA GUIA INVÁLIDO
026-1307,Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: NÚMERO DA GUIA INVÁLIDO
083-1307,Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: NÚMERO DA GUIA INVÁLIDO
023-1308,Motivo Glosa: GUIA JÁ APRESENTADA - Crítica ANS: GUIA JÁ APRESENTADA
018-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
027-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
028-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
029-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
030-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
031-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
032-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
033-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
080-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
098-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
0102-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
0113-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
0119-1323,Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: DATA PREENCHIDA INCORRETAMENTE
039-1506,Motivo Glosa: TIPO DE INTERNAÇÃO INVÁLIDO - Crítica ANS: TIPO DE INTERNAÇÃO INVÁLIDO
041-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: CÓDIGO CID INVÁLIDO
042-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: CÓDIGO CID INVÁLIDO
043-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: CÓDIGO CID INVÁLIDO
044-1509,Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: CÓDIGO CID INVÁLIDO
045-1602,Motivo Glosa: TIPO DE ATENDIMENTO INVÁLIDO OU NÃO INFORMADO - Crítica ANS: TIPO DE ATENDIMENTO INVÁLIDO OU NÃO INFORMADO
034-1603,Motivo Glosa: TIPO DE CONSULTA INVÁLIDO - Crítica ANS: TIPO DE CONSULTA INVÁLIDO
050-1705,Motivo Glosa: VALOR APRESENTADO A MAIOR - Crítica ANS: VALOR APRESENTADO A MAIOR
050-1706,Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: VALOR APRESENTADO A MENOR
0103-1706,Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: VALOR APRESENTADO A MENOR
0105-1706,Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: VALOR APRESENTADO A MENOR
046-1713,Motivo Glosa: FATURAMENTO INVÁLIDO - Crítica ANS: FATURAMENTO INVÁLIDO
052-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
053-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
054-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
055-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
056-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
057-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
059-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
060-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
061-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
073-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
074-1740,Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: ESTORNO DO VALOR DE PROCEDIMENTO PAGO
066-1801,Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: PROCEDIMENTO INVÁLIDO
078-1801,Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: PROCEDIMENTO INVÁLIDO
0108-1801,Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: PROCEDIMENTO INVÁLIDO
064-1801,Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: PROCEDIMENTO INVÁLIDO
065-1801,Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: PROCEDIMENTO INVÁLIDO
066-1801,Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: PROCEDIMENTO INVÁLIDO
070-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
072-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
079-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
0109-1806,Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
066-2601,Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO - Crítica ANS: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO
078-2601,Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO - Crítica ANS: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO
0108-2601,Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO - Crítica ANS: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO
0-5001,Motivo Glosa: MENSAGEM ELETRÔNICA FORA DO PADRÃO TISS - Crítica ANS: MENSAGEM ELETRÔNICA FORA DO PADRÃO TISS
0-5002,Motivo Glosa: NÃO FOI POSSÍVEL VALIDAR O ARQUIVO XML - Crítica ANS: NÃO FOI POSSÍVEL VALIDAR O ARQUIVO XML
0-5014,Motivo Glosa: CÓDIGO HASH INVÁLIDO. MENSAGEM PODE ESTAR CORROMPIDA. - Crítica ANS: CÓDIGO HASH INVÁLIDO. MENSAGEM PODE ESTAR CORROMPIDA.
0-5016,Motivo Glosa: SEM NENHUMA OCORRÊNCIA DE MOVIMENTO DE INCLUSÃO NA COMPETÊNCIA PARA ENVIO A ANS - Crítica ANS: SEM NENHUMA OCORRÊNCIA DE MOVIMENTO DE INCLUSÃO NA COMPETÊNCIA PARA ENVIO A ANS
0-5017,Motivo Glosa: ARQUIVO PROCESSADO PELA ANS. - Crítica ANS: ARQUIVO PROCESSADO PELA ANS.
0102-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
0113-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
0119-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
0-5023,Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
03-5024,Motivo Glosa: OPERADORA INATIVA NA COMPETÊNCIA DOS DADOS - Crítica ANS: OPERADORA INATIVA NA COMPETÊNCIA DOS DADOS
04-5025,Motivo Glosa: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA
080-5025,Motivo Glosa: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA
05-5026,Motivo Glosa: HORA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: HORA DE REGISTRO DA TRANSAÇÃO INVÁLIDA
06-5027,Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: REGISTRO ANS DA OPERADORA INVÁLIDO
081-5027,Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: REGISTRO ANS DA OPERADORA INVÁLIDO
092-5027,Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: REGISTRO ANS DA OPERADORA INVÁLIDO
010-5028,Motivo Glosa: VERSÃO DO PADRÃO INVÁLIDA - Crítica ANS: VERSÃO DO PADRÃO INVÁLIDA
012-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
02-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
013-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
014-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
015-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
016-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
017-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
019-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
020-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
021-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
022-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
023-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
024-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
025-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
035-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
037-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
038-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
039-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
040-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
045-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
046-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
062-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
063-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
093-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
097-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
0101-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
0124-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
0125-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
0129-5029,Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: INDICADOR INVÁLIDO
015-5030,Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: CÓDIGO DO MUNICÍPIO INVÁLIDO
019-5030,Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: CÓDIGO DO MUNICÍPIO INVÁLIDO
091-5030,Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: CÓDIGO DO MUNICÍPIO INVÁLIDO
099-5030,Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: CÓDIGO DO MUNICÍPIO INVÁLIDO
038-5031,Motivo Glosa: CARÁTER DE ATENDIMENTO INVÁLIDO - Crítica ANS: CARÁTER DE ATENDIMENTO INVÁLIDO
036-5032,Motivo Glosa: INDICADOR DE RECÉM–NATO INVÁLIDO - Crítica ANS: INDICADOR DE RECÉM–NATO INVÁLIDO
049-5033,Motivo Glosa: MOTIVO DE ENCERRAMENTO INVÁLIDO - Crítica ANS: MOTIVO DE ENCERRAMENTO INVÁLIDO
051-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
052-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
053-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
054-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
055-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
056-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
057-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
058-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
059-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
060-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
061-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
063-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
071-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
072-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
073-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
074-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
084-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
085-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
085-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
094-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0103-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0104-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0105-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0111-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0117-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0118-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
0128-5034,Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: VALOR NÃO INFORMADO
064-5035,Motivo Glosa: CÓDIGO DA TABELA DE REFERÊNCIA NÃO INFORMADO - Crítica ANS: CÓDIGO DA TABELA DE REFERÊNCIA NÃO INFORMADO
065-5036,Motivo Glosa: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - Crítica ANS: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO
0107-5036,Motivo Glosa: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - Crítica ANS: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO
069-5039,Motivo Glosa: CÓDIGO DA FACE DO DENTE INVÁLIDO - Crítica ANS: CÓDIGO DA FACE DO DENTE INVÁLIDO
0110-5040,Motivo Glosa: VALOR DEVE SER MAIOR QUE ZERO - Crítica ANS: VALOR DEVE SER MAIOR QUE ZERO
0116-5040,Motivo Glosa: VALOR DEVE SER MAIOR QUE ZERO - Crítica ANS: VALOR DEVE SER MAIOR QUE ZERO
050-5042,Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS
059-5042,Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS
060-5042,Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS
0-5044,Motivo Glosa: JÁ EXISTEM INFORMAÇÕES NA ANS PARA A COMPETÊNCIA INFORMADA. - Crítica ANS: JÁ EXISTEM INFORMAÇÕES NA ANS PARA A COMPETÊNCIA INFORMADA.
0119-5045,Motivo Glosa: COMPETENCIA ANTERIOR NÃO ENVIADA - Crítica ANS: COMPETENCIA ANTERIOR NÃO ENVIADA
0-5045,Motivo Glosa: COMPETENCIA ANTERIOR NÃO ENVIADA - Crítica ANS: COMPETENCIA ANTERIOR NÃO ENVIADA
0-5046,Motivo Glosa: COMPETÊNCIA INVÁLIDA - Crítica ANS: COMPETÊNCIA INVÁLIDA
082-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: VALOR INFORMADO INVÁLIDO
084-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: VALOR INFORMADO INVÁLIDO
085-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: VALOR INFORMADO INVÁLIDO
0103-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: VALOR INFORMADO INVÁLIDO
0105-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: VALOR INFORMADO INVÁLIDO
0128-5050,Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: VALOR INFORMADO INVÁLIDO
082-5052,Motivo Glosa: IDENTIFICADOR INEXISTENTE - Crítica ANS: IDENTIFICADOR INEXISTENTE
064-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
065-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
077-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
078-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
093-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
0101-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
0106-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
0107-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
0108-5053,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: IDENTIFICADOR JÁ INFORMADO
093-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: IDENTIFICADOR NÃO ENCONTRADO
0101-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: IDENTIFICADOR NÃO ENCONTRADO
0106-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: IDENTIFICADOR NÃO ENCONTRADO
0107-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: IDENTIFICADOR NÃO ENCONTRADO
0108-5054,Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: IDENTIFICADOR NÃO ENCONTRADO
0115-5055,Motivo Glosa: IDENTIFICADOR JÁ INFORMADO NA COMPETÊNCIA - Crítica ANS: IDENTIFICADOR JÁ INFORMADO NA COMPETÊNCIA
0115-5056,Motivo Glosa: IDENTIFICADOR NÃO INFORMADO NA COMPETÊNCIA - Crítica ANS: IDENTIFICADOR NÃO INFORMADO NA COMPETÊNCIA
093-5059,Motivo Glosa: EXCLUSÃO INVÁLIDA – EXISTEM LANÇAMENTOS VINCULADOS A ESTA FORMA DE CONTRATAÇÃO - Crítica ANS: EXCLUSÃO INVÁLIDA – EXISTEM LANÇAMENTOS VINCULADOS A ESTA FORMA DE CONTRATAÇÃO
0120-5061,Motivo Glosa: TIPO DE ATENDIMENTO OPERADORA INTERMEDIÁRIA NÃO INFORMADO - Crítica ANS: TIPO DE ATENDIMENTO OPERADORA INTERMEDIÁRIA NÃO INFORMADO
081-5062,Motivo Glosa: REGISTRO ANS DA OPERADORA INTERMEDIÁRIA NÃO INFORMADO - Crítica ANS: REGISTRO ANS DA OPERADORA INTERMEDIÁRIA NÃO INFORMADO
012-5063,Motivo Glosa: PAR CNPJ X CNES NAO ENCONTRADO NA BASE DO CNES - Crítica ANS: PAR CNPJ X CNES NAO ENCONTRADO NA BASE DO CNES
012-5064,Motivo Glosa: TIPO DE ESTABELECIMENTO NO CNES NÃO É APTO PARA INTERNAÇÃO - Crítica ANS: TIPO DE ESTABELECIMENTO NO CNES NÃO É APTO PARA INTERNAÇÃO
014-5065,Motivo Glosa: TIPO DE ATIVIDADE ECONOMICA DO CNPJ NÃO É APTO PARA INTERNAÇÃO - Crítica ANS: TIPO DE ATIVIDADE ECONOMICA DO CNPJ NÃO É APTO PARA INTERNAÇÃO
062-5066,Motivo Glosa: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE - Crítica ANS: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE
063-5066,Motivo Glosa: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE - Crítica ANS: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE
042-5067,Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: DIAGNÓSTICO EM DUPLICIDADE
043-5067,Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: DIAGNÓSTICO EM DUPLICIDADE
044-5067,Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: DIAGNÓSTICO EM DUPLICIDADE
041-5067,Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: DIAGNÓSTICO EM DUPLICIDADE
063-5068,Motivo Glosa: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
062-5068,Motivo Glosa: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
067-5069,Motivo Glosa: CÓDIGO DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CÓDIGO DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
068-5070,Motivo Glosa: CÓDIGO DA REGIÃO DA BOCA DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CÓDIGO DA REGIÃO DA BOCA DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
069-5071,Motivo Glosa: CÓDIGO DA FACE DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CÓDIGO DA FACE DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
071-5072,Motivo Glosa: VALOR INFORMADO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: VALOR INFORMADO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
023-5073,Motivo Glosa: O PRIMEIRO LANCAMENTO DA GUIA SÓ PODE SER EXCLUIDO SE ELE FOR O ÚNICO - Crítica ANS: O PRIMEIRO LANCAMENTO DA GUIA SÓ PODE SER EXCLUIDO SE ELE FOR O ÚNICO
0123-5074,Motivo Glosa: REGIME DE ATENDIMENTO INVÁLIDO - Crítica ANS: REGIME DE ATENDIMENTO INVÁLIDO
041-5075,Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
042-5075,Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
043-5075,Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
044-5075,Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA
0121-5076,Motivo Glosa: CPF NÃO ENCONTRADO NA RECEITA FEDERAL - Crítica ANS: CPF NÃO ENCONTRADO NA RECEITA FEDERAL
0126-5076,Motivo Glosa: CPF NÃO ENCONTRADO NA RECEITA FEDERAL - Crítica ANS: CPF NÃO ENCONTRADO NA RECEITA FEDERAL
017-5077,Motivo Glosa: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - Crítica ANS: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF
097-5077,Motivo Glosa: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - Crítica ANS: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF
018-5078,Motivo Glosa: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - Crítica ANS: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF
098-5078,Motivo Glosa: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - Crítica ANS: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF
0122-5079,Motivo Glosa: MODELO DE REMUNERAÇÃO EM DUPLICIDADE - Crítica ANS: MODELO DE REMUNERAÇÃO EM DUPLICIDADE
0122-5080,Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO INFORMADO - Crítica ANS: MODELO DE REMUNERAÇÃO NÃO INFORMADO
0122-5081,Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REEMBOLSO/PRESTADOR EVENTUAL - Crítica ANS: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REEMBOLSO/PRESTADOR EVENTUAL
0122-5082,Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REDE PRÓPRIA COM MESMO CNPJ - Crítica ANS: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REDE PRÓPRIA COM MESMO CNPJ
050-5083,Motivo Glosa: SOMA DOS VALORES DOS MODELOS DE REMUNERAÇÃO DIFERENTE DO VALOR INFORMADO DA GUIA - Crítica ANS: SOMA DOS VALORES DOS MODELOS DE REMUNERAÇÃO DIFERENTE DO VALOR INFORMADO DA GUIA
0125-5084,Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA
0127-5084,Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA
0129-5084,Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA
0125-5085,Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA
0127-5085,Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA
0129-5085,Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA
016-5086,Motivo Glosa: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - Crítica ANS: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO)
096-5086,Motivo Glosa: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - Crítica ANS: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO)

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


