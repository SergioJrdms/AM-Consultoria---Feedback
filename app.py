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
"016-1002","Motivo Glosa: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - Crítica ANS: Deve ser um número válido na base do Cartão Nacional de Saúde."
"096-1002","Motivo Glosa: NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO - Crítica ANS: Número do CNS, contratação fornecimento direto. Deve ser um número válido na base do Cartão Nacional de Saúde."
"020-1024","Motivo Glosa: PLANO NÃO EXISTENTE - Crítica ANS: Deve ser um número de registro de plano válido na base de dados de planos da operadora que realizou o envio do arquivo."
"0100-1024","Motivo Glosa: PLANO NÃO EXISTENTE - Crítica ANS: Deve ser um número de registro de plano válido na base de dados de planos da operadora que realizou o envio do arquivo."
"012-1202","Motivo Glosa: NÚMERO DO CNES INVÁLIDO - Crítica ANS: O campo deve ser preenchido com 7 dígitos, e vinculado ao CNPJ do Prestador Executante. Procurar no Datatus, ou caso não tenha o registro preencher com 9999999."
"088-1202","Motivo Glosa: NÚMERO DO CNES INVÁLIDO - Crítica ANS: CNES, contratação valor pré-estabelecido .O campo deve ser preenchido com 7 dígitos, e vinculado ao CNPJ do Prestador Executante. Procurar no Datasus, ou caso não tenha o registro preencher com 9999999."
"014-1206","Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: Número de cadastro do prestador executante. Se CPF deverá conter 11 dígitos e se CNPJ 14 dígitos. O cadastro deve existir na base de dados da Receita Federal."
"075-1206","Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: Número de cadastro do fornecedor na Receita Federal. Deve ser preenchido quando o item assistencial for pago diretamente pela operadora ao fornecedor. Tem que ser um CNPJ válido na Receita Federal."
"090-1206","Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: Número de cadastro do prestador executante, contratação valor pré-estabelecido. Se CPF deverá conter 11 dígitos e se CNPJ 14 dígitos. O cadastro deve existir na base de dados da Receita Federal."
"0115-1206","Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: Número de cadastro do recebedor, informação de outras despesas assistenciais. Se CPF deverá conter 11 dígitos e se CNPJ 14 dígitos. O cadastro deve existir na base de dados da Receita Federal."
"0121-1206","Motivo Glosa: CPF / CNPJ INVÁLIDO - Crítica ANS: No lançamento de inclusão (do segundo em diante para a mesma guia) ou alteração caso o número do CPF seja informado, este deve ser igual ao que já estava na base de dados para esta guia."
"035-1213","Motivo Glosa: CBO (ESPECIALIDADE) INVÁLIDO - Crítica ANS: O CBO deve ser preenchido no tipo de guia 1 (consulta) 2 (SP/SADT ou 4 (Odonto), e tipo de atendimento igual a 4 (consulta), o Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 24. Não pode ser informado 9999999."
"047-1304","Motivo Glosa: COBRANÇA EM GUIA INDEVIDA - Crítica ANS: Quanto tipo de Guia for 3 (internação) Informar Número diária de acompanhante."
"048-1304","Motivo Glosa: COBRANÇA EM GUIA INDEVIDA - Crítica ANS: Quanto tipo de Guia for 3 (internação) Informar Número diária de UTI."
"023-1307","Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: Número guia do Prestador.Preencher até 20 caracter. Quando a origem da guia for igual a 4-Reembolso ao beneficiário ou 5-Prestador eventual, o campo deve ser preenchido com (20 zeros), quando Inclusão ou Alteração."
"024-1307","Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: Número guia do Operadora.Preencher até 20 caracter. Quando a origem da guia for igual a 4-Reembolso ao beneficiário ou 5-Prestador eventual, o campo deve ser preenchido com (20 zeros), quando Inclusão ou Alteração."
"025-1307","Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: Número atribuído pela operadora para identificar o reembolso ao beneficiário ou o pagamento a prestador eventual, não pertencente à rede da operadora.Deve ser diferente de 20 números 0."
"026-1307","Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: Número da guia de solicitação de internação.Deve ser preenchido quando: o tio de guia for 2 e tratar-se de paciente internado, ou o tipo de guia for igual a 3 ou 5, não deve ser preenchido com zero. Não pode ser informado em guia 4 (odonto) e guia 1 (consulta)."
"083-1307","Motivo Glosa: NÚMERO DA GUIA INVÁLIDO - Crítica ANS: Número da guia principal de 2- SP/SADT ou de 4 (Odonto). Deve ser preenchido quando estiver vinculada a outra Guia de SP/SADT (principal) ou a guia vinculada a outra Guia de Tratamento Odontológico (principal). Quando for reembolso o número deverá ser preenchido com 20 zeros. Não pode ser informado em guia 1 (consulta), 3 (internação) ou 5 (honorários)."
"023-1308","Motivo Glosa: GUIA JÁ APRESENTADA - Crítica ANS: Número guia do Prestador.Preencher até 20 caracter. Quando a origem da guia for igual a 4-Reembolso ao beneficiário ou 5-Prestador eventual,o campo deve ser preenchido com (20 zeros), quando Inclusão ou Alteração."
"018-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data nascimento do beneficiário. Deve ser menor ou igual à data de realização do procedimento. Preenchida no formato AAAA-MM-DD."
"027-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data da solicitação.Deve ser preenchido em caso de autorização pela operadora para a realização do procedimento ou utilização do item assistencial. Preenchida no formato AAAA-MM-DD."
"028-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data da autorização.Data em que a autorização para realização do atendimento/procedimento foi concedida pela operadora. Preenchido no formato AAAA-MM-DD."
"029-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data de realização ou data inicial do período de atendimento.Data em que o atendimento/procedimento foi realizado ou data da internação.Deve ser anterior a data atual, menor ou igual a data de registro da transação. Preenchido no formato AAAA-MM-DD."
"030-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data início do faturamento. Deve ser preenchido para as cobranças parciais. Deve ser anterior a data atual, menor ou igual a data de registro da transação. Preenchido no formato AAAA-MM-DD."
"031-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data final do período de internação ou data do fim do faturamento.Deve ser anterior a data atual, menor ou igual a data de registro da transação. Preenchido no formato AAAA-MM-DD. A data é obrigatória informar em guias 3 (Resumo de Internação)."
"032-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data do protocolo de cobrança. Deve ser anterior a data atual, menor ou igual a data de registro da transação. Preenchido no formato AAAA-MM-DD."
"033-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data do pagamento. Esta data deve ser anterior à data atual e maior ou igual à data do protocolo da cobrança.Preenchido no formato AAAA-MM-DD."
"080-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data do processamento da guia. Deve ser anterior à data atual. Preenchido no formato AAAA-MM-DD."
"098-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data nascimento do beneficiário, contratação fornecimento direto. Deve ser anterior a data atual, menor ou igual a data de registro da transação."
"0102-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data do fornecimento, contratação fornecimento direto. Deve ser anterior à data atual, e seu ano e mês devem ser equivalentes ao ano e mês contidos no campo competencia dos dados. Preenchido no formado AAAA-MM-DD."
"0113-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Data do processamento, informação de outras despesas assistenciais. Deve ser anterior à data atual, e seu ano e mês devem ser equivalente ao ano e mês contidos no campo competencia dos dados."
"0119-1323","Motivo Glosa: DATA PREENCHIDA INCORRETAMENTE - Crítica ANS: Competência da cobertura contratada, contratação valor pré-estabelecido. Dever ser menor que o mê/ano da data atual e seu ano e mês dever ser equivalente ao ano e mês contidos no campo competencia dos dados. Preenchido no formato AAAA-MM-DD."
"039-1506","Motivo Glosa: TIPO DE INTERNAÇÃO INVÁLIDO - Crítica ANS: Preencher no tipo de guia 3 (internação) conforme opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 57."
"041-1509","Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Informar o CID Principal constante no CID 10. Não pode ser igual ao informado nos demais campos de CID.Somente pode ser informado em guia tipo 3."
"042-1509","Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Informar o CID Secundário constante no CID 10. Não pode ser igual ao informado nos demais campos de CID. Somente pode ser informado em guia tipo 3."
"043-1509","Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Informar o CID Terciário constante no CID 10. Não pode ser igual ao informado nos demais campos de CID. Somente pode ser informado em guia tipo 3."
"044-1509","Motivo Glosa: CÓDIGO CID INVÁLIDO - Crítica ANS: Informar o CID Quartiário constante no CID 10. Não pode ser igual ao informado nos demais campos de CID. Somente pode ser informado em guia tipo 3."
"045-1602","Motivo Glosa: TIPO DE ATENDIMENTO INVÁLIDO OU NÃO INFORMADO - Crítica ANS: Preencher em guia tipo 2 (SP/SADT) conforme as opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 50"
"034-1603","Motivo Glosa: TIPO DE CONSULTA INVÁLIDO - Crítica ANS: Preencher em guia tipo 1 (consulta) ou em guia tipo 2 (SP/SADT), e o tipo de atendimento for igual a 4, conforme as opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 52."
"050-1705","Motivo Glosa: VALOR APRESENTADO A MAIOR - Crítica ANS: Valor informado da guia. Deve ser igual ou maior que zero. Deve ser igual a soma dos valores informados de procedimentos ou itens assistenciais. Caso já exista o mesmo lançamento na base de dados da ANS, não pode ser maior que o valor informado anteriormente."
"050-1706","Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: Valor informado da guia. Deve ser igual ou maior que zero. Deve ser igual a soma dos valores informados de procedimentos ou itens assistenciais.Caso já exista o mesmo lançamento na base de dados da ANS, não pode ser maior que o valor informado anteriormente."
"0103-1706","Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: Valor total dos itens fornecidos, contratação fornecimento direto. Deve ser maior ou igual a soma do valor forneceido dos procedimentos/itens assistenciais informados no arquivo já existente na base de dados da ANS."
"0105-1706","Motivo Glosa: VALOR APRESENTADO A MENOR - Crítica ANS: Valor total de coparticipação, contratação fornecimento direto. Deve ser maior ou igual a soma do valor de coparticipação dos itens informados no lançamento."
"046-1713","Motivo Glosa: FATURAMENTO INVÁLIDO - Crítica ANS: Tipo de Faturamento. Preencher em tipo de guia 3 (internação) ou 4 (Odonto)opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 55"
"052-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago de procedimentos. Deve ser igual ou maior que zero. Valor total de todos os procedimentos realizados pago ao prestador executante ou reembolsado ao beneficiário."
"053-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago de diárias. Deve ser igual ou maior que zero. Valor total das diárias pago ao prestador executante ou reembolsado ao beneficiário."
"054-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago de taxas e aluguéis. Deve ser igual ou maior que zero. Valor total das taxas e aluguéis pago ao prestador executante ou reembolsado ao beneficiário."
"055-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago de materiais. Deve ser igual ou maior que zero. Valor total de materiais pago ao prestador executante ou reembolsado ao beneficiário."
"056-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago de OPME. Deve ser igual ou maior que zero. Valor total de OPME pago ao prestador executante ou reembolsado ao beneficiário."
"057-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago de medicamentos. Deve ser igual ou maior que zero. Valor total de medicamentos pago ao prestador executante ou reembolsado ao beneficiário."
"059-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago. Deve ser igual ou maior que zero. Valor total pago ao prestador executante ou reembolsado ao beneficiário."
"060-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor total pago pela operadora diretamente aos fornecedores. Deve ser igual ou maior que zero."
"061-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor pago quando realizado procedimento de tabela própria. Deve ser igual ou maior que zero."
"073-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor pago ao prestador executante ou reembolsado ao beneficiário. Deve ser igual ou maior que zero."
"074-1740","Motivo Glosa: ESTORNO DO VALOR DE PROCEDIMENTO PAGO - Crítica ANS: Valor pago diretamente ao fornecedor. Deve ser igual ou maior que zero."
"066-1801","Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do procedimento ou item assistencial. Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma individualizada, conforme tabela 64."
"078-1801","Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do procedimento realizado ou item assistencial que compõe o pacote, deve ser um código existente na TUSS."
"0108-1801","Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do procedimento, contratação fornecimento direto.Deve ser preenchido quando houver procedimentos/itens assistenciais constante na TUSS, que devem ser enviados de forma individualizada, conforme tabela 64, ou tratar-se de código próprio"
"064-1801","Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Tabela do procedimento ou item assistencial realizado.Deve ser preenchido com o código da tabela de referencia. Exemplo: 00, 18, 19, 20, 22, 90 ou 98."
"065-1801","Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do grupo do procedimento ou item assistencial.Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma consolidada, conforme tabela 64"
"066-1801","Motivo Glosa: PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do procedimento ou item assistencial. Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma individualizada, conforme tabela 64, ou quando tratar=se de código próprio"
"070-1806","Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: Quantidade informada de procedimentos ou itens assistenciais. Deve ser maior que zero."
"072-1806","Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: Se o valor pago do procedimento for maior que zero, a quantidade de procedimentos paga deve ser maior que zero."
"079-1806","Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: Quantidade de procedimentos ou itens assistenciais que compõe o pacote. Deve ser igual ou maior que zero."
"0109-1806","Motivo Glosa: QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO - Crítica ANS: Quantidade informada de itens assistenciais fornecido ao beneficiário. Deve maior que zero."
"066-2601","Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO - Crítica ANS: Código do procedimento ou item assistencial. A forma de envio do procedimento informado deve ser consolidado (informação na tabela TUSS 64)."
"078-2601","Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO - Crítica ANS: Código do procedimento realizado ou item assistencial que compõe o pacote. A forma de envio do procedimento informado deve ser consolidado (informação na tabela TUSS 64)."
"0108-2601","Motivo Glosa: CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO - Crítica ANS: Código do procedimento, contratação fornecimento direto.Deve ser preenchido quando houver procedimentos/itens assistenciais. A forma de envio do item informado deve ser consolidado (informação na tabela TUSS 64)."
"00-5001","Motivo Glosa: MENSAGEM ELETRÔNICA FORA DO PADRÃO TISS - Crítica ANS: Entende-se como erro de estrutura a informação de conteúdo no XML enviado pelas operadoras não previsto nos campos do XSD que define as terminologias da TUSS ou a falta de campos obrigatórios previstos neste mesmo XSD."
"00-5002","Motivo Glosa: NÃO FOI POSSÍVEL VALIDAR O ARQUIVO XML - Crítica ANS: Caso o formato de compactação do arquivo enviado seja diferente de ZIP ou o método de compressão seja diferente de DEFLATE. Caso a operadora envie um arquivo ZTE que não possua nenhum arquivo a ser descompactado."
"00-5014","Motivo Glosa: CÓDIGO HASH INVÁLIDO. MENSAGEM PODE ESTAR CORROMPIDA. - Crítica ANS: O código HASH não está de acordo com o conteúdo do arquivo, o arquivo pode ter sido alterado após ser gerado ou está corrompido."
"00-5016","Motivo Glosa: SEM NENHUMA OCORRÊNCIA DE MOVIMENTO DE INCLUSÃO NA COMPETÊNCIA PARA ENVIO A ANS - Crítica ANS: Este código informa a ANS que a operadora não teve nenhum movimento em determinada competencia. Este código é enviado pela operadora para a ANS."
"00-5017","Motivo Glosa: ARQUIVO PROCESSADO PELA ANS. - Crítica ANS: Este código no arquivo de retorno indica que o arquivo foi processado pela ANS."
"0102-5023","Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: Data do fornecimento, contratação fornecimento direto. Deve ser anterior à data atual."
"0113-5023","Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: Data do processamento, informação de outras despesas assistenciais. Deve ser anterior à data atual."
"0119-5023","Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: Competência da cobertura contratada, contratação valor pré-estabelecido. Quando o indicador do tipo de registro for igual a 1 (inclusão), deve ser igual à competencia dos dados."
"00-5023","Motivo Glosa: COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS - Crítica ANS: COMPETÊNCIA NÃO ESTÁ LIBERADA PARA ENVIO DE DADOS. A competência deve estar liberada pela ANS para ser recebida."
"03-5024","Motivo Glosa: OPERADORA INATIVA NA COMPETÊNCIA DOS DADOS - Crítica ANS: Competência do dado transmitido. A competência deve estar liberada pela ANS para ser recebida."
"04-5025","Motivo Glosa: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: Data de registro da transação. Deve ser menor ou igual à data atual.Preenchido no formato AAAA-MM-DD."
"080-5025","Motivo Glosa: DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: Data do processamento da guia. Deve ser anterior à data atual.Preenchido no formato AAAA-MM-DD."
"05-5026","Motivo Glosa: HORA DE REGISTRO DA TRANSAÇÃO INVÁLIDA - Crítica ANS: Hora de registro da transação (HH:MM:SS). Se data de registro da transação for igual à data atual, a hora de registro da transação deve ser menor ou igual à hora atual."
"06-5027","Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: Deve ser um registro existente no cadastro de operadoras da ANS, deverá ser equivalente à operadora que enviou o arquivo via PTA."
"081-5027","Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: Deve ser preenchido caso o atendimento tenha sido intermediado por operadora diferente da Operadora que enviou o arquivo (Intercâmbio, Reciprocidade)."
"092-5027","Motivo Glosa: REGISTRO ANS DA OPERADORA INVÁLIDO - Crítica ANS: Registro ANS da operadora intermediária, contratação valor pré-estabelecido. Deve ser preenchido caso o atendimento tenha sido intermediado por operadora diferente da Operadora que enviou o arquivo (Intercâmbio, Reciprocidade)."
"010-5028","Motivo Glosa: VERSÃO DO PADRÃO INVÁLIDA - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 69. Deverá ser informado a mesma versão para todos os lançamentos da guia."
"012-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número do lote. Deve ser diferente de brancos ou nulos."
"02-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: CNES. O campo deve ser preenchido com 7 dígitos, e vinculado ao CNPJ do Prestador Executante. Procurar no Datasus, ou caso não tenha o registro preencher com 9999999."
"013-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Tipo da identificação prestador executante, 1- CNPJ ou 2-CPF. Para guias tipo 3 - deverá ser preenchido com 1-CNPJ"
"014-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número de cadastro do prestador executante. Se CPF deverá conter 11 dígitos e se CNPJ 14 dígitos. O cadastro deve existir na base de dados da Receita Federal."
"015-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Município do Prestador Executante.Deve ser um código de município válido no IBGE."
"016-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número do CNS. Deve ser um número válido na base do Cartão Nacional de Saúde."
"017-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Sexo beneficiário. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 43"
"019-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Município de residência do beneficiário. Deve ser um código de município válido no IBGE."
"020-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número de registro ou código de plano válido do beneficiário, registrado na base de dados de planos da ANS."
"021-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Tipo de guia (tipo evento atenção). Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 54"
"022-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Origem da guia.Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 40"
"023-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número guia do Prestador.Preencher até 20 caracter. Quando a origem da guia for igual a 4-Reembolso ao beneficiário ou 5-Prestador eventual, o campo deve ser preenchido com (20 zeros)"
"024-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número guia do Operadora.Preencher até 20 caracter. Quando a origem da guia for igual a 4-Reembolso ao beneficiário ou 5-Prestador eventual, o campo deve ser preenchido com (20 zeros)"
"025-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número atribuído pela operadora para identificar o reembolso ao beneficiário ou o pagamento a prestador eventual, não pertencente à rede da operadora."
"035-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: CBO. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 24. Não pode ser informado 9999999."
"037-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Indicação de Acidente. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 36. Não deve ser preenchido quando os tipos de guia forem iguais a 4 - Tratamento Odontológico ou 5 - Honorários"
"038-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Caráter do atendimento. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 23"
"039-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Tipo de Internação. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 57"
"040-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Regime de Internação. Preencher em guia 3 (Resumo de Internação), conforme opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 41"
"045-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Tipo de Atendmento. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 50"
"046-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Tipo de Faturamento. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 55"
"062-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número da Declaração de Nascido Vivo. Preencher quando o Tipo de Internação for 3-Obstétrica onde tenha havido nascido vivo, e quando o tipo de guia for igua a 3 (Internação).Não pode haver repetição do mesmo número nas ocorrências deste campo."
"063-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Número da Declaração de Óbito. Preencher quando o Motivo de Encerramento for 41 ou 63 ou 65 ou 67, e quando o tipo de guia for igua a 3 (Resumo de Internação)."
"093-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Identificador de contratação por valor preestabelecido. Deve ser preenchido com o número atribuído pela operadora para identificar uma contratação por valor preestabelecido."
"097-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Sexo beneficiário, contratação fornecimento direto. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 43"
"0101-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Identificador da operação de fornecimento de materiais e medicamentos, contratação fornecimento direto. Número atribuído pela operadora para identificar a operação."
"0124-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Saúde ocupacional. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 77. Não deve ser preenchido quando a versão enviada pelo prestador à operadora for menor que 023 ou tipo de guia for igual a 3 (Internação), 4 (Tratamento Odontológico) ou 5 (Honorários)."
"0125-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Unidade de medida. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60."
"0129-5029","Motivo Glosa: INDICADOR INVÁLIDO - Crítica ANS: Unidade de medida de itens assistenciais que compõem o pacote.Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60."
"015-5030","Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: Município do Prestador Executante.Deve ser um código de município válido no IBGE."
"019-5030","Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: Município de residência do beneficiário. Deve ser um código de município válido no IBGE."
"091-5030","Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: Município do Prestador Executante, contratação valor pré-estabelecido. Deve ser um código de município válido no IBGE."
"099-5030","Motivo Glosa: CÓDIGO DO MUNICÍPIO INVÁLIDO - Crítica ANS: Município de residência do beneficiário, contratação fornecimento direto. Deve ser um código de município válido no IBGE."
"038-5031","Motivo Glosa: CARÁTER DE ATENDIMENTO INVÁLIDO - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 23. Não deve ser preenchido quando o tipo de guia for igual a 1 (Consulta), 4 (Tratamento Odontológico) ou 5 (Honorários)."
"036-5032","Motivo Glosa: INDICADOR DE RECÉM–NATO INVÁLIDO - Crítica ANS: Indica se o atendimento foi prestado a recém-nato. S para SIM e N para NÃO. Preencher tipo de guia 1 (consulta), 2 (SP/SADT) ou 3 (Internação)."
"049-5033","Motivo Glosa: MOTIVO DE ENCERRAMENTO INVÁLIDO - Crítica ANS: Preencher em tipo de guia 3 (Resumo de Intrernação) referente as opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 39"
"051-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor processado da guia. Deve ser igual ou maior que zero. Corresponde ao valor informado da guia menos o valor de glosa da guia."
"052-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago de procedimentos. Deve ser igual ou maior que zero. Valor total de todos os procedimentos realizados pago ao prestador executante ou reembolsado ao beneficiário."
"053-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago de diárias. Deve ser igual ou maior que zero. Valor total das diárias pago ao prestador executante ou reembolsado ao beneficiário."
"054-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago de taxas e aluguéis. Deve ser igual ou maior que zero. Valor total das taxas e aluguéis pago ao prestador executante ou reembolsado ao beneficiário."
"055-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago de materiais. Deve ser igual ou maior que zero. Valor total de materiais pago ao prestador executante ou reembolsado ao beneficiário."
"056-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago de OPME. Deve ser igual ou maior que zero. Valor total de OPME pago ao prestador executante ou reembolsado ao beneficiário."
"057-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago de medicamentos. Deve ser igual ou maior que zero. Valor total de medicamentos pago ao prestador executante ou reembolsado ao beneficiário."
"058-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total de glosa.Deve ser igual ou maior que zero."
"059-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago. Deve ser igual ou maior que zero. Valor total pago ao prestador executante ou reembolsado ao beneficiário."
"060-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago pela operadora diretamente aos fornecedores. Deve ser igual ou maior que zero."
"061-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor pago quando realizado procedimento de tabela própria. Deve ser igual ou maior que zero."
"063-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Número da Declaração de Óbito. Preencher quando o Motivo de Encerramento for 41 ou 63 ou 65 ou 67, e quando o tipo de guia for igua a 3 (Resumo de Internação)."
"071-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor informado de procedimentos ou itens assistenciais. Deve ser igual ou maior que zero. Tem que ser o mesmo para todos os lançamentos do mesmo procedimento ou item assistencial, na mesma guia."
"072-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Quantidade paga de procedimentos ou itens assistenciais. Deve ser igual ou maior que zero."
"073-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor pago ao prestador executante ou reembolsado ao beneficiário. Deve ser igual ou maior que zero, se a a quantidade informada for maior que zero."
"074-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor pago diretamente ao fornecedor. Deve ser igual ou maior que zero."
"084-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total de coparticipação. Dever ser maior ou igual a zero. Quando internado deve ser preenchido com zero."
"085-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor de coparticipação. Deve ser maior ou igual a zero. Quando internado deve ser preenchido com zero."
"094-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor da cobertura contratada na competência, contratação valor pré-estabelecido. Deve ser maior do que zero."
"0103-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total dos itens fornecidos, contratação fornecimento direto. Deve ser maior ou igual a zero."
"0104-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total em tabela própria da operadora, contratação fornecimento direto. Deve ser maior ou igual a zero."
"0105-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total de coparticipação, contratação fornecimento direto. Deve ser maior ou igual a zero."
"0111-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor de coparticipação, contratação fornecimento direto. Deve ser maior ou igual a zero."
"0117-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total de glosa, informação de outras despesas assistenciais. Deve ser maior ou igual a zero."
"0118-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor total pago, informação de outras despesas assistenciais. Deve ser maior ou igual a zero."
"0128-5034","Motivo Glosa: VALOR NÃO INFORMADO - Crítica ANS: Valor informado do modelo de remuneração .Deve ser maior ou igual a zero."
"064-5035","Motivo Glosa: CÓDIGO DA TABELA DE REFERÊNCIA NÃO INFORMADO - Crítica ANS: Tabela do procedimento ou item assistencial realizado.Deve ser preenchido com o código da tabela de referencia. Exemplo: 00, 18, 19, 20, 22, 90 ou 98."
"065-5036","Motivo Glosa: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do grupo do procedimento ou item assistencial.Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma consolidada, conforme tabela 64."
"0107-5036","Motivo Glosa: CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO - Crítica ANS: Código do grupo do procedimento ou item assistencial, contratação fornecimento direto. Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma consolidada, conforme tabela 64"
"069-5039","Motivo Glosa: CÓDIGO DA FACE DO DENTE INVÁLIDO - Crítica ANS: Identificação da face do dente. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 32. Não pode ser informado em guia diferente de 4 (Odonto)."
"0110-5040","Motivo Glosa: VALOR DEVE SER MAIOR QUE ZERO - Crítica ANS: Valor do item assistencial fornecido ao beneficiário. Deve ser maior que zero."
"0116-5040","Motivo Glosa: VALOR DEVE SER MAIOR QUE ZERO - Crítica ANS: Valor total informado, informação de outras despesas assistenciais. Deve ser maior que zero."
"050-5042","Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: Valor informado da guia. Deve ser igual ou maior que zero. Deve ser igual a soma dos valores informados de procedimentos ou itens assistenciais."
"059-5042","Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: Valor total pago. Deve ser igual ou maior que zero. Valor total pago ao prestador executante ou reembolsado ao beneficiário."
"060-5042","Motivo Glosa: VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS - Crítica ANS: Valor total pago pela operadora diretamente aos fornecedores. Deve ser igual ou maior que zero."
"00-5044","Motivo Glosa: JÁ EXISTEM INFORMAÇÕES NA ANS PARA A COMPETÊNCIA INFORMADA. - Crítica ANS: A operadora está enviando um arquivo sem movimento (5016) e já existem lançamentos para a competência na base de dados da ANS."
"0119-5045","Motivo Glosa: COMPETENCIA ANTERIOR NÃO ENVIADA - Crítica ANS: Acontece quando há uma lacuna (falta de informação) entre a competência enviada e a última incorporada ao banco de dados da ANS."
"00-5045","Motivo Glosa: COMPETENCIA ANTERIOR NÃO ENVIADA - Crítica ANS: A competência anterior a que está sendo enviada não contem nenhum registro ativo na base de dados da ANS."
"00-5046","Motivo Glosa: COMPETÊNCIA INVÁLIDA - Crítica ANS: COMPETÊNCIA INVÁLIDA"
"082-5050","Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Identificador de contratação por valor preestabelecido. Deve ser preenchido com o número atribuído pela operadora para identificar uma contratação por valor preestabelecido."
"084-5050","Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Valor total de coparticipação. Dever ser maior ou igual a zero. Quando internado deve ser preenchido com zeros."
"085-5050","Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Valor de coparticipação. Deve ser maior ou igual a zero. Quando internado deve ser preenchido com zeros."
"0103-5050","Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Valor total dos itens fornecidos, contratação fornecimento direto. Deve ser maior ou igual a zero."
"0105-5050","Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Valor total de coparticipação, contratação fornecimento direto. Deve ser maior ou igual a zero."
"0128-5050","Motivo Glosa: VALOR INFORMADO INVÁLIDO - Crítica ANS: Valor informado do modelo de remuneração .Deve ser maior ou igual a zero."
"082-5052","Motivo Glosa: IDENTIFICADOR INEXISTENTE - Crítica ANS: Identificador de contratação por valor preestabelecido. Deve ser preenchido com o número atribuído pela operadora para identificar uma contratação por valor preestabelecido."
"064-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Tabela do procedimento ou item assistencial realizado.Deve ser preenchido com o código da tabela de referencia. Exemplo: 00, 18, 19, 20, 22, 90 ou 98."
"065-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Código do grupo do procedimento ou item assistencial.Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma consolidada, conforme tabela 64"
"077-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Tabela de referência do procedimento ou item assistencial que compõe o pacote. Quando informado a tabela do procedimento ou item assistencial realizado for igual a 90 ou 98."
"078-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Código do procedimento realizado ou item assistencial que compõe o pacote. Quando informado a tabela 90 ou 98, deve ser um código existente na TUSS."
"093-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Identificador de contratação por valor preestabelecido. Deve ser preenchido com o número atribuído pela operadora para identificar uma contratação por valor preestabelecido."
"0101-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Identificador da operação de fornecimento de materiais e medicamentos, contratação fornecimento direto. Número atribuído pela operadora para identificar a operação."
"0106-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Tabela de referência do item assistencial fornecido, contratação fornecimento direto. Deve ser preenchido com o código da tabela de referencia. Exemplo: 00, 18, 19, 20, 22, 90 ou 98."
"0107-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Código do grupo do procedimento ou item assistencial, contratação fornecimento direto. Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma consolidada, conforme tabela 64"
"0108-5053","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO - Crítica ANS: Código do item, contratação fornecimento direto.Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma individualizada, conforme tabela 64, ou tratar-se de código próprio"
"093-5054","Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Identificador de contratação por valor preestabelecido. Deve ser preenchido com o número atribuído pela operadora para identificar uma contratação por valor preestabelecido."
"0101-5054","Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Identificador da operação de fornecimento de materiais e medicamentos, contratação fornecimento direto. Número atribuído pela operadora para identificar a operação."
"0106-5054","Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Tabela de referência do item assistencial fornecido, contratação fornecimento direto. Deve ser preenchido com o código da tabela de referencia. Exemplo: 00, 18, 19, 20, 22, 90 ou 98."
"0107-5054","Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Código do grupo do procedimento ou item assistencial, contratação fornecimento direto. Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma consolidada, conforme tabela 64"
"0108-5054","Motivo Glosa: IDENTIFICADOR NÃO ENCONTRADO - Crítica ANS: Código do procedimento, contratação fornecimento direto.Deve ser preenchido quando houver procedimentos/itens assistenciais que devem ser enviados de forma individualizada, conforme tabela 64, ou tratar-se de código próprio"
"0115-5055","Motivo Glosa: IDENTIFICADOR JÁ INFORMADO NA COMPETÊNCIA - Crítica ANS: Número de cadastro do recebedor, informação de outras despesas assistenciais. Se CPF deverá conter 11 dígitos e se CNPJ 14 dígitos. O cadastro deve existir na base de dados da Receita Federal. Acontece a rejeição quando é enviado um lançamento de outras formas de remuneração que já conste na base de dados da ANS"
"0115-5056","Motivo Glosa: IDENTIFICADOR NÃO INFORMADO NA COMPETÊNCIA - Crítica ANS: Número de cadastro do recebedor, informação de outras despesas assistenciais. Se CPF deverá conter 11 dígitos e se CNPJ 14 dígitos. O cadastro deve existir na base de dados da Receita Federal. Acontece a rejeição quando é enviado um lançamento de alteração ou exclusão de outras formas de remuneração que não conste na base de dados da ANS."
"093-5059","Motivo Glosa: EXCLUSÃO INVÁLIDA – EXISTEM LANÇAMENTOS VINCULADOS A ESTA FORMA DE CONTRATAÇÃO - Crítica ANS: Identificador de contratação por valor preestabelecido. Deve ser preenchido com o número atribuído pela operadora para identificar uma contratação por valor preestabelecido. Não é possível excluir um registro de identificação de valor pré-estabelecido enquanto houver lançamentos vinculados a ele."
"0120-5061","Motivo Glosa: TIPO DE ATENDIMENTO OPERADORA INTERMEDIÁRIA NÃO INFORMADO - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 71."
"081-5062","Motivo Glosa: REGISTRO ANS DA OPERADORA INTERMEDIÁRIA NÃO INFORMADO - Crítica ANS: Deve ser preenchido caso o atendimento tenha sido intermediado por operadora diferente da enviada o arquivo (Intercâmbio, Reciprocidade)."
"012-5063","Motivo Glosa: PAR CNPJ X CNES NAO ENCONTRADO NA BASE DO CNES - Crítica ANS: O campo deve ser preenchido com 7 dígitos, e vinculado ao CNPJ do Prestador Executante. Procurar no Datasus, ou caso não tenha o registro preencher com 9999999."
"012-5064","Motivo Glosa: TIPO DE ESTABELECIMENTO NO CNES NÃO É APTO PARA INTERNAÇÃO - Crítica ANS: Quando o tipo de guia for igual a 3( Internação), o CNES for diferente de '9999999' e o tipo de prestador for pessoa jurídica, o tipo do estabelecimento do prestador, conforme registro na base do CNES, deve ser apto a realizar internações"
"014-5065","Motivo Glosa: TIPO DE ATIVIDADE ECONOMICA DO CNPJ NÃO É APTO PARA INTERNAÇÃO - Crítica ANS: Quando o tipo de guia for igual a 3 – Internação e o tipo de prestador for pessoa jurídica, o tipo de atividade econômica (CNAE) do prestador, conforme registro na base de pessoas jurídicas da Receita Federal, deve ser apto a realizar internações."
"062-5066","Motivo Glosa: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE - Crítica ANS: Número da Declaração de Nascido Vivo. Preencher quando o Tipo de Internação for 3-Obstétrica onde tenha havido nascido vivo, e quando o tipo de guia for igua a 3 (Internação).Não pode haver repetição do mesmo número nas ocorrências deste campo."
"063-5066","Motivo Glosa: NÚMERO DA DECLARAÇÃO EM DUPLICIDADE - Crítica ANS: Número da Declaração de Óbito. Preencher quando o Motivo de Encerramento for 41 ou 63 ou 65 ou 67, e quando o tipo de guia for igua a 3 (Resumo de Internação). Não pode haver repetição do mesmo número nas ocorrências deste campo."
"042-5067","Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: Informar o CID Secundário constante no CID 10. Não pode ser igual ao informado nos demais campos de CID. Somente pode ser informado em guia tipo 3 (Internação)."
"043-5067","Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: Informar o CID Terciário constante no CID 10. Não pode ser igual ao informado nos demais campos de CID. Somente pode ser informado em guia tipo 3 (Inernação)."
"044-5067","Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: Informar o CID Quartiário constante no CID 10. Não pode ser igual ao informado nos demais campos de CID. Somente pode ser informado em guia tipo 3 (Internação)."
"041-5067","Motivo Glosa: DIAGNÓSTICO EM DUPLICIDADE - Crítica ANS: Informar o CID Principal constante no CID 10. Não pode ser igual ao informado nos demais campos de CID.Somente pode ser informado em guia tipo 3 (Internação)."
"063-5068","Motivo Glosa: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Número da Declaração de Óbito. Preencher quando o Motivo de Encerramento for 41 ou 63 ou 65 ou 67, e quando o tipo de guia for igua a 3 (Resumo de Internação). Não pode ser diferente do lançamento anterior."
"062-5068","Motivo Glosa: DECLARAÇÃO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Número da Declaração de Nascido Vivo. Preencher quando o Tipo de Internação for 3-Obstétrica onde tenha havido nascido vivo, e quando o tipo de guia for igua a 3 (Internação). Não pode ser diferente do lançamento anterior."
"067-5069","Motivo Glosa: CÓDIGO DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Identificação do dente. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 28. Não pode ser diferente do lançamento anterior."
"068-5070","Motivo Glosa: CÓDIGO DA REGIÃO DA BOCA DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Identificação da região da boca. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 42.Não pode ser diferente do lançamento anterior."
"069-5071","Motivo Glosa: CÓDIGO DA FACE DO DENTE DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Identificação da face do dente. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 32. Não pode ser diferente do lançamento anterior."
"071-5072","Motivo Glosa: VALOR INFORMADO DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Valor informado de procedimentos ou itens assistenciais. Deve ser igual ou maior que zero. Tem que ser o mesmo para todos os lançamentos do mesmo procedimento ou item assistencial, na mesma guia. Não pode ser diferente do lançamento anterior."
"023-5073","Motivo Glosa: O PRIMEIRO LANCAMENTO DA GUIA SÓ PODE SER EXCLUIDO SE ELE FOR O ÚNICO - Crítica ANS: Número guia do Prestador.Preencher até 20 caracter. Caso seja enviado uma exclusão do primeiro lançamento de uma guia e já existam outros lançamentos desta guia no banco de dados, este lançamento será rejeitado."
"0123-5074","Motivo Glosa: REGIME DE ATENDIMENTO INVÁLIDO - Crítica ANS: Preencher tipo de guia 1 (Consulta) ou 2 (SP/SADT) conforme opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 76"
"041-5075","Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Informar o CID Principal constante no CID 10, informado diferente do lançamento anterior da guia. Somente pode ser informado em guia tipo 3 (Internação)."
"042-5075","Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Informar o CID Secundário constante no CID 10, informado diferente do lançamento anterior da guia.Somente pode ser informado em guia tipo 3 (Internação)."
"043-5075","Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Informar o CID Terciário constante no CID 10, informado diferente do lançamento anterior da guia. Somente pode ser informado em guia tipo 3 (Inernação)."
"044-5075","Motivo Glosa: CID DIFERENTE DO LANÇAMENTO ANTERIOR DA GUIA - Crítica ANS: Informar o CID Quartiário constante no CID 10, , informado diferente do lançamento anterior da guia. Somente pode ser informado em guia tipo 3 (Internação)."
"0121-5076","Motivo Glosa: CPF NÃO ENCONTRADO NA RECEITA FEDERAL - Crítica ANS: Número do CPF do beneficiário. Código deve existir na base de dados da Receita Federal, e deverá conter 11 caracter."
"0126-5076","Motivo Glosa: CPF NÃO ENCONTRADO NA RECEITA FEDERAL - Crítica ANS: Número do CPF do beneficiário, contratação fornecimento direto. Código deve existir na base de dados da Receita Federal, e deverá conter 11 caracter."
"017-5077","Motivo Glosa: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - Crítica ANS: Sexo beneficiário. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 43"
"097-5077","Motivo Glosa: SEXO NA RECEITA FEDERAL DIFERENTE DO INFORMADO PARA O CPF - Crítica ANS: Sexo beneficiário, contratação fornecimento direto. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 43"
"018-5078","Motivo Glosa: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - Crítica ANS: Data nascimento do beneficiário. Deve ser menor ou igual à data de realização do procedimento."
"098-5078","Motivo Glosa: DATA DE NASCIMENTO NA RECEITA FEDERAL DIFERENTE DA INFORMADA PARA O CPF - Crítica ANS: Data nascimento do beneficiário, contratação fornecimento direto. Deve ser menor ou igual à data de realização do fornecimento."
"0122-5079","Motivo Glosa: MODELO DE REMUNERAÇÃO EM DUPLICIDADE - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 79, não deve haver repetição do mesmo código no campo modelo de remuneração."
"0122-5080","Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO INFORMADO - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 79."
"0122-5081","Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REEMBOLSO/PRESTADOR EVENTUAL - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 79. Não deve ser preenchido se a origem da guia foi 4 (Reembolso) ou 5 (Prestador eventual)."
"0122-5082","Motivo Glosa: MODELO DE REMUNERAÇÃO NÃO DEVE SER INFORMADO PARA REDE PRÓPRIA COM MESMO CNPJ - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 79. Não deve ser preenchido nos atendimentos em rede própria de mesmo CNPJ."
"050-5083","Motivo Glosa: SOMA DOS VALORES DOS MODELOS DE REMUNERAÇÃO DIFERENTE DO VALOR INFORMADO DA GUIA - Crítica ANS: Valor informado da guia. Deve ser igual ou maior que zero. Deve ser igual a soma dos valores informados de procedimentos ou itens assistenciais na guia."
"0125-5084","Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60. Não se aplica às tabelas TUSS 22, 63, 90 ou 98."
"0127-5084","Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: Unidade de medida, contratação fornecimento direto. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60. Não se aplica às tabelas TUSS 22, 63, 90 ou 98."
"0129-5084","Motivo Glosa: UNIDADE DE MEDIDA NÃO DEVE SER PREENCHIDA PARA A TABELA TUSS INFORMADA - Crítica ANS: Unidade de medida de itens assistenciais que compõem o pacote.Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60.Não se aplica às tabelas TUSS 22"
"0125-5085","Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: Unidade de medida. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60. É obrigatório para itens da TUSS de Medicamentos (Tabela 20)."
"0127-5085","Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: Unidade de medida, contratação fornecimento direto. Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60. É obrigatório para itens da TUSS de Medicamentos (Tabela 20)."
"0129-5085","Motivo Glosa: UNIDADE DE MEDIDA É OBRIGATÓRIA PARA A TABELA TUSS INFORMADA - Crítica ANS: Unidade de medida de itens assistenciais que compõem o pacote.Preencher opções do Componente de Representação de Conceitos em Saúde - TUSS Demais Terminologias - Tabela 60. É obrigatório para itens da TUSS de Medicamentos (Tabela 20)."
"016-5086","Motivo Glosa: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - Crítica ANS: Número do CNS. Deve ser um número válido na base do Cartão Nacional de Saúde, sexo e data de nascimento devem ser iguais às informadas no SIB."
"096-5086","Motivo Glosa: DADO DIVERGENTE COM A RECEITA FEDERAL PARA ESTE CNS (CPF/SEXO/DATA NASCIMENTO) - Crítica ANS: Número do CNS, contratação fornecimento direto. Deve ser um número válido na base do Cartão Nacional de Saúde, sexo e data de nascimento devem ser iguais às informadas no SIB."
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





