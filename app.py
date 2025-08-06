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
import pytz # Importar pytz

#================================================================================
# DADOS INTERNOS
#================================================================================

def carregar_erros_ans():
    """
    Carrega a planilha de erros da ANS que agora está embutida no código.
    """
    csv_data = """Código do Termo,Termo
1001,BENEFICIÁRIO INEXISTENTE
1002,BENEFICIÁRIO INATIVO
1004,NÚMERO DA CARTEIRA INVÁLIDO
1005,NÚMERO DA CARTEIRA EM DUPLICIDADE NO SISTEMA
1006,VÍNCULO DO BENEFICIÁRIO COM A EMPRESA INEXISTENTE
1007,VÍNCULO DO BENEFICIÁRIO COM A EMPRESA INATIVO
1008,BENEFICIÁRIO COM O CONTRATO SUSPENSO
1009,BENEFICIÁRIO COM A CARÊNCIA NÃO CUMPRIDA
1010,BENEFICIÁRIO FALECIDO
1011,BENEFICIÁRIO NÃO PERTENCE A REDE DE ATENDIMENTO
1012,NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO OU NÃO INFORMADO
1013,ATENDIMENTO FORA DA VIGÊNCIA DO CONTRATO
1014,DIVERGÊNCIA NO NOME DO BENEFICIÁRIO
1015,BENEFICIÁRIO EM ATENDIMENTO DE CPT
1016,BENEFICIÁRIO EM ATENDIMENTO DE CPT E NÃO FAZ JUS AO ATENDIMENTO
1017,PLANO DO BENEFICIÁRIO NÃO CONTRATUALIZADO PELO PRESTADOR
1018,PLANO DO BENEFICIÁRIO INATIVO
1019,PLANO DO BENEFICIÁRIO NÃO COBRE O PROCEDIMENTO
1020,BENEFICIÁRIO SEM DIREITO A REEMBOLSO
1021,PLANO DO BENEFICIÁRIO NÃO PERMITE ATENDIMENTO FORA DA REDE CREDENCIADA
1022,TIPO DE DOENÇA OU TEMPO DE DOENÇA NÃO COBERTO PELO PLANO
1023,BENEFICIÁRIO COM RESTRIÇÃO CONTRATUAL AO ATENDIMENTO
1101,TIPO DE GUIA INVÁLIDA
1102,VERSÃO DO PADRÃO TISS INVÁLIDA
1103,ASSINATURA DIGITAL NÃO CONFERE
1104,REAPRESENTAÇÃO DE GUIA
1201,DATA DE EMISSÃO DA GUIA INVÁLIDA
1202,DATA DE VALIDADE DA CARTEIRA INVÁLIDA
1203,DATA DE VALIDADE DA CARTEIRA VENCIDA
1204,CARÁTER DO ATENDIMENTO INVÁLIDO
1205,TIPO DE ATENDIMENTO INVÁLIDO
1206,DATA DE INÍCIO DO FATURAMENTO INVÁLIDA
1207,DATA FINAL DO FATURAMENTO INVÁLIDA
1208,DATA DE NASCIMENTO INVÁLIDA
1209,DATA DE VALIDADE DA SENHA INVÁLIDA
1210,DATA DA REALIZAÇÃO DO PROCEDIMENTO FORA DO PRAZO DE VALIDADE DA GUIA
1301,NÚMERO DA GUIA PRINCIPAL NÃO INFORMADO E/OU INVÁLIDO
1302,CÓDIGO TIPO GUIA PRINCIPAL E NÚMERO GUIAS INCOMPATÍVEIS
1303,NÃO EXISTE O NUMERO GUIA PRINCIPAL INFORMADO
1304,COBRANÇA EM GUIA INDEVIDA
1305,ITEM PAGO EM OUTRA GUIA
1306,NÃO EXISTE NUMERO GUIA PRINCIPAL E/OU CÓDIGO GUIA PRINCIPAL
1307,NUMERO DA GUIA INVALIDO
1308,GUIA JA APRESENTADA
1309,PROCEDIMENTO CONTRATADO NÃO ESTÁ DE ACORDO COM O TIPO DE GUIA UTILIZADO
1310,SERVIÇO DO TIPO CIRÚRGICO E INVASIVO. EQUIPE MEDICA NÃO INFORMADA NA GUIA
1311,PRESTADOR EXECUTANTE NÃO INFORMADO
1312,PRESTADOR CONTRATADO NÃO INFORMADO
1313,GUIA COM RASURA
1314,GUIA SEM ASSINATURA E/OU CARIMBO DO CREDENCIADO.
1315,GUIA SEM DATA DO ATO CIRURGICO.
1316,GUIA COM LOCAL DE ATENDIMENTO PREENCHIDO INCORRETAMENTE
1317,GUIA SEM DATA DO ATENDIMENTO
1318,GUIA COM CÓDIGO DE SERVIÇO PREENCHIDO INCORRETAMENTE.
1319,GUIA SEM ASSINATURA DO ASSISTIDO.
1320,IDENTIFICAÇÃO DO ASSISTIDO INCOMPLETA
1321,VALIDADE DA GUIA EXPIRADA
1322,COMPROVANTE PRESENCIAL OU GTO NÃO ENVIADO
1323,DATA PREENCHIDA INCORRETAMENTE
1401,ACOMODAÇÃO NÃO AUTORIZADA
1402,DIÁRIAS EXCEDENTES AO AUTORIZADO
1403,DIÁRIAS NÃO AUTORIZADAS
1404,PROCEDIMENTO CONTRATADO SEM COBERTURA PARA DIÁRIA DE ACOMPANHANTE
1405,DIÁRIA DE ACOMPANHANTE NÃO AUTORIZADA
1406,DIÁRIA DE UTI NÃO AUTORIZADA
1407,INTERNAÇÃO NÃO AUTORIZADA
1408,INTERNAÇÃO ELETIVA SEM AUTORIZAÇÃO PRÉVIA
1409,AUTORIZAÇÃO INEXISTENTE
1410,NÚMERO DA SENHA INVÁLIDO
1411,SENHA VENCIDA
1412,AUTORIZAÇÃO NEGADA
1413,SERVIÇO SOLICITADO NÃO CONFERE COM O AUTORIZADO
1414,SOLICITAÇÃO DE EXAMES E/OU PROCEDIMENTOS SEM O PEDIDO DO MÉDICO ASSISTENTE.
1415,EXAMES E/OU PROCEDIMENTOS NÃO CORRESPONDEM A INDICAÇÃO CLÍNICA.
1416,PROCEDIMENTO NÃO AUTORIZADO
1501,CÓDIGO DO PRESTADOR INVÁLIDO
1502,PRESTADOR NÃO HABILITADO PARA O PROCEDIMENTO
1503,PRESTADOR INATIVO
1504,PRESTADOR EXECUTANTE NÃO PERTENCE A REDE DO PLANO
1505,PRESTADOR SOLICITANTE NÃO PERTENCE A REDE DO PLANO
1506,PRESTADOR NÃO PERTENCE A REDE DE ATENDIMENTO
1507,CÓDIGO DE IDENTIFICAÇÃO DO PRESTADOR INVÁLIDO (CNPJ/CPF)
1508,NOME DO PRESTADOR INVÁLIDO
1509,CÓDIGO DO CONSELHO PROFISSIONAL INVÁLIDO
1510,NÚMERO NO CONSELHO PROFISSIONAL INVÁLIDO
1511,UF DO CONSELHO PROFISSIONAL INVÁLIDA
1512,CÓDIGO BRASILEIRO DE OCUPAÇÕES (CBO) INVÁLIDO OU INCOMPATÍVEL
1513,PRESTADOR NÃO INDICADO NA GUIA
1514,PRESTADOR NÃO IDENTIFICADO
1515,PRESTADOR EXECUTANTE NÃO INFORMADO NA GUIA
1516,NOME DO PROFISSIONAL EXECUTANTE INVÁLIDO
1517,GRAU DE PARTICIPAÇÃO INVÁLIDO
1601,CÓDIGO DO PROCEDIMENTO INVÁLIDO
1602,PROCEDIMENTO INCOMPATÍVEL COM O SEXO
1603,PROCEDIMENTO INCOMPATÍVEL COM A IDADE
1604,PROCEDIMENTO REQUER CID
1605,PROCEDIMENTO SEM COBERTURA CONTRATUAL
1606,PROCEDimento EXIGE AUTORIZAÇÃO E A GUIA NÃO POSSUI
1607,PROCEDIMENTO EXIGE LAUDO TÉCNICO E O MESMO NÃO CONSTA NA GUIA
1608,PROCEDIMENTO REALIZADO EM QUANTIDADE SUPERIOR A PERMITIDA
1609,PROCEDIMENTO REALIZADO EM LOCAL IMPRÓPRIO
1610,O PROCEDIMENTO EXIGE FILME E O MESMO NÃO FOI APRESENTADO.
1611,PROCEDIMENTO INCOMPATIVEL COM O CARÁTER DO ATENDIMENTO
1612,HORA INICIAL E/OU FINAL INVÁLIDA
1613,TEMPO DE DURAÇÃO DO PROCEDIMENTO INCOMPATÍVEL
1614,PROCEDIMENTO INCOMPATÍVEL COM A ESPECIALIDADE DO PROFISSIONAL
1615,VIA DE ACESSO INCOMPATÍVEL
1616,TÉCNICA UTILIZADA INCOMPATÍVEL
1617,PROCEDIMENTO EXIGE INTERNAÇÃO
1618,PROCEDIMENTO EXCLUSIVO PARA INTERNAÇÃO
1619,PROCEDIMENTO EXIGE PLANO ODONTOLÓGICO
1620,PROCEDIMENTO EXIGE PLANO MÉDICO-HOSPITALAR
1621,PROCEDIMENTO PAGO EM OUTRA GUIA
1622,CÓDIGO DE PROCEDIMENTO INEXISTENTE NA TABELA DE DOMÍNIO
1623,PROCEDIMENTO INCOMPATÍVEL COM O TIPO DE ATENDIMENTO
1624,PROCEDIMENTO REQUER EQUIPE CIRURGICA
1625,PROCEDIMENTO INCOMPATÍVEL COM O REGIME DE INTERNAÇÃO
1626,PROCEDIMENTO INCOMPATÍVEL COM O TIPO DE CONSULTA
1627,PROCEDIMENTO INCOMPATÍVEL COM TIPO DE GUIA
1701,VALOR DO PROCEDIMENTO INVÁLIDO
1702,VALOR TOTAL DA GUIA INVÁLIDO
1703,VALOR APRESENTADO EXCEDE O VALOR PACTUADO
1704,VALOR DE CO-PARTICIPAÇÃO INVÁLIDO
1705,VALOR DE FRANQUIA INVÁLIDO
1706,VALOR PAGO PELO PACIENTE INVÁLIDO
1707,REDUÇÃO/ACRÉSCIMO INVÁLIDO
1708,VALORES NÃO DISCRIMINADOS
1709,VALOR TOTAL NÃO CONFERE COM A SOMA DOS ITENS
1710,VALORES APRESENTADOS NA GUIA NÃO CORRESPONDEM AO AUTORIZADO
1711,TABELA DE PREÇOS INVÁLIDA
1801,CÓDIGO DE MATERIAL/MEDICAMENTO/OPME/TAXA INVÁLIDO
1802,QUANTIDADE DE MATERIAL/MEDICAMENTO/OPME/TAXA INVÁLIDA
1803,VALOR DE MATERIAL/MEDICAMENTO/OPME/TAXA INVÁLIDO
1804,CÓDIGO DA TABELA DE PREÇO DO MATERIAL/MEDICAMENTO/OPME/TAXA INVÁLIDO
1805,UNIDADE DE MEDIDA DO MATERIAL/MEDICAMENTO/OPME/TAXA INVÁLIDA
1806,MATERIAL/MEDICAMENTO/OPME/TAXA SEM COBERTURA CONTRATUAL
1807,MATERIAL/MEDICAMENTO/OPME/TAXA INCOMPATÍVEL COM O PROCEDIMENTO
1808,MATERIAL/MEDICAMENTO/OPME/TAXA INCOMPATÍVEL COM O SEXO
1809,MATERIAL/MEDICAMENTO/OPME/TAXA INCOMPATÍVEL COM A IDADE
1810,MATERIAL/MEDICAMENTO/OPME/TAXA NÃO AUTORIZADO
1811,MATERIAL/MEDICAMENTO/OPME/TAXA COM PREÇO ACIMA DO NEGOCIADO
1812,NOTA FISCAL DO MATERIAL/MEDICAMENTO/OPME/TAXA NÃO APRESENTADA
1813,NOTA FISCAL DO MATERIAL/MEDICAMENTO/OPME/TAXA INVÁLIDA
1814,LOTE DO MATERIAL/MEDICAMENTO/OPME/TAXA NÃO INFORMADO OU INVÁLIDO
1815,REGISTRO ANVISA DO MATERIAL/MEDICAMENTO/OPME/TAXA NÃO INFORMADO OU INVÁLIDO
1816,CÓDIGO DE BARRAS DO MATERIAL/MEDICAMENTO/OPME/TAXA NÃO INFORMADO OU INVÁLIDO
1817,MATERIAL/MEDICAMENTO/OPME/TAXA PAGO EM OUTRA GUIA
1818,CÓDIGO DO TIPO DE GUIA INCOMPATÍVEL COM A COBRANÇA
1901,CID INVÁLIDO
1902,CID INCOMPATÍVEL COM O PROCEDIMENTO
1903,CID INCOMPATÍVEL COM O SEXO
1904,CID INCOMPATÍVEL COM A IDADE
1905,CID EXIGE LAUDO TÉCNICO
1906,CID EXIGE AUTORIZAÇÃO
1907,CID INCOMPATÍVEL COM O CARÁTER DO ATENDIMENTO
1908,CID NÃO INFORMADO
2001,TIPO DE INTERNAÇÃO INVÁLIDO
2002,REGIME DE INTERNAÇÃO INVÁLIDO
2003,TIPO DE FATURAMENTO INVÁLIDO
2004,MOTIVO DE ENCERRAMENTO DO ATENDIMENTO INVÁLIDO
2005,TIPO DE CONSULTA INVÁLIDO
5001,ARQUIVO XML FORA DO PADRÃO TISS
5002,ERRO NA VALIDAÇÃO DO HASH
5003,NÚMERO DO LOTE INVÁLIDO
5004,LOTE JÁ ENVIADO
5005,PROTOCOLO NÃO POSSUI LOTE ASSOCIADO
5006,LOTE EM PROCESSAMENTO
5007,PROTOCOLO INEXISTENTE
5008,FALHA NO ACESSO AS BASES DE DADOS
5009,PRESTADOR NÃO POSSUI AUTORIZAÇÃO PARA ENVIO
5010,VERSÃO DO PADRÃO NÃO SUPORTADA
5011,ARQUIVO COM MAIS DE 10MB
5012,ERRO INESPERADO
5013,REGISTRO ANS DA OPERADORA INCONSISTENTE
5014,COMPETÊNCIA INVÁLIDA OU INEXISTENTE
5015,LOTE DE PROTOCOLO DE SOLICITAÇÃO NÃO FOI RECEBIDO
5016,LOTE CONTENDO MAIS DE 100 GUIAS
5017,GUIA COM MAIS DE 200 PROCEDIMENTOS E/OU ITENS ASSISTENCIAIS"""
    
    data_file = io.StringIO(csv_data)
    df = pd.read_csv(data_file)
    return df

#================================================================================
# FUNÇÕES DE PARSE (FASE 1 e FASE 2)
#================================================================================

@st.cache_data
def parse_xte(file):
    # Esta função não foi alterada
    file.seek(0)
    content = file.read().decode('iso-8859-1')
    tree = ET.ElementTree(ET.fromstring(content))
    root = tree.getroot()
    ns = {'ans': 'http://www.ans.gov.br/padroes/tiss/schemas'}
    all_data = []
    
    cabecalho_info = {}
    cabecalho = root.find('.//ans:cabecalho', namespaces=ns)
    if cabecalho is not None:
        identificacao = cabecalho.find('ans:identificacaoTransacao', namespaces=ns)
        if identificacao is not None:
            cabecalho_info['tipoTransacao'] = identificacao.findtext('ans:tipoTransacao', default='', namespaces=ns)
            cabecalho_info['numeroLote'] = identificacao.findtext('ans:numeroLote', default='', namespaces=ns)
            cabecalho_info['competenciaLote'] = identificacao.findtext('ans:competenciaLote', default='', namespaces=ns)
            cabecalho_info['dataRegistroTransacao'] = identificacao.findtext('ans:dataRegistroTransacao', default='', namespaces=ns)
            cabecalho_info['horaRegistroTransacao'] = identificacao.findtext('ans:horaRegistroTransacao', default='', namespaces=ns)
        cabecalho_info['registroANS'] = cabecalho.findtext('ans:registroANS', default='', namespaces=ns)
        cabecalho_info['versaoPadrao'] = cabecalho.findtext('ans:versaoPadrao', default='', namespaces=ns)

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
                proc_data['codigoProcedimento'] = (proc.findtext('ans:identProcedimento/ans:Procedimento/ans:codigoProcedimento', namespaces=ns) or '').strip()
                proc_data['grupoProcedimento'] = (proc.findtext('ans:identProcedimento/ans:Procedimento/ans:grupoProcedimento', namespaces=ns) or '').strip()
                proc_data['valorInformado'] = (proc.findtext('ans:valorInformado', namespaces=ns) or '').strip()
                proc_data['valorPagoProc'] = (proc.findtext('ans:valorPagoProc', namespaces=ns) or '').strip()
                campos_procedimento = ['quantidadeInformada', 'quantidadePaga', 'valorPagoFornecedor', 'valorCoParticipacao', 'unidadeMedida']
                for campo in campos_procedimento:
                    proc_data[campo] = (proc.findtext(f'ans:{campo}', namespaces=ns) or '').strip()
                proc_data['codigoTabela'] = (proc.findtext('ans:identProcedimento/ans:codigoTabela', namespaces=ns) or '').strip()
                proc_data['registroANSOperadoraIntermediaria'] = (proc.findtext('ans:registroANSOperadoraIntermediaria', namespaces=ns) or '').strip()
                proc_data['tipoAtendimentoOperadoraIntermediaria'] = (proc.findtext('ans:tipoAtendimentoOperadoraIntermediaria', namespaces=ns) or '').strip()
                all_data.append(proc_data)
        else:
            all_data.append(guia_data)

    df = pd.DataFrame(all_data)
    df['Nome da Origem'] = file.name

    date_columns = [col for col in df.columns if 'data' in col.lower()]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        except Exception:
            pass

    if 'dataRealizacao' in df.columns and 'dataNascimento' in df.columns:
        def calcular_idade(row):
            try:
                data_realizacao = datetime.strptime(row['dataRealizacao'], '%d/%m/%Y')
                data_nascimento = datetime.strptime(row['dataNascimento'], '%d/%m/%Y')
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
        'dataFimPeriodo','dataInicialFaturamento', 'dataProtocoloCobranca', 'dataPagamento', 'dataProcessamentoGuia',
        'tipoConsulta', 'cboExecutante', 'indicacaoRecemNato', 'indicacaoAcidente',
        'caraterAtendimento', 'tipoInternacao', 'regimeInternacao', 'tipoAtendimento',
        'regimeAtendimento', 'tipoFaturamento', 'diariasAcompanhante', 'diariasUTI', 'motivoSaida',
        'valorTotalInformado', 'valorProcessado', 'valorTotalPagoProcedimentos', 'valorTotalDiarias',
        'valorTotalTaxas', 'valorTotalMateriais', 'valorTotalOPME', 'valorTotalMedicamentos',
        'valorGlosaGuia', 'valorPagoGuia', 'valorPagoFornecedores', 'valorTotalTabelaPropria',
        'valorTotalCoParticipacao', 'declaracaoNascido', 'declaracaoObito', 'codigoTabela',
        'grupoProcedimento', 'codigoProcedimento', 'quantidadeInformada','valorInformado', 'valorInformado_proc',
        'valorPagoFornecedor','quantidadePaga', 'unidadeMedida','valorCoParticipacao', 'valorPagoProc', 'valorPagoFornecedor_proc',
        'Idade_na_Realização', 'diagnosticoCID'
    ]

    for col in colunas_finais:
        if col not in df.columns:
            df[col] = None
    
    df = df[colunas_finais]
    return df, content, tree

@st.cache_data
def parse_xtr(file):
    # Esta função não foi alterada
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

        erros_na_guia = []
        
        for erro in guia_rejeitada.findall('ans:errosGuia', namespaces=ns):
            erros_na_guia.append({
                'codigoErro': erro.findtext('ans:codigoErro', default='', namespaces=ns).strip(),
                'identificadorCampo': erro.findtext('ans:identificadorCampo', default='', namespaces=ns).strip()
            })

        for item_erro in guia_rejeitada.findall('ans:errosItensGuia', namespaces=ns):
            for relacao_erro in item_erro.findall('ans:relacaoErros', namespaces=ns):
                 erros_na_guia.append({
                    'codigoErro': relacao_erro.findtext('ans:codigoErro', default='', namespaces=ns).strip(),
                    'identificadorCampo': relacao_erro.findtext('ans:identificadorCampo', default='', namespaces=ns).strip()
                })

        for erro_item in erros_na_guia:
            linha_completa = guia_info.copy()
            linha_completa.update(erro_item)
            all_errors.append(linha_completa)
            
    df_errors = pd.DataFrame(all_errors)
    return df_errors


#================================================================================
# INTERFACE GRÁFICA (STREAMLIT)
#================================================================================

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

#-------------------------------------------------------------------------------
# PÁGINA ÚNICA: ANÁLISE DE ERROS
#-------------------------------------------------------------------------------
st.subheader("🔍 Análise de Erros do Retorno da ANS (XTR)")
st.markdown("""
Esta ferramenta de demonstração cruza os dados de um arquivo de retorno (`.XTR`) com os dados originais (`.XTE`) para gerar um relatório de análise. A lista de erros da ANS já está integrada.

**Siga os passos:**
1.  Faça o upload do(s) arquivo(s) `.XTE` que você enviou. 
2.  Faça o upload do(s) arquivo(s) de retorno `.XTR` correspondente(s).
3.  Clique em "Analisar Erros" para gerar o relatório consolidado.
""")

col1, col2 = st.columns(2)
with col1:
    uploaded_xte_files = st.file_uploader("1. Arquivos de Envio (.xte)", accept_multiple_files=True, type=["xte"])
with col2:
    uploaded_xtr_files = st.file_uploader("2. Arquivos de Retorno (.xtr)", accept_multiple_files=True, type=["xtr"])

if st.button("Analisar Erros", type="primary"):
    if not uploaded_xte_files or not uploaded_xtr_files:
        st.warning("Por favor, faça o upload dos arquivos XTE e XTR para a análise.")
    else:
        try:
            progress_bar = st.progress(0)
            st.markdown("---")
            
            # --- NOVA ESTRUTURA DE LOGS PERSISTENTES ---
            log_container = st.container()
            log_leitura_xte = log_container.empty()
            download_xte_placeholder = log_container.empty()
            log_leitura_xtr = log_container.empty()
            download_xtr_placeholder = log_container.empty()
            status_placeholder = log_container.empty() # Placeholder para os passos seguintes

            # Passo 1: Consolidar XTE
            log_leitura_xte.info("Passo 1: Consolidando arquivos de envio (.xte)... ⏳")
            df_xte_list = [parse_xte(f)[0] for f in uploaded_xte_files]
            df_xte_full = pd.concat(df_xte_list, ignore_index=True)
            progress_bar.progress(12)
            log_leitura_xte.success(f"Passo 1: Arquivos XTE consolidados! ✅ ({len(df_xte_full)} registros)")
            
            # Prepara e exibe o botão de download para o XTE
            excel_buffer_xte = io.BytesIO()
            df_xte_full.to_excel(excel_buffer_xte, index=False, engine='xlsxwriter')
            download_xte_placeholder.download_button(
                label="⬇ Baixar Planilha Consolidada XTE",
                data=excel_buffer_xte.getvalue(),
                file_name="dados_consolidados_xte.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            time.sleep(1)

            # Passo 2: Consolidar XTR
            log_leitura_xtr.info("Passo 2: Consolidando arquivos de retorno (.xtr)... ⏳")
            df_xtr_list = [parse_xtr(f) for f in uploaded_xtr_files]
            df_xtr_full = pd.concat(df_xtr_list, ignore_index=True)
            progress_bar.progress(25)
            log_leitura_xtr.success(f"Passo 2: Arquivos XTR consolidados! ✅ ({len(df_xtr_full)} erros reportados)")

            # Prepara e exibe o botão de download para o XTR
            excel_buffer_xtr = io.BytesIO()
            df_xtr_full.to_excel(excel_buffer_xtr, index=False, engine='xlsxwriter')
            download_xtr_placeholder.download_button(
                label="⬇ Baixar Planilha de Erros XTR",
                data=excel_buffer_xtr.getvalue(),
                file_name="erros_consolidados_xtr.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            time.sleep(1)
            
            # Início dos passos seguintes (que não precisam ser permanentes)
            # Passo 3: Carregar Padrão ANS
            status_placeholder.info("Passo 3/8: Carregando padrão de erros da ANS... ⏳")
            df_ans_errors = carregar_erros_ans()
            df_ans_errors.rename(columns={'Código do Termo': 'codigoErro', 'Termo': 'descricaoErro'}, inplace=True)
            df_ans_errors['codigoErro'] = df_ans_errors['codigoErro'].astype(str)
            progress_bar.progress(37)
            status_placeholder.success("Passo 3/8: Padrão de erros carregado! ✅")
            time.sleep(1)

            # Passo 4: Preparar Chaves de Cruzamento
            status_placeholder.info("Passo 4/8: Preparando chaves de cruzamento nos dados... ⏳")
            df_xte_full['nomeArquivo_base'] = df_xte_full['Nome da Origem'].apply(lambda x: os.path.splitext(x)[0])
            df_xte_full['chave_cruzamento'] = df_xte_full['nomeArquivo_base'].astype(str) + "_" + df_xte_full['numeroGuia_operadora'].astype(str)
            if not df_xtr_full.empty:
                df_xtr_full['nomeArquivo_base'] = df_xtr_full['nomeArquivo_xtr'].apply(lambda x: os.path.splitext(x)[0])
                df_xtr_full['chave_cruzamento'] = df_xtr_full['nomeArquivo_base'].astype(str) + "_" + df_xtr_full['numeroGuiaOperadora_xtr'].astype(str)
            progress_bar.progress(50)
            status_placeholder.success("Passo 4/8: Chaves de cruzamento preparadas! ✅")
            time.sleep(1)
            
            # Passo 5: Transformar Erros em Colunas (Pivot)
            status_placeholder.info("Passo 5/8: Transformando lista de erros em colunas (Pivot)... ⏳")
            if not df_xtr_full.empty:
                erros_pivot = df_xtr_full.pivot_table(
                    index='chave_cruzamento', 
                    columns='codigoErro', 
                    values='identificadorCampo',
                    aggfunc='first'
                ).reset_index()
                erros_pivot.columns = [f'Campo_Erro_{col}' if col != 'chave_cruzamento' else col for col in erros_pivot.columns]
                for col in erros_pivot.columns:
                    if 'Campo_Erro_' in col:
                        error_code = col.split('_')[-1]
                        erros_pivot[f'Erro_{error_code}'] = erros_pivot[col].apply(lambda x: error_code if pd.notna(x) else pd.NA)
            else:
                erros_pivot = pd.DataFrame(columns=['chave_cruzamento'])
            progress_bar.progress(62)
            status_placeholder.success("Passo 5/8: Lista de erros transformada! ✅")
            time.sleep(1)

            # Passo 6: Juntar Dados Originais com Erros
            status_placeholder.info("Passo 6/8: Juntando dados originais com os erros... ⏳")
            df_final = pd.merge(df_xte_full, erros_pivot, on='chave_cruzamento', how='left')
            progress_bar.progress(75)
            status_placeholder.success("Passo 6/8: Dados unificados! ✅")
            time.sleep(1)

            # Passo 7: Enriquecer com Descrições
            status_placeholder.info("Passo 7/8: Enriquecendo com as descrições dos erros... ⏳")
            error_map = pd.Series(df_ans_errors.descricaoErro.values, index=df_ans_errors.codigoErro).to_dict()
            for code, description in error_map.items():
                error_col_name = f'Erro_{code}'
                if error_col_name in df_final.columns:
                     df_final[f'Descricao_{code}'] = df_final[error_col_name].apply(lambda x: description if pd.notna(x) else pd.NA)
            progress_bar.progress(87)
            status_placeholder.success("Passo 7/8: Descrições adicionadas! ✅")
            time.sleep(1)

            # Passo 8: Organizar Relatório Final
            status_placeholder.info("Passo 8/8: Organizando o relatório final... ⏳")
            error_cols = [col for col in df_final.columns if 'Erro_' in col or 'Campo_Erro_' in col or 'Descricao_' in col]
            final_cols_order = list(df_xte_full.columns.drop(['nomeArquivo_base', 'chave_cruzamento']))
            final_cols_order += sorted(error_cols)
            df_final = df_final[final_cols_order]
            progress_bar.progress(100)
            status_placeholder.success("Passo 8/8: Relatório final organizado! ✅")
            time.sleep(1.5)
            
            # Limpa o último placeholder de status para dar espaço ao resultado
            status_placeholder.empty()

            st.success(f"🎉 Análise concluída!")
            st.markdown("Abaixo está uma **pré-visualização** do resultado:")
            st.dataframe(df_final)
            
            st.markdown("---")
            with st.spinner('Preparando sua planilha para download... ⏳'):
                excel_buffer_final = io.BytesIO()
                df_final.to_excel(excel_buffer_final, index=False, engine='xlsxwriter')
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
