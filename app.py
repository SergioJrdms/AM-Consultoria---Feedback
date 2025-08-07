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
1002,NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO
1002,NÚMERO DO CARTÃO NACIONAL DE SAÚDE INVÁLIDO
1024,PLANO NÃO EXISTENTE
1024,PLANO NÃO EXISTENTE
1202,NÚMERO DO CNES INVÁLIDO
1202,NÚMERO DO CNES INVÁLIDO
1206,CPF / CNPJ INVÁLIDO
1206,CPF / CNPJ INVÁLIDO
1206,CPF / CNPJ INVÁLIDO
1206,CPF / CNPJ INVÁLIDO
1213,CBO (ESPECIALIDADE) INVÁLIDO
1213,CBO (ESPECIALIDADE) INVÁLIDO
1304,COBRANÇA EM GUIA INDEVIDA
1304,COBRANÇA EM GUIA INDEVIDA
1307,NÚMERO DA GUIA INVÁLIDO
1307,NÚMERO DA GUIA INVÁLIDO
1307,NÚMERO DA GUIA INVÁLIDO
1307,NÚMERO DA GUIA INVÁLIDO
1307,NÚMERO DA GUIA INVÁLIDO
1308,GUIA JÁ APRESENTADA
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1323,DATA PREENCHIDA INCORRETAMENTE
1506, TIPO DE INTERNAÇÃO INVÁLIDO
1509,CÓDIGO CID INVÁLIDO
1509,CÓDIGO CID INVÁLIDO
1509,CÓDIGO CID INVÁLIDO
1509,CÓDIGO CID INVÁLIDO
1602,TIPO DE ATENDIMENTO INVÁLIDO OU NÃO INFORMADO
1603,TIPO DE CONSULTA INVÁLIDO
1705,VALOR APRESENTADO A MAIOR
1706,VALOR APRESENTADO A MENOR
1706,VALOR APRESENTADO A MENOR
1706,VALOR APRESENTADO A MENOR
1713,FATURAMENTO INVÁLIDO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1740,ESTORNO DO VALOR DE PROCEDIMENTO PAGO
1801,PROCEDIMENTO INVÁLIDO
1801,PROCEDIMENTO INVÁLIDO
1801,PROCEDIMENTO INVÁLIDO
1801,PROCEDIMENTO INVÁLIDO
1801,PROCEDIMENTO INVÁLIDO
1806,QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
1806,QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
1806,QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
1806,QUANTIDADE DE PROCEDIMENTO DEVE SER MAIOR QUE ZERO
2601,CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO
2601,CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO
2601,CODIFICAÇÃO INCORRETA/INADEQUADA DO PROCEDIMENTO
5001,MENSAGEM ELETRÔNICA FORA DO PADRÃO TISS
5002,NÃO FOI POSSÍVEL VALIDAR O ARQUIVO XML
5014,CÓDIGO HASH INVÁLIDO. MENSAGEM PODE ESTAR CORROMPIDA.
5016,SEM NENHUMA OCORRÊNCIA DE MOVIMENTO DE INCLUSÃO NA COMPETÊNCIA PARA ENVIO A ANS
5017,ARQUIVO PROCESSADO PELA ANS.
5023,COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
5023,COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
5023,COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
5023,COMPETÊNCIA NÃO ESTÁ ABERTA PARA ENVIO DE DADOS
5024,OPERADORA INATIVA NA COMPETÊNCIA DOS DADOS
5025,DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA
5025,DATA DE REGISTRO DA TRANSAÇÃO INVÁLIDA
5026,HORA DE REGISTRO DA TRANSAÇÃO INVÁLIDA
5027,REGISTRO ANS DA OPERADORA INVÁLIDO
5027,REGISTRO ANS DA OPERADORA INVÁLIDO
5027,REGISTRO ANS DA OPERADORA INVÁLIDO
5028,VERSÃO DO PADRÃO INVÁLIDA
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5029,INDICADOR INVÁLIDO
5030,CÓDIGO DO MUNICÍPIO INVÁLIDO
5030,CÓDIGO DO MUNICÍPIO INVÁLIDO
5030,CÓDIGO DO MUNICÍPIO INVÁLIDO
5030,CÓDIGO DO MUNICÍPIO INVÁLIDO
5031,CARÁTER DE ATENDIMENTO INVÁLIDO
5032,INDICADOR DE RECÉM–NATO INVÁLIDO
5033,MOTIVO DE ENCERRAMENTO INVÁLIDO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5034,VALOR NÃO INFORMADO
5035,CÓDIGO DA TABELA DE REFERÊNCIA NÃO INFORMADO
5036,CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO
5036,CÓDIGO DO GRUPO DO PROCEDIMENTO INVÁLIDO
5039,CÓDIGO DA FACE DO DENTE INVÁLIDO
5040,VALOR DEVE SER MAIOR QUE ZERO
5040,VALOR DEVE SER MAIOR QUE ZERO
5042,VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS
5042,VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS
5042,VALOR INFORMADO DA GUIA DIFERENTE DO SOMATÓRIO DO VALOR INFORMADO DOS ITENS
5044,JÁ EXISTEM INFORMAÇÕES NA ANS PARA A COMPETÊNCIA INFORMADA.
5045,COMPETENCIA ANTERIOR NÃO ENVIADA
5045,COMPETENCIA ANTERIOR NÃO ENVIADA
5046,COMPETÊNCIA INVÁLIDA
5050,VALOR INFORMADO INVÁLIDO
5050,VALOR INFORMADO INVÁLIDO
5050,VALOR INFORMADO INVÁLIDO
5050,VALOR INFORMADO INVÁLIDO
5050,VALOR INFORMADO INVÁLIDO
5052,IDENTIFICADOR INEXISTENTE
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5053,IDENTIFICADOR JÁ INFORMADO
5054,IDENTIFICADOR NÃO ENCONTRADO
5054,IDENTIFICADOR NÃO ENCONTRADO
5054,IDENTIFICADOR NÃO ENCONTRADO
5054,IDENTIFICADOR NÃO ENCONTRADO
5054,IDENTIFICADOR NÃO ENCONTRADO
5055,IDENTIFICADOR JÁ INFORMADO NA COMPETÊNCIA
5056,IDENTIFICADOR NÃO INFORMADO NA COMPETÊNCIA
5059,EXCLUSÃO INVÁLIDA – EXISTEM LANÇAMENTOS VINCULADOS A ESTA FORMA DE CONTRATAÇÃO
5061,TIPO DE ATENDIMENTO OPERADORA INTERMEDIÁRIA NÃO INFORMADO
5062,REGISTRO ANS DA OPERADORA INTERMEDIÁRIA NÃO INFORMADO"""
    
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
Esta ferramenta cruza os dados de um arquivo de retorno (`.XTR`) com os dados originais (`.XTE`) para gerar um relatório de análise focado **apenas nas guias com erro**. A lista de erros da ANS já está integrada.

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
            
            # --- ESTRUTURA DE LOGS PERSISTENTES E DOWNLOADS ---
            log_container = st.container()
            log_leitura_xte = log_container.empty()
            download_xte_placeholder = log_container.empty()
            log_leitura_xtr = log_container.empty()
            download_xtr_placeholder = log_container.empty()
            status_placeholder = log_container.empty() # Placeholder para os passos sequenciais

            # Passo 1: Consolidar XTE
            log_leitura_xte.info("Passo 1: Consolidando arquivos de envio (.xte)... ⏳")
            df_xte_list = [parse_xte(f)[0] for f in uploaded_xte_files]
            df_xte_full = pd.concat(df_xte_list, ignore_index=True)
            progress_bar.progress(14) # Ajustado para 7 passos
            log_leitura_xte.success(f"Passo 1: Arquivos XTE consolidados! ✅ ({len(df_xte_full)} registros)")
            
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
            progress_bar.progress(28)
            log_leitura_xtr.success(f"Passo 2: Arquivos XTR consolidados! ✅ ({len(df_xtr_full)} erros reportados)")

            excel_buffer_xtr = io.BytesIO()
            df_xtr_full.to_excel(excel_buffer_xtr, index=False, engine='xlsxwriter')
            download_xtr_placeholder.download_button(
                label="⬇ Baixar Planilha de Erros XTR",
                data=excel_buffer_xtr.getvalue(),
                file_name="erros_consolidados_xtr.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            time.sleep(1)
            
            # Passo 3: Carregar Padrão ANS
            status_placeholder.info("Passo 3/7: Carregando padrão de erros da ANS... ⏳")
            df_ans_errors = carregar_erros_ans()
            df_ans_errors.rename(columns={'Código do Termo': 'codigoErro', 'Termo': 'descricaoErro'}, inplace=True)
            df_ans_errors['codigoErro'] = df_ans_errors['codigoErro'].astype(str)
            progress_bar.progress(42)
            status_placeholder.success("Passo 3/7: Padrão de erros carregado! ✅")
            time.sleep(1)

            # Passo 4: Preparar Chaves de Cruzamento
            status_placeholder.info("Passo 4/7: Preparando chaves de cruzamento nos dados... ⏳")
            df_xte_full['nomeArquivo_base'] = df_xte_full['Nome da Origem'].apply(lambda x: os.path.splitext(x)[0])
            df_xte_full['chave_cruzamento'] = df_xte_full['nomeArquivo_base'].astype(str) + "_" + df_xte_full['numeroGuia_operadora'].astype(str)
            if not df_xtr_full.empty:
                df_xtr_full['nomeArquivo_base'] = df_xtr_full['nomeArquivo_xtr'].apply(lambda x: os.path.splitext(x)[0])
                df_xtr_full['chave_cruzamento'] = df_xtr_full['nomeArquivo_base'].astype(str) + "_" + df_xtr_full['numeroGuiaOperadora_xtr'].astype(str)
            progress_bar.progress(56)
            status_placeholder.success("Passo 4/7: Chaves de cruzamento preparadas! ✅")
            time.sleep(1)
            
            # Passo 5: Transformar Erros em Colunas (Pivot)
            status_placeholder.info("Passo 5/7: Transformando lista de erros em colunas...")
            if not df_xtr_full.empty:
                erros_pivot = df_xtr_full.pivot_table(
                    index='chave_cruzamento', 
                    columns='codigoErro', 
                    values='identificadorCampo',
                    aggfunc='first'
                ).reset_index()
                
                error_map = pd.Series(df_ans_errors.descricaoErro.values, index=df_ans_errors.codigoErro).to_dict()

                colunas_renomeadas = {'chave_cruzamento': 'chave_cruzamento'}
                for col in erros_pivot.columns:
                    if col != 'chave_cruzamento':
                        colunas_renomeadas[col] = f'Campo_Erro_{col}'
                        descricao = error_map.get(col, f'Descrição não encontrada para {col}')
                        erros_pivot[f'Erro_{col}'] = erros_pivot[col].apply(lambda x: descricao if pd.notna(x) else pd.NA)

                erros_pivot.rename(columns=colunas_renomeadas, inplace=True)
            else:
                erros_pivot = pd.DataFrame(columns=['chave_cruzamento'])
            progress_bar.progress(70)
            status_placeholder.success("Passo 5/7: Lista de erros transformada! ✅")
            time.sleep(1)

            # Passo 6: Juntar Dados Originais com Erros (INNER JOIN)
            status_placeholder.info("Passo 6/7: Filtrando apenas guias com erro (INNER JOIN)...")
            df_final = pd.merge(df_xte_full, erros_pivot, on='chave_cruzamento', how='inner')
            progress_bar.progress(85)
            status_placeholder.success(f"Passo 6/7: Guias com erro filtradas! ✅")
            time.sleep(1)

            # Passo 7: Organizar Relatório Final
            status_placeholder.info("Passo 7/7: Organizando o relatório final...")
            if not df_final.empty:
                error_cols = [col for col in df_final.columns if 'Erro_' in col or 'Campo_Erro_' in col]
                final_cols_order = list(df_xte_full.columns.drop(['nomeArquivo_base', 'chave_cruzamento']))
                final_cols_order += [col for col in sorted(error_cols) if col in df_final.columns]
                final_cols_order = [col for col in final_cols_order if col in df_final.columns]
                df_final = df_final[final_cols_order]
            progress_bar.progress(100)
            status_placeholder.success("Passo 7/7: Relatório final organizado! ✅")
            time.sleep(1.5)
            
            status_placeholder.empty()
            
            if df_final.empty:
                st.error("Nenhuma correspondência de guia com erro encontrada. Verifique se os nomes dos arquivos XTE e XTR correspondem.")
            else:
                st.success(f"🎉 Análise concluída! Foram encontradas {len(df_final)} linhas de procedimento nas guias com erro.")
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
