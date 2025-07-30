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
# FUNÇÕES DE PARSE (FASE 1 e FASE 2)
#================================================================================

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
            'numeroGuiaOperadora_xtr': guia_rejeitada.findtext('ans:numeroGuiaOperadora', default='', namespaces=ns)
        }

        for erro in guia_rejeitada.findall('ans:errosGuia', namespaces=ns):
            error_details = guia_info.copy()
            error_details.update({
                'guia_identificadorCampo': erro.findtext('ans:identificadorCampo', default='', namespaces=ns),
                'guia_codigoErro': erro.findtext('ans:codigoErro', default='', namespaces=ns)
            })
            all_errors.append(error_details)

        for item_erro in guia_rejeitada.findall('ans:errosItensGuia', namespaces=ns):
            procedimento_info = {}
            ident_proc = item_erro.find('ans:identProcedimento', namespaces=ns)
            if ident_proc is not None:
                procedimento_info = {
                    'item_codigoTabela': ident_proc.findtext('ans:codigoTabela', default='', namespaces=ns),
                    'item_codigoProcedimento': ident_proc.findtext('ans:Procedimento/ans:codigoProcedimento', default='', namespaces=ns),
                    'item_grupoProcedimento': ident_proc.findtext('ans:Procedimento/ans:grupoProcedimento', default='', namespaces=ns),
                }
            
            for relacao_erro in item_erro.findall('ans:relacaoErros', namespaces=ns):
                error_details = guia_info.copy()
                error_details.update(procedimento_info)
                error_details.update({
                    'item_identificadorCampo': relacao_erro.findtext('ans:identificadorCampo', default='', namespaces=ns),
                    'item_codigoErro': relacao_erro.findtext('ans:codigoErro', default='', namespaces=ns)
                })
                all_errors.append(error_details)

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
# PÁGINA ÚNICA: ANÁLISE DE ERROS (MODO DEMONSTRAÇÃO)
#-------------------------------------------------------------------------------
st.subheader("🔍 Análise de Erros do Retorno da ANS (XTR)")
st.markdown("""
Esta ferramenta de demonstração cruza os dados de um arquivo de retorno (`.XTR`) com os dados originais (`.XTE`) e uma planilha de referência da ANS para gerar um relatório de análise. 

**Siga os passos:**
1.  Faça o upload do(s) arquivo(s) `.XTE` que você enviou. 
2.  Faça o upload do(s) arquivo(s) de retorno `.XTR` correspondente(s).
3.  Faça o upload da planilha de referência com os códigos de erro da ANS. 
4.  Clique em "Analisar Erros" para gerar uma pré-visualização do relatório consolidado.
""")

col1, col2, col3 = st.columns(3)
with col1:
    uploaded_xte_files = st.file_uploader("1. Arquivos de Envio (.xte)", accept_multiple_files=True, type=["xte"])
with col2:
    uploaded_xtr_files = st.file_uploader("2. Arquivos de Retorno (.xtr)", accept_multiple_files=True, type=["xtr"])
with col3:
    uploaded_ans_errors_file = st.file_uploader("3. Planilha de Erros da ANS (.xlsx)", type=["xlsx"])

if st.button("Analisar Erros", type="primary"):
    if not uploaded_xte_files or not uploaded_xtr_files or not uploaded_ans_errors_file:
        st.warning("Por favor, faça o upload de todos os arquivos necessários para a análise.")
    else:
        try:
            progress_bar = st.progress(0)
            st.markdown("---")
            
            # Criar placeholders que serão atualizados
            log_container = st.container()
            log_leitura_xte = log_container.empty()
            download_xte_placeholder = log_container.empty() # Placeholder para o botão de download
            log_leitura_xtr = log_container.empty()
            download_xtr_placeholder = log_container.empty() # Placeholder para o botão de download
            log_leitura_ans = log_container.empty()
            log_cruzamento = log_container.empty()
            log_enriquecimento = log_container.empty()
            
            # Etapa 1.1: Ler arquivos XTE
            log_leitura_xte.info("Passo 1/5: Lendo e consolidando arquivos de envio (.xte)... ⏳")
            df_xte_list = []
            total_xte = len(uploaded_xte_files)
            for i, f in enumerate(uploaded_xte_files):
                df_xte_list.append(parse_xte(f)[0])
                progress_bar.progress(int(((i + 1) / total_xte) * 20))
            df_xte_full = pd.concat(df_xte_list, ignore_index=True)
            log_leitura_xte.success(f"Passo 1/5: Arquivos XTE lidos e consolidados! ✅ ({len(df_xte_full)} registros encontrados)")
            
            # --- NOVO: Botão de Download para Passo 1 ---
            excel_buffer_xte = io.BytesIO()
            df_xte_full.to_excel(excel_buffer_xte, index=False, engine='xlsxwriter')
            download_xte_placeholder.download_button(
                label="⬇ Baixar Planilha de Dados (.xlsx)",
                data=excel_buffer_xte.getvalue(),
                file_name="dados_consolidados_xte.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            time.sleep(1)

            # Etapa 1.2: Ler arquivos XTR
            log_leitura_xtr.info("Passo 2/5: Lendo e consolidando arquivos de retorno (.xtr)... ⏳")
            df_xtr_list = []
            total_xtr = len(uploaded_xtr_files)
            for i, f in enumerate(uploaded_xtr_files):
                df_xtr_list.append(parse_xtr(f))
                progress_bar.progress(20 + int(((i + 1) / total_xtr) * 20))
            df_xtr_full = pd.concat(df_xtr_list, ignore_index=True)
            log_leitura_xtr.success(f"Passo 2/5: Arquivos XTR lidos e consolidados! ✅ ({len(df_xtr_full)} erros reportados)")

            # --- NOVO: Botão de Download para Passo 2 ---
            excel_buffer_xtr = io.BytesIO()
            df_xtr_full.to_excel(excel_buffer_xtr, index=False, engine='xlsxwriter')
            download_xtr_placeholder.download_button(
                label="⬇ Baixar Planilha de Erros (.xlsx)",
                data=excel_buffer_xtr.getvalue(),
                file_name="erros_consolidados_xtr.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            time.sleep(1)

            # Etapa 1.3: Ler Planilha ANS
            log_leitura_ans.info("Passo 3/5: Lendo planilha de referência da ANS... ⏳")
            df_ans_errors = pd.read_excel(uploaded_ans_errors_file)
            df_ans_errors.rename(columns={'Código do Termo': 'codigoErro', 'Termo': 'descricaoErro'}, inplace=True)
            df_ans_errors['codigoErro'] = df_ans_errors['codigoErro'].astype(str)
            progress_bar.progress(50)
            log_leitura_ans.success(f"Passo 3/5: Planilha de erros da ANS lida! ✅ ({len(df_ans_errors)} códigos de erro carregados)")
            time.sleep(1)

            # Passo 2 e 3: Preparar e Cruzar dados
            log_cruzamento.info("Passo 4/5: Cruzando guias com erro com os dados originais... ⏳")
            df_xte_full['nomeArquivo_base'] = df_xte_full['Nome da Origem'].apply(lambda x: os.path.splitext(x)[0])
            df_xte_full['chave_cruzamento'] = df_xte_full['nomeArquivo_base'].astype(str) + "_" + df_xte_full['numeroGuia_operadora'].astype(str)
            
            df_xtr_full['nomeArquivo_base'] = df_xtr_full['nomeArquivo_xtr'].apply(lambda x: os.path.splitext(x)[0])
            df_xtr_full['chave_cruzamento'] = df_xtr_full['nomeArquivo_base'].astype(str) + "_" + df_xtr_full['numeroGuiaOperadora_xtr'].astype(str)

            df_analise = pd.merge(
                df_xte_full,
                df_xtr_full,
                on='chave_cruzamento',
                how='inner' 
            )
            progress_bar.progress(80)
            log_cruzamento.success("Passo 4/5: Cruzamento de dados concluído! ✅")
            time.sleep(1)

            if df_analise.empty:
                log_container.empty()
                progress_bar.empty()
                st.error("Nenhuma correspondência de guia com erro encontrada. Verifique se os nomes dos arquivos XTE e XTR correspondem e se os números de guia são os mesmos.")
            else:
                # Passo 4: Adicionar descrições dos erros
                log_enriquecimento.info("Passo 5/5: Adicionando descrições aos códigos de erro... ⏳")
                df_analise['codigoErro'] = df_analise['guia_codigoErro'].fillna(df_analise['item_codigoErro'])
                
                df_final = pd.merge(
                    df_analise,
                    df_ans_errors[['codigoErro', 'descricaoErro']],
                    on='codigoErro',
                    how='left'
                )
                progress_bar.progress(100)
                log_enriquecimento.success("Passo 5/5: Análise enriquecida com as descrições dos erros! ✅")
                time.sleep(1.5)
                
                log_container.empty()
                progress_bar.empty()

                st.success(f"🎉 Análise concluída! Foram encontradas {len(df_final)} ocorrências de erro.")
                st.markdown("Abaixo está uma **pré-visualização** do resultado da análise (primeiras 50 linhas):")
                
                st.dataframe(df_final.head(50))
                
        except Exception as e:
            st.error(f"Ocorreu um erro durante a análise: {e}")
            st.error("Verifique se os arquivos estão no formato correto e se as colunas da planilha de erros da ANS estão corretas ('Código do Termo', 'Termo').")
