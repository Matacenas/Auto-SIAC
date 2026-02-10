import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
import time

# Page config
st.set_page_config(page_title="Validador SIAC Pro", page_icon="🐾", layout="wide")

# Constants
SITE_URL = "https://www.siac.pt/pt"
# SIAC Text Detectors based on user requirements
TEXT_REGISTERED = "Animal com registo no SIAC"
TEXT_NOT_REGISTERED = "Animal sem registo"
TEXT_MISSING = "Animal com registo no SIAC e que se encontra desaparecido"

# Authenticate with Google Sheets
def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "\\n" in creds_dict["private_key"]:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    else:
        st.error("Credenciais do Google (Service Account) não encontradas nos Secrets.")
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

async def check_siac_single(browser_context, microchip):
    page = await browser_context.new_page()
    try:
        # Back to networkidle for safety
        await page.goto(SITE_URL, timeout=60000, wait_until="networkidle")
        
        # Wait for any input (usually the microchip search)
        input_field = await page.wait_for_selector("input", timeout=15000)
        
        await page.evaluate("""
            () => {
                const inputs = Array.from(document.querySelectorAll('input'));
                const target = inputs.find(i => i.placeholder && i.placeholder.toLowerCase().includes('transponder')) || inputs[0];
                target.value = '';
                target.focus();
            }
        """)
        
        # Use keyboard.type as it's more reliable for triggering site listeners
        await page.keyboard.type(str(microchip), delay=100)
        
        # Conservative wait for the dynamic result to appear
        await asyncio.sleep(4.0)
            
        content = await page.content()
        
        if TEXT_MISSING in content:
            return "🚩 DESAPARECIDO"
        elif TEXT_REGISTERED in content:
            return "✅ REGISTADO"
        elif TEXT_NOT_REGISTERED in content:
            return "❌ SEM REGISTO"
        else:
            return "❓ Desconhecido/Erro"
    except Exception as e:
        return f"⚠️ Erro: {str(e)}"
    finally:
        await page.close()

async def process_list(microchips):
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
        except:
            os.system("playwright install chromium")
            browser = await p.chromium.launch(headless=True)
            
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Back to sequential processing for maximum stability
        for i, chip in enumerate(microchips):
            cleaned_chip = str(chip).strip().split('.')[0]
            if not cleaned_chip or cleaned_chip == "nan" or cleaned_chip == "":
                results.append("N/A")
                continue
                
            status_text.text(f"A validar {i+1}/{len(microchips)}: {cleaned_chip}")
            res = await check_siac_single(context, cleaned_chip)
            results.append(res)
            progress_bar.progress((i + 1) / len(microchips))
            
        await browser.close()
    return results

# UI
st.title("🐾 Validador Automático SIAC Pro")
st.markdown("""
Valide números de microchips no portal SIAC. Resultados automáticos para **Fêmea** e **Cria**.
""")

# Initialize session state for results if not exists
if 'siac_results_femea' not in st.session_state:
    st.session_state.siac_results_femea = []
if 'siac_results_cria' not in st.session_state:
    st.session_state.siac_results_cria = []

tab_gsheet, tab_file = st.tabs(["📊 Google Sheets", "📂 Arquivo (Excel/CSV)"])

with tab_gsheet:
    st.subheader("Integração Google Sheets")
    
    st.info("💡 **DICA:** Partilhe a folha como **Editor** com o email: `teste-sql@arcane-rigging-486715-n6.iam.gserviceaccount.com` para que a automação consiga ler e gravar os dados.")
    
    gsheet_url = st.text_input("Link do Google Sheet")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔍 1. Ler Dados e Mostrar"):
            if not gsheet_url:
                st.error("Insira o link!")
            else:
                try:
                    gc = get_gspread_client()
                    if gc:
                        sh = gc.open_by_url(gsheet_url)
                        worksheet = sh.get_worksheet(0)
                        
                        # Columns F (6) and G (7)
                        femeas = worksheet.col_values(6)[1:]
                        crias = worksheet.col_values(7)[1:]
                        
                        st.session_state.temp_femeas = femeas
                        st.session_state.temp_crias = crias
                        st.session_state.gsheet_attached = gsheet_url
                        st.success(f"Dados lidos! Fêmeas: {len(femeas)} | Crias: {len(crias)}")
                        
                        max_len = max(len(femeas), len(crias))
                        femeas_padded = femeas + [""] * (max_len - len(femeas))
                        crias_padded = crias + [""] * (max_len - len(crias))
                        
                        preview_df = pd.DataFrame({
                            "Fêmea (Col F)": femeas_padded,
                            "Cria (Col G)": crias_padded
                        })
                        st.table(preview_df.head(10))
                except Exception as e:
                    st.error(f"Erro ao ler folha: {e}")

    with col2:
        if st.button("🚀 2. Validar e Gravar na Folha"):
            if 'temp_femeas' not in st.session_state:
                st.warning("Primeiro, clique em 'Ler Dados'.")
            else:
                try:
                    femeas = st.session_state.temp_femeas
                    crias = st.session_state.temp_crias
                    url = st.session_state.gsheet_attached
                    
                    # 1. Local Alerts Logic
                    local_alerts_f = []
                    local_alerts_c = []
                    
                    # Track duplicates in Crias
                    cria_counts = {}
                    for idx, c in enumerate(crias):
                        chip = str(c).strip().split('.')[0]
                        if chip and chip != "nan" and chip != "":
                            if chip in cria_counts:
                                cria_counts[chip].append(idx + 2) # +2 for sheet row
                            else:
                                cria_counts[chip] = [idx + 2]

                    # Interleave chips for row-by-row validation (F2, G2, F3, G3...)
                    interleaved_chips = []
                    num_rows = max(len(femeas), len(crias))
                    for i in range(num_rows):
                        if i < len(femeas): interleaved_chips.append(femeas[i])
                        if i < len(crias): interleaved_chips.append(crias[i])

                    with st.spinner("A validar toda a lista (Sequencialmente par-a-par)..."):
                        results = asyncio.run(process_list(interleaved_chips))
                    
                    # De-interleave results back to F and G
                    siac_f = []
                    siac_c = []
                    idx = 0
                    for i in range(num_rows):
                        if i < len(femeas):
                            siac_f.append(results[idx])
                            idx += 1
                        if i < len(crias):
                            siac_c.append(results[idx])
                            idx += 1
                    
                    final_res_f = []
                    final_res_c = []
                    
                    for i in range(num_rows):
                        f_chip = str(femeas[i]).strip().split('.')[0] if i < len(femeas) else ""
                        c_chip = str(crias[i]).strip().split('.')[0] if i < len(crias) else ""
                        
                        f_res = siac_f[i] if i < len(siac_f) else ""
                        c_res = siac_c[i] if i < len(siac_c) else ""
                        
                        # Symmetric alert for Femea == Cria
                        if f_chip != "" and f_chip == c_chip:
                            f_res = f"⚠️ Cria e Fêmea = | {f_res}"
                            c_res = f"⚠️ Cria e Fêmea = | {c_res}"
                        
                        # Duplicate alert (Cria only as requested)
                        if c_chip != "" and c_chip in cria_counts and len(cria_counts[c_chip]) > 1:
                            others = [str(r) for r in cria_counts[c_chip] if r != i + 2]
                            c_res = f"⚠️ Repetido com a linha nº{', '.join(others)} | {c_res}"
                            
                        final_res_f.append([f_res])
                        final_res_c.append([c_res])
                    
                    gc = get_gspread_client()
                    if gc:
                        sh = gc.open_by_url(url)
                        worksheet = sh.get_worksheet(0)
                        
                        # Write back to H (8) and I (9)
                        if final_res_f:
                            worksheet.update(range_name=f"H2:H{1+len(final_res_f)}", values=final_res_f)
                        if final_res_c:
                            worksheet.update(range_name=f"I2:I{1+len(final_res_c)}", values=final_res_c)
                        
                        st.success("✅ Folha atualizada! Verifique as colunas H e I.")
                        st.balloons()
                except Exception as e:
                    st.error(f"Erro ao gravar: {e}")

with tab_file:
    uploaded_file = st.file_uploader("Escolha um ficheiro Excel ou CSV", type=["xlsx", "csv"])
    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        st.write("Pré-visualização:")
        st.dataframe(df.head())
        
        col_f = st.selectbox("Coluna da Fêmea", df.columns, index=min(5, len(df.columns)-1))
        col_g = st.selectbox("Coluna da Cria", df.columns, index=min(6, len(df.columns)-1))
        
        if st.button("🚀 Iniciar Validação em Massa"):
            with st.spinner("A validar microchips..."):
                femeas_raw = df[col_f].tolist()
                crias_raw = df[col_g].tolist()
                
                # Internal alerts for File
                cria_counts = {}
                for idx, c in enumerate(crias_raw):
                    chip = str(c).strip().split('.')[0]
                    if chip and chip != "nan" and chip != "":
                        if chip in cria_counts:
                            cria_counts[chip].append(idx + 2)
                        else:
                            cria_counts[chip] = [idx + 2]
                
                # Interleave for row-by-row validation
                interleaved_chips = []
                for f, c in zip(femeas_raw, crias_raw):
                    interleaved_chips.append(f)
                    interleaved_chips.append(c)
                
                siac_results = asyncio.run(process_list(interleaved_chips))
                
                siac_f = [siac_results[i*2] for i in range(len(femeas_raw))]
                siac_c = [siac_results[i*2+1] for i in range(len(femeas_raw))]
                
                final_f = []
                final_c = []
                
                for i in range(len(df)):
                    f_chip = str(femeas_raw[i]).strip().split('.')[0]
                    c_chip = str(crias_raw[i]).strip().split('.')[0]
                    
                    res_f = siac_f[i]
                    res_c = siac_c[i]
                    
                    # Symmetric alert for Femea == Cria
                    if f_chip != "" and f_chip == c_chip:
                        res_f = f"⚠️ Cria e Fêmea = | {res_f}"
                        res_c = f"⚠️ Cria e Fêmea = | {res_c}"
                    
                    # Duplicate alert (Cria only as requested)
                    if c_chip != "" and c_chip in cria_counts and len(cria_counts[c_chip]) > 1:
                        others = [str(r) for r in cria_counts[c_chip] if r != i + 2]
                        res_c = f"⚠️ Repetido com a linha nº{', '.join(others)} | {res_c}"
                        
                    final_f.append(res_f)
                    final_c.append(res_c)
                
                df[f'Resultado SIAC_{col_f}'] = final_f
                df[f'Resultado SIAC_{col_g}'] = final_c
                
                st.success("Concluído!")
                st.dataframe(df)
                
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                
                st.download_button(
                    label="📥 Download Resultados (Excel)",
                    data=buffer.getvalue(),
                    file_name="siac_validado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

st.divider()
st.caption("Auto SIAC Pro - Validação Inteligente")
