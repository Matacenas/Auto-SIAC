import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
import time

# --- CONFIGURATION ---
SITE_URL = "https://www.siac.pt/pt"
TEXT_REGISTERED = "Animal com registo no SIAC"
TEXT_NOT_REGISTERED = "Animal sem registo"
TEXT_MISSING = "Animal com registo no SIAC e que se encontra desaparecido"

st.set_page_config(page_title="Validador SIAC", page_icon="🐾", layout="wide")

# --- SERVICES ---

def get_gspread_client():
    """Authenticates with Google Sheets using Streamlit Secrets."""
    if "gcp_service_account" not in st.secrets:
        st.error("Credenciais do Google (Service Account) não encontradas!")
        return None
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "\\n" in creds_dict["private_key"]:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro na autenticação: {e}")
        return None

async def check_siac_single(browser_context, microchip):
    """Stable sequential validation for a single microchip."""
    page = await browser_context.new_page()
    try:
        # Stable navigation
        await page.goto(SITE_URL, timeout=60000, wait_until="networkidle")
        
        # Select input field
        await page.wait_for_selector("input", timeout=15000)
        await page.evaluate("""
            () => {
                const inputs = Array.from(document.querySelectorAll('input'));
                const target = inputs.find(i => i.placeholder && i.placeholder.toLowerCase().includes('transponder')) || inputs[0];
                target.value = '';
                target.focus();
            }
        """)
        
        # Real-time typing simulation
        await page.keyboard.type(str(microchip), delay=100)
        
        # Fixed stable wait for the portal to process
        await asyncio.sleep(4.0)
            
        content = await page.content()
        
        if TEXT_MISSING in content:
            return "🚩 DESAPARECIDO"
        elif TEXT_REGISTERED in content:
            return "✅ REGISTADO"
        elif TEXT_NOT_REGISTERED in content:
            return "❌ SEM REGISTO"
        else:
            return "❓ Desconhecido"
    except Exception as e:
        return f"⚠️ Erro"
    finally:
        await page.close()

async def process_list(microchips):
    """Processes a list of chips sequentially to ensure stability."""
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
        
        total = len(microchips)
        for i, chip in enumerate(microchips):
            cleaned = str(chip).strip().split('.')[0]
            if not cleaned or cleaned == "nan" or cleaned == "":
                results.append("N/A")
            else:
                status_text.text(f"🔍 Validando {i+1}/{total}: {cleaned}")
                res = await check_siac_single(context, cleaned)
                results.append(res)
            
            progress_bar.progress((i + 1) / total)
            
        await browser.close()
    return results

# --- UI LOGIC ---

st.title("🐾 Validador Automático SIAC")
st.markdown("Interface estável para validação de microchips.")

tab_gsheet, tab_file = st.tabs(["📊 Google Sheets", "📂 Arquivo Excel/CSV"])

with tab_gsheet:
    st.subheader("Integração Google Sheets")
    st.info("💡 **MUITO IMPORTANTE:** Partilhe a folha como **Editor** com: `teste-sql@arcane-rigging-486715-n6.iam.gserviceaccount.com`")
    
    gsheet_url = st.text_input("URL do Google Sheet")
    
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("🔍 1. Ler Dados e Mostrar"):
            if not gsheet_url:
                st.error("Insira o link!")
            else:
                try:
                    gc = get_gspread_client()
                    if gc:
                        sh = gc.open_by_url(gsheet_url)
                        worksheet = sh.get_worksheet(0)
                        femeas = worksheet.col_values(6)[1:]
                        crias = worksheet.col_values(7)[1:]
                        
                        st.session_state.temp_femeas = femeas
                        st.session_state.temp_crias = crias
                        st.session_state.gs_url = gsheet_url
                        
                        st.success(f"Dados lidos! Fêmeas: {len(femeas)} | Crias: {len(crias)}")
                        max_len = max(len(femeas), len(crias))
                        st.table(pd.DataFrame({
                            "Fêmea (F)": femeas + [""]*(max_len-len(femeas)),
                            "Cria (G)": crias + [""]*(max_len-len(crias))
                        }).head(10))
                except Exception as e:
                    st.error(f"Erro ao ler: {e}")

    with col_btn2:
        if st.button("🚀 2. Validar e Gravar"):
            if 'temp_femeas' not in st.session_state:
                st.warning("Primeiro clique em 'Ler Dados'.")
            else:
                try:
                    femeas = st.session_state.temp_femeas
                    crias = st.session_state.temp_crias
                    url = st.session_state.gs_url
                    
                    # Interleaved logic
                    interleaved = []
                    rows = max(len(femeas), len(crias))
                    for i in range(rows):
                        if i < len(femeas): interleaved.append(femeas[i])
                        if i < len(crias): interleaved.append(crias[i])
                    
                    with st.spinner("Processando (Modo Estável)..."):
                        raw_results = asyncio.run(process_list(interleaved))
                    
                    # De-interleave
                    siac_f, siac_c = [], []
                    ptr = 0
                    for i in range(rows):
                        if i < len(femeas):
                            siac_f.append(raw_results[ptr])
                            ptr += 1
                        else: siac_f.append("N/A")
                        
                        if i < len(crias):
                            siac_c.append(raw_results[ptr])
                            ptr += 1
                        else: siac_c.append("N/A")
                    
                    # Alerts Logic (Duplicates in Cria)
                    c_counts = {}
                    for i, v in enumerate(crias):
                        c = str(v).strip().split('.')[0]
                        if c and c != "nan":
                            if c in c_counts: c_counts[c].append(i+2)
                            else: c_counts[c] = [i+2]
                    
                    final_f, final_c = [], []
                    for i in range(rows):
                        f_chip = str(femeas[i]).strip().split('.')[0] if i < len(femeas) else ""
                        c_chip = str(crias[i]).strip().split('.')[0] if i < len(crias) else ""
                        res_f, res_c = siac_f[i], siac_c[i]
                        
                        if f_chip != "" and f_chip == c_chip:
                            res_f = f"⚠️ Cria e Fêmea = | {res_f}"
                            res_c = f"⚠️ Cria e Fêmea = | {res_c}"
                        
                        if c_chip != "" and c_chip in c_counts and len(c_counts[c_chip]) > 1:
                            others = [str(r) for r in c_counts[c_chip] if r != i + 2]
                            res_c = f"⚠️ Repetido com a linha nº{', '.join(others)} | {res_c}"
                        
                        final_f.append([res_f])
                        final_c.append([res_c])

                    gc = get_gspread_client()
                    if gc:
                        sh = gc.open_by_url(url)
                        ws = sh.get_worksheet(0)
                        if final_f: ws.update(range_name=f"H2:H{1+len(final_f)}", values=final_f)
                        if final_c: ws.update(range_name=f"I2:I{1+len(final_c)}", values=final_c)
                        st.success("✅ Folha atualizada!")
                        st.balloons()
                except Exception as e:
                    st.error(f"Erro ao gravar: {e}")

with tab_file:
    uploaded = st.file_uploader("Upload Excel/CSV", type=["xlsx", "csv"])
    if uploaded:
        df = pd.read_csv(uploaded) if uploaded.name.endswith('.csv') else pd.read_excel(uploaded)
        st.dataframe(df.head())
        col_f = st.selectbox("Coluna Fêmea", df.columns)
        col_c = st.selectbox("Coluna Cria", df.columns)
        
        if st.button("🎯 Iniciar Validação"):
            f_list, c_list = df[col_f].tolist(), df[col_c].tolist()
            full_list = []
            for f, c in zip(f_list, c_list): full_list.extend([f, c])
            
            raw = asyncio.run(process_list(full_list))
            
            df[f'SIAC_{col_f}'] = [raw[i*2] for i in range(len(f_list))]
            df[f'SIAC_{col_c}'] = [raw[i*2+1] for i in range(len(f_list))]
            
            st.success("Processado!")
            st.dataframe(df)
            
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            st.download_button("📥 Baixar Excel", buffer.getvalue(), "siac_results.xlsx")

st.divider()
st.caption("Auto SIAC - Versão Estável v2.1")
