import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
import time
from typing import List, Optional, Callable, Awaitable

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

async def check_siac_on_page(page, microchip: str, retries: int = 2) -> str:
    """Stable validation reusing the same page with retries."""
    for attempt in range(retries + 1):
        try:
            # Select and clear input
            await page.evaluate("""
                () => {
                    const inputs = Array.from(document.querySelectorAll('input'));
                    const target = inputs.find(i => i.placeholder && i.placeholder.toLowerCase().includes('transponder')) || inputs[0];
                    if (target) {
                        target.value = '';
                        target.focus();
                    }
                }
            """)
            
            # Real-time typing simulation
            await page.keyboard.type(str(microchip), delay=60)
            await page.keyboard.press("Enter")
            
            # Wait for content change or processing
            await asyncio.sleep(4.0)
                
            content = await page.content()
            
            if TEXT_MISSING in content:
                return "🚩 DESAPARECIDO"
            elif TEXT_REGISTERED in content:
                return "✅ REGISTADO"
            elif TEXT_NOT_REGISTERED in content:
                return "❌ SEM REGISTO"
            
            # If no conclusive text found, maybe it's still loading or errored
            if attempt < retries:
                await asyncio.sleep(2)
                continue
                
            return "❓ Desconhecido"
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(2)
                continue
            print(f"Erro persistente no chip {microchip}: {e}")
            return f"⚠️ Erro"
    return "⚠️ Erro"

async def process_list_incremental(microchips: List[str], existing_results: Optional[List[str]] = None, callback: Optional[Callable[[List[str]], Awaitable[None]]] = None, batch_size: int = 10, refresh_every: int = 50) -> List[str]:
    """Processes a list of chips with page reuse, periodic refresh and resume support."""
    results: List[str] = list(existing_results) if existing_results else ["..."] * len(microchips)
    progress_bar = st.progress(0)
    status_text = st.empty()
    chips_list = list(microchips)
    
    async with async_playwright() as p:
        browser = None
        context = None
        page = None

        async def init_browser():
            nonlocal browser, context, page
            if browser: await browser.close()
            try:
                browser = await p.chromium.launch(headless=True)
            except:
                os.system("playwright install chromium")
                browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            await page.goto(SITE_URL, timeout=60000, wait_until="networkidle")

        await init_browser()

        total = len(chips_list)
        for i, chip in enumerate(chips_list):
            # RESUME LOGIC: Skip if already has a conclusive result
            if i < len(results) and any(icon in results[i] for icon in ["✅", "❌", "🚩"]):
                progress_bar.progress((i + 1) / total)
                continue

            # PERIODIC REFRESH: Re-init browser every N items to prevent memory leaks/crashes
            if i > 0 and i % refresh_every == 0:
                status_text.text(f"♻️ A reiniciar navegador para estabilidade...")
                await init_browser()

            cleaned = str(chip).strip().split('.')[0]
            if not cleaned or cleaned == "nan" or cleaned == "":
                res = "N/A"
            else:
                status_text.text(f"🔍 Nº {i+1}/{total}: {cleaned}")
                res = await check_siac_on_page(page, cleaned)
            
            if i < len(results): results[i] = res
            else: results.append(res)
            
            progress_bar.progress((i + 1) / total)
            
            if callback is not None and (i + 1) % batch_size == 0:
                await callback(results)
            
        if callback is not None:
            await callback(results)
            
        if browser is not None: await browser.close()
    return results

# --- UI LOGIC ---

st.title("🐾 Validação Automática SIAC")
st.markdown("Interface para validação de números de identificação de cães/gatos.")

tab_gsheet, tab_file = st.tabs(["📊 Google Sheets", "📂 Arquivo Excel/CSV"])

with tab_gsheet:
    st.subheader("Integração Google Sheets")
    st.info("💡 **MUITO IMPORTANTE:** Partilha a folha como **Editor** com: `teste-sql@arcane-rigging-486715-n6.iam.gserviceaccount.com`")
    
    gsheet_url = st.text_input("URL do Google Sheet")
    
    col_btn1, col_btn2, col_btn3 = st.columns(3)
    with col_btn1:
        if st.button("🔍 1. Ler Dados"):
            if not gsheet_url:
                st.error("Insira o link!")
            else:
                try:
                    gc = get_gspread_client()
                    if gc:
                        sh = gc.open_by_url(gsheet_url)
                        worksheet = sh.get_worksheet(0)
                        femeas = worksheet.col_values(7)[1:]
                        crias = worksheet.col_values(8)[1:]
                        # Read existing results for Resume feature
                        try:
                            res_f = worksheet.col_values(9)[1:]
                            res_c = worksheet.col_values(10)[1:]
                        except:
                            res_f, res_c = [], []
                        
                        st.session_state.temp_femeas = femeas
                        st.session_state.temp_crias = crias
                        st.session_state.temp_res_f = res_f
                        st.session_state.temp_res_c = res_c
                        st.session_state.gs_url = gsheet_url
                        
                        st.success(f"Dados lidos! Fêmeas: {len(femeas)} | Crias: {len(crias)}")
                        max_len = max(len(femeas), len(crias))
                        st.table(pd.DataFrame({
                            "Fêmea (G)": femeas + [""]*(max_len-len(femeas)),
                            "Cria (H)": crias + [""]*(max_len-len(crias))
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
                    res_f_init = st.session_state.temp_res_f
                    res_c_init = st.session_state.temp_res_c
                    url = st.session_state.gs_url
                    rows = max(len(femeas), len(crias))
                    
                    # Interleaved list for processing
                    interleaved = []
                    existing_interleaved = []
                    for i in range(rows):
                        if i < len(femeas): 
                            interleaved.append(femeas[i])
                            existing_interleaved.append(res_f_init[i] if i < len(res_f_init) else "...")
                        if i < len(crias): 
                            interleaved.append(crias[i])
                            existing_interleaved.append(res_c_init[i] if i < len(res_c_init) else "...")
                    
                    async def main_validation():
                        # Prepare counts for duplicate alert (Cria only)
                        c_counts = {}
                        for i, v in enumerate(crias):
                            c = str(v).strip().split('.')[0]
                            if c and c != "nan":
                                if c in c_counts: c_counts[c].append(i+2)
                                else: c_counts[c] = [i+2]

                        async def update_sheet_callback(current_results: List[str]) -> None:
                            """Callback to update the sheet incrementally."""
                            siac_f, siac_c = [], []
                            ptr = 0
                            for i in range(rows):
                                # Femea
                                if i < len(femeas):
                                    raw_res = current_results[ptr] if ptr < len(current_results) else "..."
                                    # Clean old alerts
                                    res = raw_res.split("|")[-1].strip()
                                    
                                    f_chip = str(femeas[i]).strip().split('.')[0]
                                    c_chip = str(crias[i]).strip().split('.')[0] if i < len(crias) else ""
                                    # Alerts
                                    if f_chip != "" and f_chip == c_chip: res = f"⚠️ Cria e Fêmea = | {res}"
                                    siac_f.append([res])
                                    ptr += 1
                                else: siac_f.append(["N/A"])
                                
                                # Cria
                                if i < len(crias):
                                    raw_res = current_results[ptr] if ptr < len(current_results) else "..."
                                    # Clean old alerts
                                    res = raw_res.split("|")[-1].strip()
                                    
                                    c_chip = str(crias[i]).strip().split('.')[0]
                                    f_chip = str(femeas[i]).strip().split('.')[0] if i < len(femeas) else ""
                                    # Alerts
                                    if c_chip != "" and f_chip == c_chip: res = f"⚠️ Cria e Fêmea = | {res}"
                                    if c_chip != "" and c_chip in c_counts and len(c_counts[c_chip]) > 1:
                                        others = [str(r) for r in c_counts[c_chip] if r != i + 2]
                                        res = f"⚠️ Repetido com a linha nº{', '.join(others)} | {res}"
                                    siac_c.append([res])
                                    ptr += 1
                                else: siac_c.append(["N/A"])
                            
                            # Write to sheet
                            gc_internal = get_gspread_client()
                            if gc_internal:
                                try:
                                    sh_internal = gc_internal.open_by_url(url)
                                    ws_internal = sh_internal.get_worksheet(0)
                                    if siac_f: ws_internal.update(range_name=f"I2:I{1+len(siac_f)}", values=siac_f)
                                    if siac_c: ws_internal.update(range_name=f"J2:J{1+len(siac_c)}", values=siac_c)
                                except Exception as e_sheet:
                                    print(f"Erro ao atualizar folha (batch): {e_sheet}")

                        await process_list_incremental(interleaved, existing_results=existing_interleaved, callback=update_sheet_callback, batch_size=20, refresh_every=50)

                    with st.spinner("A validar e a atualizar a folha em tempo real..."):
                        asyncio.run(main_validation())
                    
                    st.success("✅ Folha totalmente atualizada!")
                    st.balloons()
                except Exception as e:
                    st.error(f"Erro ao processar: {e}")

    with col_btn3:
        if st.button("🧹 3. Limpar Registados"):
            if 'gs_url' not in st.session_state:
                st.error("Primeiro insira o link e 'Ler Dados'.")
            else:
                try:
                    url = st.session_state.gs_url
                    gc = get_gspread_client()
                    if gc:
                        sh = gc.open_by_url(url)
                        ws = sh.get_worksheet(0)
                        
                        with st.spinner("A analisar linhas para remoção..."):
                            data = ws.get_all_values()
                            # Columns I (index 8) and J (index 9)
                            to_delete = []
                            for i, row in enumerate(data[1:], start=2): # skip header, 1-indexed
                                if len(row) > 9:
                                    val_h = str(row[8]).strip()
                                    val_i = str(row[9]).strip()
                                    if val_h == "✅ REGISTADO" and val_i == "✅ REGISTADO":
                                        to_delete.append(i)
                            
                            if not to_delete:
                                st.info("Nenhuma linha encontrada para remoção (Ambas ✅ REGISTADO e sem alertas).")
                            else:
                                for row_idx in sorted(to_delete, reverse=True):
                                    ws.delete_rows(row_idx)
                                st.success(f"Removidas {len(to_delete)} linhas com sucesso!")
                                st.balloons()
                except Exception as e:
                    st.error(f"Erro ao limpar: {e}")

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
            
            with st.spinner("A Processar..."):
                raw = asyncio.run(process_list_incremental(full_list))
            
            # Alerts Logic (Duplicates in Cria)
            c_counts = {}
            for i, v in enumerate(c_list):
                c = str(v).strip().split('.')[0]
                if c and c != "nan":
                    if c in c_counts: c_counts[c].append(i+2) # +2 to match sheet row style
                    else: c_counts[c] = [i+2]
            
            final_f, final_c = [], []
            for i in range(len(f_list)):
                f_chip = str(f_list[i]).strip().split('.')[0]
                c_chip = str(c_list[i]).strip().split('.')[0]
                res_f, res_c = raw[i*2], raw[i*2+1]
                
                # Equality alert
                if f_chip != "" and f_chip == c_chip:
                    res_f = f"⚠️ Cria e Fêmea = | {res_f}"
                    res_c = f"⚠️ Cria e Fêmea = | {res_c}"
                
                # Duplicate alert (Cria only)
                if c_chip != "" and c_chip in c_counts and len(c_counts[c_chip]) > 1:
                    others = [str(r) for r in c_counts[c_chip] if r != i + 2]
                    res_c = f"⚠️ Repetido com a linha nº{', '.join(others)} | {res_c}"
                
                final_f.append(res_f)
                final_c.append(res_c)
            
            df[f'SIAC_{col_f}'] = final_f
            df[f'SIAC_{col_c}'] = final_c
            
            st.success("Processado!")
            st.dataframe(df)
            
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            st.download_button("📥 Download do Excel", buffer.getvalue(), "siac_results.xlsx")

st.divider()
st.caption("Auto SIAC 2026")
