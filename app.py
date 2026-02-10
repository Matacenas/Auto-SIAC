import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright, BrowserContext, Page
import gspread
from google.oauth2.service_account import Credentials
import os
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# --- CONFIGURATION & CONSTANTS ---
SITE_URL = "https://www.siac.pt/pt"
TEXT_REGISTERED = "Animal com registo no SIAC"
TEXT_NOT_REGISTERED = "Animal sem registo"
TEXT_MISSING = "Animal com registo no SIAC e que se encontra desaparecido"

st.set_page_config(page_title="Auto SIAC Pro", page_icon="🛡️", layout="wide")

# Custom CSS for Premium Look
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stButton>button { width: 100%; border-radius: 8px; height: 3em; font-weight: bold; }
    .stProgress .st-bo { background-color: #4CAF50; }
    .status-box { padding: 10px; border-radius: 5px; margin-bottom: 10px; }
    .log-msg { font-family: monospace; font-size: 0.85em; color: #555; }
    </style>
""", unsafe_allow_html=True)

# --- MODELS & SERVICES ---

class Logger:
    """Handles professional logging inside Streamlit expander."""
    def __init__(self):
        if 'logs' not in st.session_state:
            st.session_state.logs = []
    
    def info(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        st.session_state.logs.append(f"[{timestamp}] ℹ️ {msg}")
    
    def error(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        st.session_state.logs.append(f"[{timestamp}] ❌ {msg}")
    
    def success(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        st.session_state.logs.append(f"[{timestamp}] ✅ {msg}")

    def render(self):
        with st.expander("📄 Logs de Atividade", expanded=False):
            if not st.session_state.logs:
                st.write("Nenhuma atividade registada.")
            else:
                for line in reversed(st.session_state.logs[-50:]): # Show last 50
                    st.markdown(f"<div class='log-msg'>{line}</div>", unsafe_allow_html=True)

class SIACService:
    """Handles SIAC website automation using Playwright."""
    
    def __init__(self, logger: Logger):
        self.logger = logger

    async def _init_browser(self, p):
        try:
            return await p.chromium.launch(headless=True)
        except Exception:
            self.logger.info("A instalar dependências do navegador...")
            os.system("playwright install chromium")
            return await p.chromium.launch(headless=True)

    async def validate_chip(self, context: BrowserContext, chip: str) -> str:
        """Validates a single microchip with optimized waits."""
        if not chip or chip.lower() in ["nan", "n/a", ""]:
            return "N/A"
            
        page: Page = await context.new_page()
        try:
            # Faster navigation
            await page.goto(SITE_URL, timeout=30000, wait_until="domcontentloaded")
            
            # Smart interaction - fill and trigger validation via JS
            await page.evaluate("""
                (chip) => {
                    const inputs = Array.from(document.querySelectorAll('input'));
                    const target = inputs.find(i => i.placeholder && i.placeholder.toLowerCase().includes('transponder')) || inputs[0];
                    if (target) {
                        target.value = chip;
                        target.dispatchEvent(new Event('input', { bubbles: true }));
                        target.dispatchEvent(new Event('change', { bubbles: true }));
                        target.blur();
                    }
                }
            """, chip)
            
            # Wait for any of the 3 outcomes or timeout
            try:
                await page.wait_for_function(f"""
                    () => document.body.innerText.includes("{TEXT_REGISTERED}") || 
                          document.body.innerText.includes("{TEXT_NOT_REGISTERED}") ||
                          document.body.innerText.includes("{TEXT_MISSING}")
                """, timeout=10000)
            except:
                pass # Proceed to content check anyway
            
            content = await page.content()
            
            if TEXT_MISSING in content: return "🚩 DESAPARECIDO"
            if TEXT_REGISTERED in content: return "✅ REGISTADO"
            if TEXT_NOT_REGISTERED in content: return "❌ SEM REGISTO"
            
            return "❓ Desconhecido"
        except Exception as e:
            self.logger.error(f"Erro no chip {chip}: {str(e)}")
            return f"⚠️ Erro"
        finally:
            await page.close()

    async def process_batch(self, chips: List[str]) -> List[str]:
        """Processes chips in parallel batches for speed."""
        results = []
        progress_bar = st.progress(0)
        status = st.empty()
        
        async with async_playwright() as p:
            browser = await self._init_browser(p)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Use chunks of 2 for safety on Streamlit Cloud memory limits
            chunk_size = 2
            for i in range(0, len(chips), chunk_size):
                chunk = chips[i : i + chunk_size]
                self.logger.info(f"A processar lote: {chunk}")
                status.text(f"🚀 Validando {i+1} a {min(i+chunk_size, len(chips))} de {len(chips)}...")
                
                tasks = [self.validate_chip(context, str(c).strip().split('.')[0]) for c in chunk]
                chunk_results = await asyncio.gather(*tasks)
                results.extend(chunk_results)
                
                progress_bar.progress(min(1.0, (i + chunk_size) / len(chips)))
            
            await browser.close()
        return results

class SheetService:
    """Handles Google Sheets interactions."""
    
    def __init__(self, logger: Logger):
        self.logger = logger
        self.client = self._get_client()

    def _get_client(self):
        if "gcp_service_account" not in st.secrets:
            st.error("Secrets 'gcp_service_account' não encontradas!")
            return None
        
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "\\n" in creds_dict["private_key"]:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            self.logger.error(f"Erro na autenticação Google: {e}")
            return None

    def read_sheet_data(self, url: str) -> Tuple[List[str], List[str]]:
        """Reads columns F (6) and G (7) from the first worksheet."""
        if not self.client: return [], []
        try:
            sh = self.client.open_by_url(url)
            ws = sh.get_worksheet(0)
            femeas = ws.col_values(6)[1:] # Skip header
            crias = ws.col_values(7)[1:]
            return femeas, crias
        except Exception as e:
            self.logger.error(f"Erro ao ler folha: {e}")
            raise e

    def update_results(self, url: str, results_f: List[List[str]], results_c: List[List[str]]):
        """Writes lists to columns H (8) and I (9)."""
        if not self.client: return
        try:
            sh = self.client.open_by_url(url)
            ws = sh.get_worksheet(0)
            
            if results_f:
                range_f = f"H2:H{1+len(results_f)}"
                ws.update(range_name=range_f, values=results_f)
                self.logger.success(f"Coluna H atualizada ({len(results_f)} linhas)")
            
            if results_c:
                range_c = f"I2:I{1+len(results_c)}"
                ws.update(range_name=range_c, values=results_c)
                self.logger.success(f"Coluna I atualizada ({len(results_c)} linhas)")
                
        except Exception as e:
            self.logger.error(f"Erro ao gravar na folha: {e}")
            raise e

# --- UI LOGIC ---

def main():
    logger = Logger()
    siac = SIACService(logger)
    sheet = SheetService(logger)

    st.title("🛡️ Auto SIAC Pro - Validador Inteligente")
    
    tabs = st.tabs(["📊 Google Sheets", "📂 Arquivo Excel/CSV"])
    
    with tabs[0]:
        st.info("💡 **DICA:** Partilhe a folha como **Editor** com: `teste-sql@arcane-rigging-486715-n6.iam.gserviceaccount.com`")
        gsheet_url = st.text_input("URL da Folha")
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔍 1. Ler Dados"):
                if not gsheet_url:
                    st.error("Insira o link!")
                else:
                    with st.spinner("Lendo folha..."):
                        try:
                            femeas, crias = sheet.read_sheet_data(gsheet_url)
                            st.session_state.temp_f = femeas
                            st.session_state.temp_c = crias
                            st.session_state.current_url = gsheet_url
                            
                            st.success(f"Importado: {len(femeas)} Fêmeas | {len(crias)} Crias")
                            
                            # Preview
                            max_l = max(len(femeas), len(crias))
                            df = pd.DataFrame({
                                "Fêmea (F)": femeas + [""]*(max_l - len(femeas)),
                                "Cria (G)": crias + [""]*(max_l - len(crias))
                            })
                            st.dataframe(df.head(10), use_container_width=True)
                        except Exception as e:
                            st.error(f"Falha ao aceder à folha: {e}")

        with c2:
            if st.button("🚀 2. Validar e Gravar"):
                if 'temp_f' not in st.session_state:
                    st.warning("Primeiro clique em 'Ler Dados'.")
                else:
                    try:
                        femeas = st.session_state.temp_f
                        crias = st.session_state.temp_c
                        url = st.session_state.current_url
                        
                        # Prepare interleaved list (F1, G1, F2, G2...)
                        interleaved = []
                        num_rows = max(len(femeas), len(crias))
                        for i in range(num_rows):
                            if i < len(femeas): interleaved.append(femeas[i])
                            if i < len(crias): interleaved.append(crias[i])
                        
                        logger.info(f"Iniciando validação de {len(interleaved)} chips...")
                        raw_results = asyncio.run(siac.process_batch(interleaved))
                        
                        # De-interleave and detect alerts
                        siac_f = []
                        siac_c = []
                        ptr = 0
                        for i in range(num_rows):
                            if i < len(femeas):
                                siac_f.append(raw_results[ptr])
                                ptr += 1
                            else: siac_f.append("N/A")
                            
                            if i < len(crias):
                                siac_c.append(raw_results[ptr])
                                ptr += 1
                            else: siac_c.append("N/A")
                        
                        # Apply Business Rules (Duplicates & Equality)
                        cria_counts = {}
                        for i, val in enumerate(crias):
                            c = str(val).strip().split('.')[0]
                            if c and c != "nan":
                                if c in cria_counts: cria_counts[c].append(i+2)
                                else: cria_counts[c] = [i+2]
                        
                        final_f = []
                        final_c = []
                        for i in range(num_rows):
                            f_chip = str(femeas[i]).strip().split('.')[0] if i < len(femeas) else ""
                            c_chip = str(crias[i]).strip().split('.')[0] if i < len(crias) else ""
                            
                            res_f = siac_f[i]
                            res_c = siac_c[i]
                            
                            # Symmetry Rule: Cria == Fêmea
                            if f_chip != "" and f_chip == c_chip:
                                res_f = f"⚠️ Cria e Fêmea = | {res_f}"
                                res_c = f"⚠️ Cria e Fêmea = | {res_c}"
                            
                            # Duplicate Rule: Cria only
                            if c_chip != "" and c_chip in cria_counts and len(cria_counts[c_chip]) > 1:
                                others = [str(r) for r in cria_counts[c_chip] if r != i + 2]
                                res_c = f"⚠️ Repetido com a linha nº{', '.join(others)} | {res_c}"
                            
                            final_f.append([res_f])
                            final_c.append([res_c])
                        
                        with st.spinner("Gravando no Google Sheets..."):
                            sheet.update_results(url, final_f, final_c)
                        
                        st.success("✅ Concluído com sucesso!")
                        st.balloons()
                        
                    except Exception as e:
                        st.error(f"Erro fatal: {e}")
                        logger.error(f"Erro: {e}")

    with tabs[1]:
        st.subheader("Processamento de Arquivos Locais")
        uploaded = st.file_uploader("Escolha Excel ou CSV", type=["xlsx", "csv"])
        if uploaded:
            df = pd.read_csv(uploaded) if uploaded.name.endswith('.csv') else pd.read_excel(uploaded)
            st.dataframe(df.head())
            
            cols = st.columns(2)
            col_f = cols[0].selectbox("Coluna Fêmea", df.columns)
            col_c = cols[1].selectbox("Coluna Cria", df.columns)
            
            if st.button("🔥 Iniciar Validação em Lote"):
                f_list = df[col_f].tolist()
                c_list = df[col_c].tolist()
                
                # Interleaved validation
                full_list = []
                for f, c in zip(f_list, c_list):
                    full_list.extend([f, c])
                
                raw = asyncio.run(siac.process_batch(full_list))
                
                # Split back
                df[f'SIAC_{col_f}'] = [raw[i*2] for i in range(len(f_list))]
                df[f'SIAC_{col_c}'] = [raw[i*2+1] for i in range(len(f_list))]
                
                st.success("Resultados processados!")
                st.dataframe(df.head(20))
                
                # Download link
                import io
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                st.download_button("📥 Descarregar Excel", buf.getvalue(), "siac_results.xlsx")

    st.divider()
    logger.render()
    st.caption("Auto SIAC Pro v3.0 - Professional Standard")

if __name__ == "__main__":
    main()
