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
RESULT_SUCCESS_TEXT = "Animal com registo no SIAC"

# Authenticate with Google Sheets
def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        # For Streamlit Cloud secrets
        creds_dict = dict(st.secrets["gcp_service_account"])
        # Fix private key escaping if needed
        if "\\n" in creds_dict["private_key"]:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    else:
        # Local fallback
        st.error("Credenciais do Google (Service Account) não encontradas nos Secrets.")
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

async def check_siac_single(browser_context, microchip):
    page = await browser_context.new_page()
    try:
        await page.goto(SITE_URL, timeout=60000)
        # Find the input field for transponder
        input_field = await page.wait_for_selector("input", timeout=15000)
        
        # More robust input detection
        await page.evaluate("""
            (chip) => {
                const inputs = Array.from(document.querySelectorAll('input'));
                const target = inputs.find(i => i.placeholder && i.placeholder.toLowerCase().includes('transponder')) || inputs[0];
                target.value = '';
                target.focus();
            }
        """, str(microchip))
        
        await page.keyboard.type(str(microchip), delay=50)
        await asyncio.sleep(2.5) # Wait for dynamic validation
        
        content = await page.content()
        if RESULT_SUCCESS_TEXT in content:
            return "✅ Registado no SIAC"
        else:
            return "❌ Não Registado"
    except Exception as e:
        return f"⚠️ Erro: {str(e)}"
    finally:
        await page.close()

async def process_list(microchips):
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    async with async_playwright() as p:
        # Important for Streamlit Cloud: handle browser installation
        try:
            browser = await p.chromium.launch(headless=True)
        except:
            # Try to install if not found (though packages.txt handles this usually)
            os.system("playwright install chromium")
            browser = await p.chromium.launch(headless=True)
            
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        for i, chip in enumerate(microchips):
            if not chip or pd.isna(chip) or str(chip).strip() == "":
                results.append("N/A")
                continue
                
            status_text.text(f"A validar {i+1}/{len(microchips)}: {chip}")
            res = await check_siac_single(context, str(int(float(chip))) if isinstance(chip, (float, int)) else str(chip))
            results.append(res)
            progress_bar.progress((i + 1) / len(microchips))
            
        await browser.close()
    return results

# UI
st.title("🐾 Validador Automático SIAC")
st.markdown("""
Esta ferramenta valida números de microchips diretamente no portal SIAC. 
Os resultados são atualizados automaticamente no Google Sheets.
""")

tab_gsheet, tab_excel = st.tabs(["📊 Google Sheets", "Excel Upload"])

with tab_gsheet:
    st.subheader("Configuração Google Sheets")
    gsheet_url = st.text_input("Link do Google Sheet", help="Certifique-se que partilhou a folha com o email da Service Account.")
    
    if st.button("Validar do Google Sheets"):
        if not gsheet_url:
            st.error("Por favor, insira o link do Google Sheets.")
        else:
            try:
                gc = get_gspread_client()
                if gc:
                    sh = gc.open_by_url(gsheet_url)
                    worksheet = sh.get_worksheet(0) # Primeira aba
                    data = pd.DataFrame(worksheet.get_all_records())
                    
                    if data.empty:
                        # Fallback if no headers
                        raw_data = worksheet.get_all_values()
                        data = pd.DataFrame(raw_data[1:], columns=raw_data[0]) if raw_data else pd.DataFrame()

                    st.info(f"Ficheiro aberto: {sh.title}")
                    
                    # Target columns E (index 4) and F (index 5)
                    # Let's check columns
                    cols = worksheet.row_values(1)
                    st.write(f"Colunas detetadas: {', '.join(cols)}")
                    
                    # Columns E and F are likely Female and Cria
                    # We'll ask user to confirm or just use indexes
                    to_validate = []
                    # Column E = index 5 (1-based for gspread is tricky, 0-based index 4)
                    # In gspread worksheet.col_values(5) is Col E
                    col_e_vals = worksheet.col_values(5)[1:] # Skip header
                    col_f_vals = worksheet.col_values(6)[1:] # Skip header
                    
                    chips = col_e_vals + col_f_vals
                    chips = [c for c in chips if c.strip() != ""]
                    
                    if not chips:
                        st.warning("Nenhum microchip encontrado nas colunas E ou F.")
                    else:
                        st.write(f"Encontrados {len(chips)} números para validar.")
                        
                        if st.button("🚀 Iniciar Validação e Gravar na Folha"):
                            with st.spinner("A validar microchips no SIAC..."):
                                results_list = asyncio.run(process_list(chips))
                                st.session_state.siac_results = results_list
                            
                            st.info("A gravar resultados de volta no Google Sheets...")
                            
                            results = st.session_state.siac_results
                            # Update Femea Results (Col G)
                            num_femea = len(col_e_vals)
                            femea_results = [[r] for r in results[:num_femea]]
                            if femea_results:
                                worksheet.update(range_name=f"G2:G{1+num_femea}", values=femea_results)
                            
                            # Update Cria Results (Col H)
                            cria_results = [[r] for r in results[num_femea:]]
                            if cria_results:
                                worksheet.update(range_name=f"H2:H{1+len(cria_results)}", values=cria_results)
                            
                            st.success("✅ Folha atualizada com sucesso!")
                            
                            # Display summary
                            res_df = pd.DataFrame({"Microchip": chips, "Status": results})
                            st.table(res_df.head(20))

            except Exception as e:
                st.error(f"Erro ao aceder ao Google Sheets: {e}")

with tab_excel:
    uploaded_file = st.file_uploader("Escolha um ficheiro Excel", type=["xlsx"])
    if uploaded_file:
        df = pd.read_excel(uploaded_file)
        st.write("Pré-visualização:")
        st.dataframe(df.head())
        
        column = st.selectbox("Selecione a coluna com os microchips", df.columns)
        
        if st.button("Iniciar Validação (Excel)"):
            chips = df[column].tolist()
            results = asyncio.run(process_list(chips))
            df['Resultado SIAC'] = results
            
            st.success("Processamento concluído!")
            st.dataframe(df)
            
            # Use IO to create excel for download without saving to disk
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            
            st.download_button(
                label="📥 Download Excel com Resultados",
                data=buffer.getvalue(),
                file_name="siac_validado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

st.divider()
st.caption("Nota: Partilhe a folha com: teste-sql@arcane-rigging-486715-n6.iam.gserviceaccount.com")
