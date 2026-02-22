import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
from typing import List, Optional, Callable, Awaitable, Any

# --- CONFIGURATION ---
SIAC_URL = "https://www.siac.pt/pt"
OLX_BASE_URL = "https://www.olx.pt/"
RNT_AL_URL = "https://rnt.turismodeportugal.pt/RNT/Pesquisa_AL.aspx"
RNT_ET_URL = "https://rnt.turismodeportugal.pt/RNT/Pesquisa_ET.aspx"

SIAC_TEXT_REGISTERED = "Animal com registo no SIAC"
SIAC_TEXT_NOT_REGISTERED = "Animal sem registo"
SIAC_TEXT_MISSING = "Animal com registo no SIAC e que se encontra desaparecido"

st.set_page_config(page_title="Validação Automática", page_icon="🚀", layout="wide")

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

# --- SCRAPERS ---

async def check_siac_on_page(page, microchip: str, retries: int = 1) -> str:
    """Stable validation for SIAC."""
    for attempt in range(retries + 1):
        try:
            if SIAC_URL not in page.url:
                await page.goto(SIAC_URL, timeout=60000, wait_until="networkidle")

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
            await page.keyboard.type(str(microchip), delay=60)
            await page.keyboard.press("Enter")
            await asyncio.sleep(4.0)
                
            content = await page.content()
            if SIAC_TEXT_MISSING in content: return "🚩 DESAPARECIDO"
            if SIAC_TEXT_REGISTERED in content: return "✅ REGISTADO"
            if SIAC_TEXT_NOT_REGISTERED in content: return "❌ SEM REGISTO"
            
            if attempt < retries: await asyncio.sleep(2); continue
            return "❓ Desconhecido"
        except:
            if attempt < retries: await asyncio.sleep(2); continue
            return "⚠️ Erro"
    return "⚠️ Erro"

async def check_olx_km(page, ad_id: str, retries: int = 2) -> str:
    """Validates car mileage on OLX with refined extraction."""
    ad_url = f"{OLX_BASE_URL}{ad_id}"
    for attempt in range(retries + 1):
        try:
            await page.goto(ad_url, timeout=45000, wait_until="domcontentloaded")
            await asyncio.sleep(4)
            
            km_val = await page.evaluate("""
                () => {
                    const details = Array.from(document.querySelectorAll('li, div, p, span'));
                    for (const el of details) {
                        const text = el.innerText ? el.innerText.trim() : "";
                        if (text === 'Quilómetros') {
                            const parent = el.closest('li') || el.parentElement;
                            if (parent) {
                                const fullText = parent.innerText;
                                const match = fullText.match(/(\d[\d\s\.,]*)\s*km/i);
                                if (match) return match[1].trim() + " km";
                            }
                        }
                    }
                    // Fallback to searching any element with "km" that isn't just metadata labels
                    const km_candidates = Array.from(document.querySelectorAll('li span, .ad-details span, div[data-testid="ad-details-parameter-list"] span'));
                    for (const cand of km_candidates) {
                        const content = cand.innerText;
                        const m = content.match(/^(\d[\d\s\.,]*)\s*km$/i);
                        if (m) return m[0].trim();
                    }
                    return null;
                }
            """)
            if km_val: return km_val
            
            content = await page.content()
            if "não se encontra disponível" in content.lower() or "anúncio removido" in content.lower():
                return "🚫 Inativo/Vendido"
            if attempt < retries: await asyncio.sleep(2); continue
            return "❓ Km não encontrado"
        except:
            if attempt < retries: await asyncio.sleep(2); continue
            return "⚠️ Erro Conexão"
    return "⚠️ Erro"

async def check_rnt_dual(page, reg_id: str, retries: int = 1) -> List[str]:
    """Validates registration in BOTH RNAL and RNET."""
    final_res = []
    
    for url, label in [(RNT_AL_URL, "RNAL"), (RNT_ET_URL, "RNET")]:
        res = "❓ Sem Dados"
        for attempt in range(retries + 1):
            try:
                await page.goto(url, timeout=45000, wait_until="networkidle")
                await page.evaluate(f"() => {{ const i = Array.from(document.querySelectorAll('input[type=\"text\"]')).find(x => x.id.includes('NumRegisto') || x.name.includes('NumRegisto') || x.id.includes('txtNRegisto')); if (i) i.value = '{reg_id}'; }}")
                await page.evaluate("() => { const b = Array.from(document.querySelectorAll('input[type=\"submit\"], button')).find(x => x.value.includes('Pesquisar') || x.id.includes('btnPesquisar')); if (b) b.click(); }")
                await asyncio.sleep(5)
                loc = await page.evaluate("() => { const r = document.querySelector('tr.GridRow, tr.GridAlternatingRow, .GridView tr:nth-child(2)'); if (r) { const c = Array.from(r.querySelectorAll('td')); return c[c.length - 1].innerText.trim(); } return null; }")
                if loc: res = loc; break
                if "não foram encontrados" in (await page.content()).lower(): res = "❌ Não Encontrado"; break
                if attempt < retries: await asyncio.sleep(2)
            except: 
                if attempt < retries: await asyncio.sleep(2)
                else: res = f"⚠️ Erro {label}"
        final_res.append(res)
    return final_res

# --- CORE ENGINE ---

async def process_list_incremental(
    items: List[str], 
    checker_func: Callable, 
    init_url: Optional[str] = None,
    existing_results: Optional[List[Any]] = None, 
    callback: Optional[Callable[[List[Any]], Awaitable[None]]] = None, 
    batch_size: int = 10, 
    refresh_every: int = 50,
    **extra_params
) -> List[Any]:
    results: List[Any] = list(existing_results) if existing_results else ["..."] * len(items)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    async with async_playwright() as p:
        browser, context, page = None, None, None
        async def init_browser():
            nonlocal browser, context, page
            if browser: await browser.close()
            try: browser = await p.chromium.launch(headless=True)
            except:
                os.system("playwright install chromium")
                browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            if init_url: await page.goto(init_url, timeout=60000, wait_until="networkidle")

        await init_browser()
        total = len(items)
        for i, val in enumerate(items):
            if i < len(results) and results[i] != "..." and results[i] != ["...", "..."] and not (isinstance(results[i], str) and results[i].startswith("⚠️")):
                progress_bar.progress((i + 1) / total)
                continue
            if i > 0 and i % refresh_every == 0:
                status_text.text(f"♻️ Reiniciando navegador...")
                await init_browser()

            cleaned = str(val).strip().split('.')[0]
            if not cleaned or cleaned == "nan": res = "N/A"
            else:
                status_text.text(f"🔍 [{i+1}/{total}] Processando: {cleaned}")
                res = await checker_func(page, cleaned, **extra_params)
            
            results[i] = res
            progress_bar.progress((i + 1) / total)
            if callback and (i + 1) % batch_size == 0: await callback(results)
            
        if callback: await callback(results)
        if browser: await browser.close()
    return results

# --- UI LOGIC ---

st.title("🚀 Validação Automática")
st.markdown("Plataforma integrada para validação de dados SIAC, AL e OLX.")

tab_siac, tab_rnt, tab_olx = st.tabs(["🐾 SIAC (Cães/Gatos)", "🏠 AL (RNAL e RNET)", "🚗 OLX (Carros)"])

# --- TAB: SIAC ---
with tab_siac:
    st.subheader("Validação SIAC")
    url_siac = st.text_input("URL Google Sheet (SIAC)", key="url_siac")
    if st.button("🚀 Iniciar Validação SIAC"):
        if not url_siac: st.warning("Insira o URL.")
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_siac)
                    ws = sh.get_worksheet(0)
                    femeas = ws.col_values(7)[1:] # G
                    crias = ws.col_values(8)[1:]  # H
                    rows = max(len(femeas), len(crias))
                    interleaved = []
                    for i in range(rows):
                        if i < len(femeas): interleaved.append(femeas[i])
                        if i < len(crias): interleaved.append(crias[i])
                    
                    async def update_siac_gs(res):
                        ptr, sf, sc = 0, [], []
                        for i in range(rows):
                            if i < len(femeas): sf.append([res[ptr]]); ptr += 1
                            else: sf.append(["N/A"])
                            if i < len(crias): sc.append([res[ptr]]); ptr += 1
                            else: sc.append(["N/A"])
                        gc_i = get_gspread_client()
                        if gc_i:
                            sh_i = gc_i.open_by_url(url_siac)
                            ws_i = sh_i.get_worksheet(0)
                            ws_i.update(range_name=f"I2:I{1+len(sf)}", values=sf)
                            ws_i.update(range_name=f"J2:J{1+len(sc)}", values=sc)

                    with st.spinner("Validando SIAC..."):
                        asyncio.run(process_list_incremental(interleaved, check_siac_on_page, init_url=SIAC_URL, callback=update_siac_gs))
                    st.success("Concluído!")
                except Exception as e: st.error(f"Erro: {e}")

# --- TAB: RNT ---
with tab_rnt:
    st.subheader("Validação RNAL e RNET")
    st.info("💡 Valida o ID na coluna E e devolve resultados na F (RNAL) e G (RNET).")
    url_rnt = st.text_input("URL Google Sheet (RNT)", key="url_rnt")
    
    if st.button("🚀 Iniciar Validação AL/ET"):
        if not url_rnt: st.warning("Insira o URL.")
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_rnt)
                    ws = sh.get_worksheet(0)
                    regs = ws.col_values(5)[1:] # Column E
                    
                    async def update_rnt_gs(res):
                        gc_u = get_gspread_client()
                        if gc_u:
                            sh_u = gc_u.open_by_url(url_rnt)
                            ws_u = sh_u.get_worksheet(0)
                            col_f, col_g = [], []
                            for r in res:
                                if isinstance(r, list) and len(r) == 2:
                                    col_f.append([r[0]]); col_g.append([r[1]])
                                else:
                                    col_f.append(["..."]); col_g.append(["..."])
                            if col_f: ws_u.update(range_name=f"F2:F{1+len(col_f)}", values=col_f)
                            if col_g: ws_u.update(range_name=f"G2:G{1+len(col_g)}", values=col_g)
                    
                    with st.spinner("Validando RNAL e RNET..."):
                        asyncio.run(process_list_incremental(regs, check_rnt_dual, callback=update_rnt_gs))
                    st.success("Concluído!")
                    st.balloons()
                except Exception as e: st.error(f"Erro: {e}")

# --- TAB: OLX ---
with tab_olx:
    st.subheader("Validação de Km no OLX")
    url_olx = st.text_input("URL Google Sheet (OLX)", key="url_olx")
    col1, col2 = st.columns(2)
    if col1.button("🔍 Ler IDs", key="btn_read_olx"):
        gc = get_gspread_client()
        if gc:
            try:
                sh = gc.open_by_url(url_olx)
                ws = sh.get_worksheet(0)
                ids = ws.col_values(1)[1:] # Column A
                st.session_state.olx_ids = ids
                st.success(f"Encontrados {len(ids)} IDs na coluna A.")
                st.table(pd.DataFrame({"IDs": ids}).head(5))
            except Exception as e: st.error(f"Erro: {e}")

    if col2.button("🚀 Validar Km", key="btn_run_olx"):
        if 'olx_ids' in st.session_state:
            ids = st.session_state.olx_ids
            async def update_olx(results):
                gc_u = get_gspread_client()
                if gc_u:
                    sh_u = gc_u.open_by_url(url_olx)
                    ws_u = sh_u.get_worksheet(0)
                    formatted = [[r] for r in results]
                    ws_u.update(range_name=f"B2:B{1+len(formatted)}", values=formatted) # Column B
            
            with st.spinner("Validando anúncios OLX..."):
                asyncio.run(process_list_incremental(ids, check_olx_km, callback=update_olx, batch_size=10))
            st.success("Concluído!")
            st.balloons()
        else: st.warning("Leia os dados primeiro.")

st.divider()
st.caption("Validação Automática Multi-Project 2026")
