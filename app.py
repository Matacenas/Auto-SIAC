import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from typing import List, Optional, Callable, Awaitable, Any
import time

# --- CONFIGURATION ---
LINKS_FILE = "links.json"
GLOBAL_DEFAULT_URL = "https://docs.google.com/spreadsheets/d/17sq7E56TExN8Icw9Du2oUiuzhLDzfb4VmTgFmJGS1Do"

def load_links():
    if os.path.exists(LINKS_FILE):
        try:
            with open(LINKS_FILE, "r") as f:
                return json.load(f)
        except: return {}
    return {}

def save_link(key, url):
    links = load_links()
    links[key] = url
    if not os.path.exists(os.path.dirname(LINKS_FILE)) and os.path.dirname(LINKS_FILE):
        os.makedirs(os.path.dirname(LINKS_FILE))
    with open(LINKS_FILE, "w") as f:
        json.dump(links, f)

SIAC_URL = "https://www.siac.pt/pt"
OLX_BASE_URL = "https://www.olx.pt/"
RNT_AL_DIRECT_URL = "https://rnt.turismodeportugal.pt/RNT/RNAL.aspx?nr="

SIAC_TEXT_REGISTERED = "Animal com registo no SIAC"
SIAC_TEXT_NOT_REGISTERED = "Animal sem registo"
SIAC_TEXT_MISSING = "Animal com registo no SIAC e que se encontra desaparecido"

# --- I18N SYSTEM ---
TRANSLATIONS = {
    "PT": {
        "title": "🚀 Validação Automática",
        "subtitle": "Plataforma para validação de dados SIAC, AL e OLX.",
        "sidebar_config": "🌐 Linguagem / Language",
        "lang_sel": "Escolha o Idioma / Language",
        "sheet_urls": "🔗 URLs do Google Sheets",
        "siac_tab": "🐾 SIAC",
        "siac_sub": "🐾 SIAC - Cães e Gatos",
        "rnal_tab": "🏠 RNAL",
        "rnal_sub": "🏠 RNAL - Alojamento Local",
        "olx_tab": "🚗 OLX",
        "olx_sub": "🚗 OLX - Km Carros",
        "gs_url_label": "URL Google Sheet",
        "btn_start": "🚀 Iniciar Validação",
        "btn_open_sheet": "📖 Abrir Folha",
        "btn_clear_reg": "🧹 Limpar Registados (Ambos ✅)",
        "btn_clear_mod": "🧹 Limpar Corrigidos pelo user/Moderados/Inactivos",
        "btn_clear_loc": "🧹 Limpar Localização Correcta",
        "status_working": "🔍 A Trabalhar: {}",
        "status_done": "Concluído!",
        "err_no_url": "Insira o URL.",
        "err_no_sheet": "ERRO: Aba '{}' não encontrada no ficheiro!",
        "dica_siac": "💡 **Processo de Validação:**\n\n1. Lê os dados das Colunas G e H.\n2. Realiza a validação automática do microchip no site do SIAC.pt.\n3. Regista o resultado da validação nas Colunas I e J.",
        "dica_rnal": "💡 **Processo de Validação:**\n\n1. Lê o ID do anúncio na Coluna A.\n2. Faz scraping da localização do anúncio em olx.pt e regista o resultado na Coluna C.\n3. Lê o Número de Alojamento Local da Coluna D e valida no site do RNAL.\n4. Faz scraping do resultado da validação e regista a informação na Coluna E.\n5. Compara a localização do OLX com a do RNAL e regista a sugestão na Coluna F.",
        "dica_olx": "💡 **Processo de Validação:**\n\n1. Lê o ID do anúncio na Coluna A.\n2. Acede a olx.pt e faz scraping dos quilómetros apresentados no anúncio (LIVE).\n3. Regista os quilómetros obtidos na Coluna D.\n4. Compara os valores da Coluna C com os da Coluna D.\n5. Regista o resultado da validação na Coluna E.",
        "restarting_browser": "♻️ Reiniciando navegador...",
        "val_waiting": "⚠️ Sem resultado - Confirmar no RNET ⚠️",
        "val_correct": "✅Localização Correcta ✅",
        "val_wrong": "❌ Localização Errada ❌",
        "km_wrong": "❌ KM errados ❌",
        "km_fixed": "✅ KM corrigidos pelo user ✅",
        "km_missing_param": "Parâmetro não preenchido",
        "km_moderated": "⚠️ Anúncio já foi moderado ⚠️",
        "km_inactive": "⚠️ Anúncio inactivo ⚠️",
        "cleaning": "Limpando linhas...",
        "rows_removed": "Removidas {} linhas!",
        "no_rows": "Nenhuma linha para remover.",
        "siac_registered": "✅ REGISTADO",
        "siac_not_registered": "❌ SEM REGISTO",
        "siac_missing": "🚩 DESAPARECIDO",
        "siac_unknown": "❓ Desconhecido",
        "footer": "Validação Automática Multi-Project 2026"
    },
    "EN": {
        "title": "🚀 Auto Validation",
        "subtitle": "Platform for SIAC, AL, and OLX data validation.",
        "sidebar_config": "🌐 Linguagem / Language",
        "lang_sel": "Language Selection",
        "sheet_urls": "🔗 Google Sheets URLs",
        "siac_tab": "🐾 SIAC",
        "siac_sub": "🐾 SIAC - Dogs and Cats",
        "rnal_tab": "🏠 RNAL",
        "rnal_sub": "🏠 RNAL - Local Accommodation",
        "olx_tab": "🚗 OLX",
        "olx_sub": "🚗 OLX - Car Kilometers",
        "gs_url_label": "Google Sheet URL",
        "btn_start": "🚀 Start Validation",
        "btn_open_sheet": "📖 Open Sheet",
        "btn_clear_reg": "🧹 Clear Registered (Both ✅)",
        "btn_clear_mod": "🧹 Clear Corrected by user/Moderated/Inactive",
        "btn_clear_loc": "🧹 Clear Correct Location",
        "status_working": "🔍 Working on: {}",
        "status_done": "Completed!",
        "err_no_url": "Please enter the URL.",
        "err_no_sheet": "ERROR: Sheet '{}' not found in the file!",
        "dica_siac": "💡 **Workflow:**\n\n1. Reads data from Columns G and H.\n2. Performs automatic microchip validation on the SIAC.pt website.\n3. Records the validation result in Columns I and J.",
        "dica_rnal": "💡 **Workflow:**\n\n1. Reads the ad ID from Column A.\n2. Scrapes the ad location on olx.pt and records the result in Column C.\n3. Reads the Local Accommodation Number from Column D and validates it on the RNAL website:\n   https://rnt.turismodeportugal.pt/RNT/RNAL.aspx?nr=AdID\n4. Scrapes the validation result and records the information in Column E.\n5. Compares the OLX location with the RNAL location and records the suggestion in Column F.",
        "dica_olx": "💡 **Workflow:**\n\n1. Reads the ad ID from Column A.\n2. Accesses olx.pt and scrapes the Kilometers presented in the ad (LIVE).\n3. Records the obtained Kilometers in Column D.\n4. Compares the values in Column C with those in Column D.\n5. Records the validation result in Column E.",
        "restarting_browser": "♻️ Restarting browser...",
        "val_waiting": "⚠️ No result - Confirm on RNET ⚠️",
        "val_correct": "✅ Correct Location ✅",
        "val_wrong": "❌ Wrong Location ❌",
        "km_wrong": "❌ Incorrect Kilometers ❌",
        "km_fixed": "✅ Kilometers corrected by user ✅",
        "km_missing_param": "Parameter not filled",
        "km_moderated": "⚠️ Ad Already Moderated ⚠️",
        "km_inactive": "⚠️ Ad Inactive ⚠️",
        "cleaning": "Cleaning rows...",
        "rows_removed": "Removed {} rows!",
        "no_rows": "No rows to remove.",
        "siac_registered": "✅ Registered",
        "siac_not_registered": "❌ Unregistered",
        "siac_missing": "🚩 Missing",
        "siac_unknown": "❓ Unknown",
        "footer": "Multi-Project Auto Validation 2026"
    }
}

if "lang" not in st.session_state: st.session_state.lang = "PT"

def t(key, *args):
    text = TRANSLATIONS[st.session_state.lang].get(key, key)
    if args: return text.format(*args)
    return text

st.set_page_config(page_title="Validação Automática", page_icon="🚀", layout="wide")

# --- CUSTOM CSS (PREMIUM RESTORED) ---
st.markdown("""
<style>
    [data-testid="stSidebar"] { min-width: 300px; max-width: 350px; }
    [data-testid="stNotification"] { width: 500px !important; border-radius: 10px; }
    .stAlert { width: 500px !important; border-radius: 10px; padding: 10px 20px; }
    button[data-baseweb="tab"] { font-size: 24px !important; height: 60px !important; font-weight: bold !important; }
    button[data-baseweb="tab"] p { font-size: 24px !important; }
    div[data-baseweb="tab-list"] { width: 500px !important; }
    div[data-baseweb="tab-border"] { width: 500px !important; }
    hr { width: 500px !important; margin-left: 0 !important; border-top: 2px solid #555; }
    .flag-container { display: flex; align-items: center; justify-content: center; height: 38px; }
    .flag-img { height: 26px; border-radius: 2px; box-shadow: 0 0 2px rgba(0,0,0,0.5); }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR (LAYOUT RESTORED) ---
with st.sidebar:
    st.header(t("sidebar_config"))
    cp1, cp2 = st.columns([1, 4])
    with cp1: st.markdown("<div class='flag-container'><img class='flag-img' src='https://flagcdn.com/w80/pt.png'></div>", unsafe_allow_html=True)
    with cp2: 
        if st.button("Português", key="btn_pt", use_container_width=True): st.session_state.lang = "PT"; st.rerun()
    ce1, ce2 = st.columns([1, 4])
    with ce1: st.markdown("<div class='flag-container'><img class='flag-img' src='https://flagcdn.com/w80/gb.png'></div>", unsafe_allow_html=True)
    with ce2:
        if st.button("English", key="btn_en", use_container_width=True): st.session_state.lang = "EN"; st.rerun()
    
    st.divider()
    saved_links = load_links()
    st.markdown(f"### {t('gs_url_label')}")
    current_gs = saved_links.get("gs_url") or GLOBAL_DEFAULT_URL
    url_gs = st.text_input("", value=current_gs, label_visibility="collapsed")
    if url_gs != saved_links.get("gs_url", ""): save_link("gs_url", url_gs)
    if url_gs: st.link_button(t("btn_open_sheet"), url_gs, use_container_width=True)

# --- SERVICES ---
def get_gspread_client():
    if "gcp_service_account" not in st.secrets: return None
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "\\n" in creds_dict["private_key"]: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds)
    except: return None

def get_worksheet_by_name(sh, target_name):
    try:
        ts = [ws.title for ws in sh.worksheets()]
        match = next((tt for tt in ts if tt.strip().lower() == target_name.strip().lower()), None)
        return sh.worksheet(match) if match else None
    except: return None

def normalize_id(val: Any) -> str:
    if val is None: return ""
    s = str(val).strip()
    if not s or s.lower() == "nan": return ""
    if s.endswith(".0") or s.endswith(",0"): s = s[:-2]
    if "E+" in s.upper() or "E-" in s.upper():
        try: return str(int(float(s.replace(',', '.'))))
        except: pass
    if s.replace('.', '').replace(',', '').isdigit() and len(s) > 10: return s.replace('.', '').replace(',', '')
    return s

# --- CHECKERS (ADVANCED RESTORED) ---
async def check_siac_on_page(page, microchip: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[SIAC] {m}")
    chip = normalize_id(microchip)
    if not chip or len(chip) < 10: return "❓ Formato Inválido"

    for attempt in range(2):
        try:
            if SIAC_URL not in page.url:
                await page.goto(SIAC_URL, timeout=60000, wait_until="domcontentloaded")
                await asyncio.sleep(4)
            
            sel = "input[name='searchGtWro'], input[placeholder*='transponder']"
            await page.evaluate("""([s, v]) => {
                const el = document.querySelector(s);
                if (el) { 
                    el.value = v; 
                    el.dispatchEvent(new Event('input', {bubbles:true})); 
                    el.dispatchEvent(new Event('change', {bubbles:true})); 
                    el.focus(); 
                    return true; 
                }
                return false;
            }""", [sel, chip])
            await page.keyboard.press("Enter")
            log(f"Consultando chip {chip}...")
            
            # Polling for result (Surgical targeting with chip-sync check)
            start = time.time()
            tried_click = False
            while time.time() - start < 40:
                res = await page.evaluate(f"""() => {{
                    const bodyText = document.body.innerText;
                    const clean = (s) => s.normalize("NFD").replace(/[\\u0300-\\u036f]/g, "").toLowerCase();
                    const tc = clean(bodyText);
                    
                    // IF we see "Não foram encontrados resultados", return sem registo
                    if (tc.includes("nao foram encontrados resultados") || tc.includes("nao existe nenhum animal")) return "siac_not_registered";
                    
                    // Conclusive boxes (SIAC uses specific alert classes usually)
                    const hasResultBox = document.querySelector('.alert-success, .alert-danger, .alert-warning, .card-body');
                    if (!hasResultBox && !tc.includes("resultado")) return "polling";

                    // Result logic
                    if (tc.includes("encontra desaparecido") || tc.includes("animal desaparecido")) return "siac_missing";
                    if (tc.includes("com registo") || tc.includes("animal com registo")) return "siac_registered";
                    if (tc.includes("sem registo") || tc.includes("animal sem registo")) return "siac_not_registered";
                    
                    return "polling";
                }}""")
                
                if res != "polling": 
                    log(f"Resultado final detectado: {res}")
                    return res
                
                if time.time() - start > 12 and not tried_click:
                    log("Ainda sem resposta... forçando clique novamente.")
                    await page.keyboard.press("Enter")
                    tried_click = True
                await asyncio.sleep(2)
            log("Timeout no portal SIAC (40s).")
            return "siac_error"
        except Exception as e:
            log(f"Erro: {str(e)[:50]}")
            if attempt == 0: await page.reload()
    return "siac_error"

async def check_olx_km(page, ad_id: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[OLX] {m}")
    cid = normalize_id(ad_id)
    url = f"{OLX_BASE_URL}{cid}"
    try:
        log(f"Acedendo anúncio {cid}...")
        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
        await asyncio.sleep(4)
        km = await page.evaluate("""() => {
            const m = document.body.innerText.match(/(\\d[\\d\\s\\.,]*)\\s*km/i);
            return m ? m[0].trim() : null;
        }""")
        if km: 
            log(f"Encontrado: {km}")
            return km
        content = (await page.content()).lower()
        if "não está disponível" in content or "moderado" in content: return "ERR_MODERATED"
        if "inativo" in content or "removido" in content: return "ERR_INACTIVE"
        return "ERR_NOT_FOUND"
    except Exception as e: 
        log(f"Erro: {str(e)[:40]}")
        return "⚠️ Erro OLX"

async def check_olx_location(page, ad_id: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[OLX] {m}")
    cid = normalize_id(ad_id)
    url = f"{OLX_BASE_URL}{cid}"
    try:
        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
        await asyncio.sleep(4)
        loc = await page.evaluate("""() => {
            const selectors = ['span[data-testid="location-label"]', 'a[data-testid="ad-location-link"]'];
            for (let s of selectors) {
                const el = document.querySelector(s);
                if (el && el.innerText.length > 3) return el.innerText.trim();
            }
            return null;
        }""")
        if loc: log(f"Localização: {loc}")
        return loc or "❓ Localização"
    except: return "⚠️ Erro OLX"

async def check_rnt_rnal_only(page, reg_id: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[RNT] {m}")
    rid = normalize_id(reg_id)
    url = f"{RNT_AL_DIRECT_URL}{rid}"
    try:
        log(f"Validando no RNT ID {rid}...")
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(8)
        # Surgical frame extract
        elements = [page] + page.frames
        for target in elements:
            try:
                morada = await target.evaluate("""() => {
                    const findValue = (label) => {
                        const all = Array.from(document.querySelectorAll('.TableRecords_Label, td, span, div, b'));
                        const l = all.find(el => {
                            const t = el.innerText.trim();
                            // STRICT match for "Morada" only, excluding emails or other labels
                            return (t === label || t === label + ':') && 
                                   !el.innerText.toLowerCase().includes('email') &&
                                   !el.innerText.toLowerCase().includes('electronico');
                        });
                        if (!l) return null;
                        
                        // Check if it's a table with value in next TD
                        if (l.tagName === 'TD' && l.nextElementSibling) {
                             const val = l.nextElementSibling.innerText.trim();
                             if (val.includes('@') && !val.includes(' ')) return null; // Skip if looks like email
                             return val;
                        }
                        
                        // Case: Morada: Value in same block
                        const pText = l.parentElement.innerText;
                        const match = pText.match(new RegExp(label + ":?\\\\s*([^\\\\n@]+)", "i"));
                        if (match && match[1].trim().length > 5) return match[1].trim();
                        
                        return null;
                    };
                    
                    const m = findValue('Morada');
                    if (m && m.length > 5 && (m.includes(' ') || !m.includes('@'))) return m;
                    return null;
                }""")
                if morada: 
                    log(f"Morada encontrada: {morada[:50]}...")
                    return morada.strip()
            except: continue
        return "❓ Sem Dados"
    except: return "⚠️ Erro RNT"

# --- BROWSER INSTALLER ---
def install_playwright_browsers():
    """Hook para garantir que os browsers estão instalados no Streamlit Cloud."""
    if "browsers_installed" not in st.session_state:
        with st.spinner("🔧 A configurar ambiente (Playwright)..."):
            try:
                import subprocess
                import sys
                # Usar sys.executable garante que usamos o mesmo python do streamlit
                # --with-deps tenta instalar dependências de sistema se possível
                cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    st.session_state.browsers_installed = True
                else:
                    st.error(f"Erro na instalação: {result.stderr}")
                    # Tenta apenas o install sem o -m se falhar
                    subprocess.run(["playwright", "install", "chromium"], check=True)
                    st.session_state.browsers_installed = True
            except Exception as e:
                st.error(f"Erro Crítico: {e}")
                st.info("Dica: Adicione 'playwright' ao requirements.txt e 'libnss3' ao packages.txt")

# --- ENGINE (STABLE RESTORED + CLOUD FIX) ---
async def process_list_incremental(items, checker_func, ws, col_mappings, init_url=None, refresh_every=50, existing_data=None, **extra_params):
    install_playwright_browsers()
    total = len(items)
    pb = st.progress(0)
    st.subheader("🖥️ Console Debug LIVE")
    console = st.code("Iniciando motor Playwright...", language="text")
    logs = []
    def log(m):
        logs.append(f"> {m}")
        console.code("\n".join(logs[-15:]))

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True, 
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--single-process"]
            )
        except Exception as e:
            log(f"Falha ao iniciar Chromium: {str(e)}")
            st.error("Erro no Playwright. Certifique-se de que os browsers estão instalados.")
            return []

        context = await browser.new_context(user_agent="Mozilla/5.0 Windows NT 10.0; Win64; x64")
        page = await context.new_page()
        
        for i, val in enumerate(items):
            # Granular Skip Logic
            to_skip = False
            if existing_data and i < len(existing_data):
                raw = existing_data[i]
                # SIAC (interleaved)
                if col_mappings == [9, 10]:
                    if str(raw).strip() in [t("siac_registered"), t("siac_not_registered"), t("siac_missing")]: to_skip = True
                # RNAL (check validation column)
                elif col_mappings == [3, 5, 6]:
                    if isinstance(raw, (list, tuple)) and len(raw) > 2:
                        if str(raw[2]).strip() in [t("val_correct"), t("val_wrong")]: to_skip = True
                # OLX
                elif col_mappings == [4, 5]:
                    if isinstance(raw, (list, tuple)) and len(raw) > 2:
                        if str(raw[2]).strip() in [t("km_fixed"), "CORRECTO"]: to_skip = True
                    elif str(raw).strip() not in ["...", "N/A", "", "nan"]: to_skip = True
            
            if to_skip:
                log(f"Linha {i+2}: Já validada."); pb.progress((i+1)/total); continue

            if i > 0 and i % refresh_every == 0:
                log("Refresh periódico do contexto...")
                await context.close(); context = await browser.new_context(); page = await context.new_page()
            
            cid = normalize_id(val[0] if isinstance(val, (tuple, list)) else val)
            if not cid: res = "N/A"
            else:
                try:
                    if page.is_closed(): 
                        context = await browser.new_context(); page = await context.new_page()
                    res = await checker_func(page, cid if not isinstance(val, (tuple, list)) else val, log_func=log, **extra_params)
                except Exception as e:
                    log(f"ERRO DE MOTOR: {str(e)[:50]}"); res = "Error"
            
            # ATUALIZAÇÃO DIRETA NO SHEETS
            try:
                row = (i // 2 if col_mappings == [9, 10] else i) + 2
                if isinstance(res, (tuple, list)):
                    ws.update_cell(row, col_mappings[0], res[0])
                    if len(res) > 1: ws.update_cell(row, col_mappings[1], res[1])
                    if len(col_mappings) > 2:
                        o, r = str(res[0]).lower(), str(res[1]).lower()
                        v = t("val_waiting") if "sem dados" in r or not r else (t("val_correct") if any(w in r for w in o.split() if len(w)>3) else t("val_wrong"))
                        ws.update_cell(row, col_mappings[2], v)
                else:
                    if res != "SKIP":
                        vw = t(res) if "siac_" in str(res) else res
                        tc = col_mappings[0] if i % 2 == 0 else col_mappings[1] if len(col_mappings) > 1 else col_mappings[0]
                        ws.update_cell(row, tc, vw)
            except: pass
            pb.progress((i+1)/total)
        await browser.close()

# --- UI LOGIC (FULL BUTTONS RESTORED) ---
st.title(t("title"))
st.markdown(t("subtitle"))
t_siac, t_rnt, t_olx = st.tabs([t("siac_tab"), t("rnal_tab"), t("olx_tab")])

with t_siac:
    st.subheader(t("siac_sub"))
    st.info(t("dica_siac"))
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if st.button(t("btn_start"), key="run_siac", use_container_width=True):
            gc = get_gspread_client()
            if gc:
                sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "AUTO SIAC")
                if ws:
                    f, c = ws.col_values(7)[1:], ws.col_values(8)[1:]
                    rf, rc = ws.col_values(9)[1:], ws.col_values(10)[1:]
                    items, exists = [], []
                    for i in range(max(len(f), len(c))):
                        if i < len(f): items.append(f[i]); exists.append(rf[i] if i < len(rf) else "...")
                        if i < len(c): items.append(c[i]); exists.append(rc[i] if i < len(rc) else "...")
                    asyncio.run(process_list_incremental(items, check_siac_on_page, ws, [9, 10], init_url=SIAC_URL, existing_data=exists))
                    st.success(t("status_done"))
    with col_s2:
        if st.button(t("btn_clear_reg"), key="clear_siac", use_container_width=True):
            st.info(t("cleaning"))
            gc = get_gspread_client()
            if gc:
                sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "AUTO SIAC")
                if ws:
                    f, c = ws.col_values(9)[1:], ws.col_values(10)[1:]
                    count = 0
                    reg_text = t("siac_registered")
                    # Iterate backwards to avoid index shifting
                    for i in range(len(f) - 1, -1, -1):
                        if i < len(c) and f[i] == reg_text and c[i] == reg_text:
                            ws.delete_rows(i + 2)
                            count += 1
                    st.success(t("rows_removed", count))

with t_rnt:
    st.subheader(t("rnal_sub"))
    st.info(t("dica_rnal"))
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button(t("btn_start"), key="run_rnt", use_container_width=True):
            gc = get_gspread_client()
            if gc:
                sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "AUTO RNAL")
                if ws:
                    ads, regs = ws.col_values(1)[1:], ws.col_values(4)[1:]
                    lo, lr, v = ws.col_values(3)[1:], ws.col_values(5)[1:], ws.col_values(6)[1:]
                    items, exists = [], []
                    for i in range(len(ads)):
                        items.append((ads[i], regs[i]))
                        exists.append((lo[i] if i<len(lo) else "", lr[i] if i<len(lr) else "", v[i] if i<len(v) else ""))
                    
                    async def al_checker(p, val, log_func):
                        ad, reg = val
                        lo = await check_olx_location(p, ad, log_func=log_func)
                        lr = await check_rnt_rnal_only(p, reg, log_func=log_func)
                        return lo, lr
                    asyncio.run(process_list_incremental(items, al_checker, ws, [3, 5, 6], existing_data=exists))
                    st.success(t("status_done"))
    with col_r2:
        if st.button(t("btn_clear_loc"), key="clear_rnt", use_container_width=True):
            st.info(t("cleaning"))
            gc = get_gspread_client()
            if gc:
                sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "AUTO RNAL")
                if ws:
                    vals = ws.get_all_values()
                    count = 0
                    for i, row in enumerate(vals[1:], 2):
                        if i <= len(vals) and len(row) >= 6 and row[5] == t("val_correct"):
                            ws.update_cell(i, 3, ""); ws.update_cell(i, 5, ""); ws.update_cell(i, 6, "")
                            count += 1
                    st.success(t("rows_removed", count))

with t_olx:
    st.subheader(t("olx_sub"))
    st.info(t("dica_olx"))
    col_o1, col_o2 = st.columns(2)
    with col_o1:
        if st.button(t("btn_start"), key="run_olx", use_container_width=True):
            gc = get_gspread_client()
            if gc:
                sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "KM CARROS")
                if ws:
                    ads = ws.col_values(1)[1:]; kms = ws.col_values(4)[1:]; v = ws.col_values(5)[1:]
                    items, exists = [], []
                    for i in range(len(ads)):
                        items.append(ads[i])
                        exists.append((kms[i] if i<len(kms) else "", "", v[i] if i<len(v) else ""))
                    asyncio.run(process_list_incremental(items, check_olx_km, ws, [4, 5], existing_data=exists))
                    st.success(t("status_done"))
    with col_o2:
        if st.button(t("btn_clear_mod"), key="clear_olx", use_container_width=True):
            st.info(t("cleaning"))
            gc = get_gspread_client()
            if gc:
                sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "KM CARROS")
                if ws:
                    vals = ws.get_all_values()
                    count = 0
                    # Iterate backwards to avoid index shifting
                    for i in range(len(vals) - 1, 0, -1):
                        row = vals[i]
                        if len(row) >= 5 and (row[4] == t("km_fixed") or row[4] == t("km_moderated") or row[4] == t("km_inactive")):
                            ws.delete_rows(i + 1)
                            count += 1
                    st.success(t("rows_removed", count))
