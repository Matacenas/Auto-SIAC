import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from typing import List, Optional, Callable, Awaitable, Any

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
RNT_ET_URL = "https://rnt.turismodeportugal.pt/RNT/Pesquisa_ET.aspx"

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
        "dica_rnal": "💡 **Processo de Validação:**\n\n1. Lê o ID do anúncio na Coluna A.\n2. Faz scraping da localização do anúncio em olx.pt e regista o resultado na Coluna C.\n3. Lê o Número de Alojamento Local da Coluna D e valida no site do RNAL:\n   https://rnt.turismodeportugal.pt/RNT/RNAL.aspx?nr=AdID\n4. Faz scraping do resultado da validação e regista a informação na Coluna E.\n5. Compara a localização do OLX com a do RNAL e regista a sugestão na Coluna F.",
        "dica_olx": "💡 **Processo de Validação:**\n\n1. Lê o ID do anúncio na Coluna A.\n2. Acede a olx.pt e faz scraping dos quilómetros apresentados no anúncio (LIVE).\n3. Regista os quilómetros obtidos na Coluna D.\n4. Compara os valores da Coluna C com os da Coluna D.\n5. Regista o resultado da validação na Coluna E.",
        "restarting_browser": "♻️ Reiniciando navegador para estabilidade...",
        "val_waiting": "⚠️ Sem resultado - Confirmar no RNET ⚠️",
        "val_correct": "✅Localização Correcta ✅",
        "val_wrong": "❌ Localização Errada ❌",
        "km_wrong": "❌ KM errados ❌",
        "km_fixed": "✅ KM corrigidos pelo user ✅",
        "km_missing_param": "Parâmetro não preenchido",
        "km_moderated": "⚠️ Anúncio já foi moderado ⚠️",
        "km_inactive": "⚠️ Anúncio inactivo ⚠️",
        "cleaning": "Limpando linhas (Sincronizando com a folha)...",
        "rows_removed": "Removidas {} linhas!",
        "no_rows": "Nenhuma linha para remover.",
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
        "restarting_browser": "♻️ Restarting browser for stability...",
        "val_waiting": "⚠️ No result - Confirm on RNET ⚠️",
        "val_correct": "✅ Correct Location ✅",
        "val_wrong": "❌ Wrong Location ❌",
        "km_wrong": "❌ Incorrect Kilometers ❌",
        "km_fixed": "✅ Kilometers corrected by user ✅",
        "km_missing_param": "Parameter not filled",
        "km_moderated": "⚠️ Ad Already Moderated ⚠️",
        "km_inactive": "⚠️ Ad Inactive ⚠️",
        "cleaning": "Cleaning rows (Syncing with sheet)...",
        "rows_removed": "Removed {} rows!",
        "no_rows": "No rows to remove.",
        "footer": "Multi-Project Auto Validation 2026"
    }
}

if "lang" not in st.session_state: st.session_state.lang = "PT"

def t(key, *args):
    text = TRANSLATIONS[st.session_state.lang].get(key, key)
    if args: return text.format(*args)
    return text

st.set_page_config(page_title="Validação Automática", page_icon="🚀", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    /* Shrink the sidebar a bit */
    [data-testid="stSidebar"] { min-width: 280px; max-width: 320px; }
    /* Make Alert/Info boxes shrink to text width and look more professional */
    [data-testid="stNotification"] { width: fit-content !important; min-width: 300px; max-width: 100%; border-radius: 10px; }
    .stAlert { width: fit-content !important; min-width: 300px; max-width: 100%; border-radius: 10px; padding: 10px 20px; }
    
    /* ENLARGE TABS AND SHRINK BORDER */
    button[data-baseweb="tab"] { 
        font-size: 24px !important; 
        height: 60px !important; 
        font-weight: bold !important;
        padding-left: 20px !important;
        padding-right: 20px !important;
    }
    button[data-baseweb="tab"] p { font-size: 24px !important; }
    
    /* Shrink the underline of the tabs and unified dividers */
    div[data-baseweb="tab-list"] { width: 500px !important; }
    div[data-baseweb="tab-border"] { width: 500px !important; }
    
    /* UNIFY ALL DIVIDERS AND TIP BOXES (~500px) */
    hr { width: 500px !important; margin-left: 0 !important; border-top: 2px solid #555; }
    [data-testid="stNotification"] { width: 500px !important; border-radius: 10px; }
    .stAlert { width: 500px !important; border-radius: 10px; padding: 10px 20px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header(t("sidebar_config"))
    
    # Language Switcher - Vertical with Flag + Text (Final Visual Balancing)
    # PT
    colx1, colx2 = st.columns([1, 4])
    with colx1: 
        st.markdown("""
            <div style='display: flex; align-items: center; justify-content: center; height: 38px;'>
                <img src='https://flagcdn.com/w80/pt.png' style='height: 26px; border-radius: 2px; box-shadow: 0 0 2px rgba(0,0,0,0.5);'>
            </div>
        """, unsafe_allow_html=True)
    with colx2:
        if st.button("Português", key="btn_pt_final_v6", use_container_width=True):
            st.session_state.lang = "PT"; st.rerun()
    # EN
    coly1, coly2 = st.columns([1, 4])
    with coly1: 
        st.markdown("""
            <div style='display: flex; align-items: center; justify-content: center; height: 38px;'>
                <img src='https://flagcdn.com/w80/gb.png' style='height: 26px; border-radius: 2px; box-shadow: 0 0 2px rgba(0,0,0,0.5);'>
            </div>
        """, unsafe_allow_html=True)
    with coly2:
        if st.button("English", key="btn_en_final_v6", use_container_width=True):
            st.session_state.lang = "EN"; st.rerun()
    
    st.divider()
    saved_links = load_links()

    # Consolidated URL - Larger Label
    st.markdown(f"### {t('gs_url_label')}")
    current_gs = saved_links.get("gs_url") or GLOBAL_DEFAULT_URL
    url_gs = st.text_input("", value=current_gs, label_visibility="collapsed")
    if url_gs != saved_links.get("gs_url", ""): save_link("gs_url", url_gs)
    
    if url_gs:
        st.link_button(t("btn_open_sheet"), url_gs, use_container_width=True)

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

def get_worksheet_by_name(sh, target_name):
    """Try to find worksheet by name (case-insensitive). Return None if not found."""
    try:
        titles = [ws.title for ws in sh.worksheets()]
        best_match = next((t for t in titles if t.strip().lower() == target_name.strip().lower()), None)
        if best_match:
            return sh.worksheet(best_match)
        return None
    except Exception as e:
        print(f"Erro ao procurar aba: {e}")
        return None

def batch_clear_rows(ws, rows, condition_func):
    """Efficiently clear rows matching a condition by filtering and overwriting."""
    if not rows: return 0
    header = rows[0]
    new_rows = [header]
    deleted_count = 0
    for row in rows[1:]:
        if not condition_func(row):
            new_rows.append(row)
        else:
            deleted_count += 1
    if deleted_count > 0:
        ws.clear()
        ws.update(range_name='A1', values=new_rows)
    return deleted_count

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
    """Validates car mileage on OLX with very robust text-based searching."""
    if str(ad_id).isdigit():
        ad_url = f"{OLX_BASE_URL}{ad_id}"
    else:
        ad_url = ad_id if str(ad_id).startswith('http') else f"{OLX_BASE_URL}d/anuncio/{ad_id}.html"

    for attempt in range(retries + 1):
        try:
            await page.goto(ad_url, timeout=45000, wait_until="domcontentloaded")
            await asyncio.sleep(5) 
            
            km_val = await page.evaluate("""
                () => {
                    const findInText = (text) => {
                        const m = text.match(/(\\d[\\d\\s\\.,]*)\\s*km/i);
                        return m ? m[0].trim() : null;
                    };

                    const specItems = Array.from(document.querySelectorAll('li, div[data-testid="ad_properties_item"], .ad-properties__item'));
                    for (const item of specItems) {
                        const t = item.innerText || "";
                        if (t.includes('Quilómetros')) {
                            const val = findInText(t);
                            if (val) return val;
                            const children = Array.from(item.querySelectorAll('span, p, div'));
                            for (const c of children) {
                                const v = findInText(c.innerText);
                                if (v) return v;
                            }
                        }
                    }

                    const bodyText = document.body.innerText;
                    const lines = bodyText.split('\\n');
                    for (let i = 0; i < lines.length; i++) {
                        if (lines[i].includes('Quilómetros')) {
                            for (let j = i; j <= i + 3 && j < lines.length; j++) {
                                const val = findInText(lines[j]);
                                if (val) return val;
                            }
                        }
                    }

                    const genericMatch = bodyText.match(/(\\d[\\d\\s\\.,]*)\\s*km/i);
                    return genericMatch ? genericMatch[0].trim() : null;
                }
            """)
            if km_val: return km_val
            
            content = (await page.content()).lower()
            if "já não está disponível" in content or "already moderated" in content:
                # We return a specific code or the translated string directly if we want it in Col D
                # However, to maintain translation in Col D, we must use the t() function inside the checker
                # but t() is available in the UI loop. Let's return a unique string that the checker maps.
                return "ERR_MODERATED"
            if "ups, algo não está bem" in content or "inactive" in content.lower():
                return "ERR_INACTIVE"
            if "não se encontra disponível" in content or "anúncio removido" in content or "removed" in content.lower():
                return "ERR_INACTIVE"
            if attempt < retries: await asyncio.sleep(2); continue
            return "ERR_NOT_FOUND"
        except:
            if attempt < retries: await asyncio.sleep(2); continue
            return "⚠️ Erro Conexão"
    return "⚠️ Erro"

async def check_olx_location(page, ad_id: str, retries: int = 2) -> str:
    """Extracts location from OLX ad."""
    if not ad_id or str(ad_id).lower() == 'nan': return "N/A"
    
    if str(ad_id).isdigit():
        ad_url = f"{OLX_BASE_URL}{ad_id}"
    else:
        ad_url = ad_id if str(ad_id).startswith('http') else f"{OLX_BASE_URL}d/anuncio/{ad_id}.html"

    for attempt in range(retries + 1):
        try:
            await page.goto(ad_url, timeout=45000, wait_until="domcontentloaded")
            await asyncio.sleep(4)
            
            location = await page.evaluate("""
                () => {
                    const blacklist = ['LOCALIZAÇÃO', 'MAP DATA', 'CLICK TO TOGGLE', 'METRIC', 'IMPERIAL', 'UNITS', '©', 'LOJA', 'GEOGR'];
                    
                    const isMetadata = (text) => {
                        if (!text) return true;
                        const t = text.trim();
                        // Reject if contains distance pattern (e.g. "1 km", "500 m")
                        if (/\\d+.*km/i.test(t) || /\\d+.*m\\s*$/i.test(t)) return true;
                        // Reject if contains blacklist words
                        const up = t.toUpperCase();
                        return blacklist.some(b => up.includes(b));
                    };

                    const surgicalExtract = (container) => {
                        if (!container) return null;
                        // Find all direct or deep text nodes/spans
                        const elements = Array.from(container.querySelectorAll('span, a, p'))
                            .map(el => el.innerText.trim())
                            .filter(t => t.length > 2 && !isMetadata(t));
                        
                        // Deduplicate and join first 2 unique parts
                        const unique = [...new Set(elements)];
                        if (unique.length > 0) return unique.slice(0, 2).join(' - ');
                        return null;
                    };

                    // Priority 1: Direct location link
                    const locLink = document.querySelector('a[data-testid="ad-location-link"]');
                    if (locLink) {
                        const res = surgicalExtract(locLink);
                        if (res) return res;
                    }

                    // Priority 2: Section search (fallback)
                    const all = Array.from(document.querySelectorAll('span, p, a, div, h2, h3'));
                    const header = all.find(el => el.innerText && el.innerText.trim().toUpperCase() === 'LOCALIZAÇÃO');
                    if (header) {
                        let parent = header.parentElement;
                        // Go up a few levels to find the container
                        for (let i = 0; i < 3 && parent; i++) {
                            const res = surgicalExtract(parent);
                            if (res) return res;
                            parent = parent.parentElement;
                        }
                    }
                    
                    return null;
                }
            """)
            if location: return location
            if attempt < retries: await asyncio.sleep(2); continue
            return "❓ Localização"
        except:
            if attempt < retries: await asyncio.sleep(2); continue
            return "⚠️ Conexão"
    return "⚠️ Erro"

async def check_rnt_rnal_only(page, reg_id: str, retries: int = 1) -> str:
    """Validates registration in RNAL (Direct detail) with grid fallback."""
    res = "❓ Sem Dados"
    rnal_url = f"{RNT_AL_DIRECT_URL}{reg_id}"
    for attempt in range(retries + 1):
        try:
            await page.goto(rnal_url, timeout=45000, wait_until="networkidle")
            await asyncio.sleep(3) # Give it time to load the record
            
            # Try Detail Page Strategy first
            details = await page.evaluate("""
                () => {
                    const getText = (label) => {
                        const all = Array.from(document.querySelectorAll('span, label, td, b, p, div'));
                        const target = all.find(l => {
                            const t = l.innerText ? l.innerText.trim() : "";
                            return t === label || t === label + ":" || t.startsWith(label + ":");
                        });
                        if (target) {
                            const parent = target.parentElement;
                            if (parent) {
                                let text = parent.innerText.replace(label, '').replace(':', '').trim();
                                if (text.length > 1) return text;
                            }
                            const next = target.nextElementSibling;
                            if (next) return next.innerText.trim();
                        }
                        return null;
                    };
                    
                    const address = getText('Morada') || getText('Localização');
                    const concelho = getText('Concelho');
                    const freguesia = getText('Freguesia');
                    
                    if (address) {
                        return address + (freguesia ? ' - ' + freguesia : '') + (concelho ? ' (' + concelho + ')' : '');
                    }
                    return null;
                }
            """)
            if details:
                res = details
                break
                
            # Fallback: Grid Strategy (if it lands on search results)
            grid_res = await page.evaluate("""
                () => {
                    const row = document.querySelector('tr.GridRow, tr.GridAlternatingRow, .GridView tr:nth-child(2), table tr:nth-child(2)');
                    if (row) {
                        const cells = Array.from(row.querySelectorAll('td'));
                        if (cells.length > 2) {
                            // Usually the location is the second to last column or specific column
                            // We can try to guess or just join relevant info
                            const text = cells.map(c => c.innerText.trim()).filter(t => t.length > 2).join(' | ');
                            // Return the last relevant cell if it's long enough
                            return cells[cells.length - 2].innerText.trim() + " (" + cells[cells.length-3].innerText.trim() + ")";
                        }
                    }
                    return null;
                }
            """)
            if grid_res:
                res = grid_res
                break

            if "não foram encontrados" in (await page.content()).lower():
                res = "❌ Não Encontrado"
                break
            if attempt < retries: await asyncio.sleep(2)
        except: 
            if attempt < retries: await asyncio.sleep(2)
            else: res = "⚠️ Erro RNAL"
    return res

# --- CORE ENGINE ---

async def process_list_incremental(
    items: List[Any], 
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
            # Skip if we have a result. Placeholder "..." or empty string doesn't count.
            has_result = isinstance(results[i], str) and results[i].strip() not in ["", "..."]
            if i < len(results) and has_result:
                progress_bar.progress((i + 1) / total)
                continue

            if i > 0 and i % refresh_every == 0:
                status_text.text(t("restarting_browser"))
                if page: await page.close()
                if context: await context.close()
                if browser: await browser.close()
                await init_browser()

            # Improved Cleaning: Handle tuples/lists vs single values
            if isinstance(val, (tuple, list)):
                cleaned = val # Preserve for complex checkers
                status_display = str(val[0])
            else:
                raw_str = str(val).strip()
                if raw_str.endswith(".0"): cleaned = raw_str[:-2]
                else: cleaned = raw_str
                status_display = cleaned
            
            check_val = val[0] if isinstance(val, (tuple, list)) else val
            if not str(check_val).strip() or str(check_val).lower() == "nan": res = "N/A"
            else:
                status_text.text(t("status_working", status_display))
                res = await checker_func(page, cleaned, **extra_params)
            
            results[i] = res
            progress_bar.progress((i + 1) / total)
            if callback and (i + 1) % batch_size == 0: await callback(results)
            
        if callback: await callback(results)
        if browser: await browser.close()
    return results

# --- UI LOGIC ---
st.title(t("title"))
st.markdown(t("subtitle"))

tab_siac, tab_rnt, tab_olx = st.tabs([t("siac_tab"), t("rnal_tab"), t("olx_tab")])

# --- TAB: SIAC ---
with tab_siac:
    st.subheader(t("siac_sub"))
    st.info(t("dica_siac"))

    if st.button(t("btn_start"), key="btn_run_siac"):
        if not url_gs: st.warning(t("err_no_url"))
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_gs)
                    ws = get_worksheet_by_name(sh, "AUTO SIAC")
                    if not ws:
                        st.error("ERRO: Aba 'AUTO SIAC' não encontrada no ficheiro!")
                        st.stop()
                    femeas = ws.col_values(7)[1:] # G
                    crias = ws.col_values(8)[1:]  # H
                    res_femeas = ws.col_values(9)[1:] # I
                    res_crias = ws.col_values(10)[1:] # J
                    
                    rows = max(len(femeas), len(crias))
                    interleaved = []
                    existing_results = []
                    
                    for i in range(rows):
                        # Femea
                        if i < len(femeas): 
                            interleaved.append(femeas[i])
                            existing_results.append(res_femeas[i] if i < len(res_femeas) else "...")
                        # Cria
                        if i < len(crias): 
                            interleaved.append(crias[i])
                            existing_results.append(res_crias[i] if i < len(res_crias) else "...")
                    
                    async def update_siac_gs(res):
                        ptr, sf, sc = 0, [], []
                        for i in range(rows):
                            if i < len(femeas): sf.append([res[ptr]]); ptr += 1
                            else: sf.append(["N/A"])
                            if i < len(crias): sc.append([res[ptr]]); ptr += 1
                            else: sc.append(["N/A"])
                        gc_i = get_gspread_client()
                        if gc_i:
                            sh_i = gc_i.open_by_url(url_gs)
                            ws_i = get_worksheet_by_name(sh_i, "AUTO SIAC")
                            if ws_i:
                                ws_i.update(range_name=f"I2:I{1+len(sf)}", values=sf)
                                ws_i.update(range_name=f"J2:J{1+len(sc)}", values=sc)

                    with st.spinner(""):
                        asyncio.run(process_list_incremental(interleaved, check_siac_on_page, init_url=SIAC_URL, callback=update_siac_gs, existing_results=existing_results))
                    st.success(t("status_done"))
                except Exception as e: st.error(f"Erro: {e}")

    # --- BUTTON: CLEAR SIAC ---
    if st.button(t("btn_clear_reg"), key="btns_clear_siac"):
        if not url_gs: st.warning(t("err_no_url"))
        else:
            try:
                gc = get_gspread_client()
                if gc:
                    sh = gc.open_by_url(url_gs)
                    ws = get_worksheet_by_name(sh, "AUTO SIAC")
                    with st.spinner(t("cleaning")):
                        data = ws.get_all_values()
                        def is_siac_done(row):
                            if len(row) < 10: return False
                            # Exact match only, no alerts
                            return row[8].strip() == "✅ REGISTADO" and row[9].strip() == "✅ REGISTADO"
                        
                        count = batch_clear_rows(ws, data, is_siac_done)
                        if count > 0: st.success(t("rows_removed", count))
                        else: st.info(t("no_rows"))
            except Exception as e: st.error(f"Erro ao limpar: {e}")

# --- TAB: RNT ---
with tab_rnt:
    st.subheader(t("rnal_sub"))
    st.info(t("dica_rnal"))
    
    if st.button(t("btn_start"), key="btn_run_rnt"):
        if not url_gs: st.warning(t("err_no_url"))
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_gs)
                    ws = get_worksheet_by_name(sh, "AUTO RNAL")
                    if not ws:
                        st.error("ERRO: Aba 'AUTO RNAL' não encontrada no ficheiro!")
                        st.stop()
                    olx_ids = ws.col_values(1)[1:] # A
                    rnal_ids = ws.col_values(4)[1:] # D
                    existing_val = ws.col_values(6)[1:] # F
                    
                    # Pad lists
                    max_len = max(len(olx_ids), len(rnal_ids))
                    olx_ids += [""] * (max_len - len(olx_ids))
                    rnal_ids += [""] * (max_len - len(rnal_ids))
                    existing_val += ["..."] * (max_len - len(existing_val))
                    
                    async def update_al_gs(results):
                        # results is a list of (olx_loc, rnal_loc)
                        gc_u = get_gspread_client()
                        if gc_u:
                            sh_u = gc_u.open_by_url(url_gs)
                            ws_u = get_worksheet_by_name(sh_u, "AUTO RNAL")
                            if ws_u:
                                olx_formatted = [[r[0]] for r in results]
                                rnal_formatted = [[r[1]] for r in results]
                                val_formatted = []
                                for r in results:
                                    if isinstance(r, str):
                                        val_formatted.append([r])
                                        continue
                                    olx_l, rnt_l = str(r[0]).lower(), str(r[1]).lower()
                                    if rnt_l == "n/a" or not rnt_l or "sem dados" in rnt_l:
                                        val_formatted.append([t("val_waiting")])
                                    elif any(s in str(r[0]) or s in str(r[1]) for s in ["...", "⚠️", "❓"]):
                                        val_formatted.append(["..."])
                                    elif olx_l != "n/a" and any(word in rnt_l for word in olx_l.split() if len(word) > 3): 
                                        val_formatted.append([t("val_correct")])
                                    else: val_formatted.append([t("val_wrong")])
                                    
                                ws_u.update(range_name=f"C2:C{1+len(olx_formatted)}", values=olx_formatted) # OLX Loc
                                ws_u.update(range_name=f"E2:E{1+len(rnal_formatted)}", values=rnal_formatted) # RNAL Data
                                ws_u.update(range_name=f"F2:F{1+len(val_formatted)}", values=val_formatted) # Validation
                    
                    async def al_checker(page, ids_tuple):
                        o_id, r_id = ids_tuple
                        olx_loc = await check_olx_location(page, o_id)
                        rnt_data = await check_rnt_rnal_only(page, r_id)
                        return (olx_loc, rnt_data)

                    with st.spinner(""):
                        combined_ids = list(zip(olx_ids, rnal_ids))
                        asyncio.run(process_list_incremental(combined_ids, al_checker, callback=update_al_gs, existing_results=existing_val))
                    st.success(t("status_done"))
                    st.balloons()
                except Exception as e: st.error(f"Erro: {e}")

    # --- BUTTON: CLEAR RNAL ---
    if st.button(t("btn_clear_loc"), key="btn_clear_rnal"):
        if not url_gs: st.warning(t("err_no_url"))
        else:
            try:
                gc = get_gspread_client()
                if gc:
                    sh = gc.open_by_url(url_gs)
                    ws = get_worksheet_by_name(sh, "AUTO RNAL")
                    with st.spinner(t("cleaning")):
                        data = ws.get_all_values()
                        def is_rnal_done(row):
                            if len(row) < 6: return False
                            return row[5].strip() == t("val_correct")
                        
                        count = batch_clear_rows(ws, data, is_rnal_done)
                        if count > 0: st.success(t("rows_removed", count))
                        else: st.info(t("no_rows"))
            except Exception as e: st.error(f"Erro ao limpar: {e}")

# --- TAB: OLX ---
with tab_olx:
    st.subheader(t("olx_sub"))
    st.info(t("dica_olx"))
    
    if st.button(t("btn_start"), key="btn_run_olx"):
        if not url_gs: st.warning(t("err_no_url"))
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_gs)
                    ws = get_worksheet_by_name(sh, "Auto Km")
                    if not ws:
                        st.error("ERRO: Aba 'Auto Km' não encontrada no ficheiro!")
                        st.stop()
                    ids = ws.col_values(1)[1:] # Column A
                    system_km = ws.col_values(3)[1:] # Col C (User provided)
                    existing_val = ws.col_values(5)[1:] # Col E
                    
                    # Pad lists to ensure same length
                    max_len = max(len(ids), len(system_km))
                    ids += [""] * (max_len - len(ids))
                    system_km += [""] * (max_len - len(system_km))
                    existing_val += ["..."] * (max_len - len(existing_val))
                    
                    async def update_cars_gs(results):
                        # results is a list of (found_km, validation)
                        gc_u = get_gspread_client()
                        if gc_u:
                            sh_u = gc_u.open_by_url(url_gs)
                            ws_u = get_worksheet_by_name(sh_u, "Auto Km")
                            if ws_u:
                                bot_km_fmt = [[r[0]] for r in results]
                                val_fmt = [[r[1]] for r in results]
                                ws_u.update(range_name=f"D2:D{1+len(bot_km_fmt)}", values=bot_km_fmt) # Col D
                                ws_u.update(range_name=f"E2:E{1+len(val_fmt)}", values=val_fmt) # Col E
                    
                    async def cars_checker(page, id_val_tuple):
                        ad_id, sys_km_raw = id_val_tuple
                        # Get found KM from OLX
                        found_km_str = await check_olx_km(page, ad_id)
                        
                        # Compare with sys_km_raw
                        sys_km = str(sys_km_raw).replace(' ', '').replace('.', '').replace(',', '').strip()
                        found_km_clean = found_km_str.replace('km', '').replace(' ', '').replace('.', '').replace(',', '').strip().lower()
                        
                        validation = "..."
                        if sys_km:
                            sys_km_clean = "".join(filter(str.isdigit, str(sys_km)))
                        else:
                            sys_km_clean = ""

                        # Map internal codes to translated strings
                        if found_km_str == "ERR_MODERATED":
                            found_km_str = t("km_moderated")
                            validation = found_km_str
                        elif found_km_str in ["ERR_INACTIVE", "🚫 Inativo/Vendido"]:
                            found_km_str = t("km_inactive")
                            validation = found_km_str
                        elif found_km_str == "ERR_NOT_FOUND":
                            found_km_str = "❓ Km não encontrado"
                            validation = t("km_missing_param")
                        
                        if validation == "...": # If not already set by error codes
                            if found_km_str != "...":
                                found_km_clean = "".join(filter(str.isdigit, found_km_str))
                                
                                if found_km_clean:
                                    if len(found_km_clean) >= 6:
                                        # 6+ digits is the professional/corrected format (e.g., 143.940)
                                        validation = t("km_fixed")
                                    elif len(found_km_clean) < 5:
                                        # Less than 5 is definitely wrong/incomplete
                                        validation = t("km_wrong")
                                    else:
                                        # 5 digits case: check if it matches the sheet prefix
                                        if sys_km_clean and sys_km_clean in found_km_clean:
                                            validation = t("km_wrong") # Matches prefix but still in the "old/wrong" format
                                        else:
                                            validation = t("km_fixed")
                        
                        return (found_km_str, validation)

                    with st.spinner(""):
                        # Pass zipped list to show Ad ID in status
                        combined = list(zip(ids, system_km))
                        asyncio.run(process_list_incremental(combined, cars_checker, callback=update_cars_gs, batch_size=10, existing_results=existing_val))
                    st.success(t("status_done"))
                    st.balloons()
                except Exception as e: st.error(f"Erro: {e}")

    # --- BUTTON: CLEAR OLX ---
    if st.button(t("btn_clear_mod"), key="btn_clear_olx"):
        if not url_gs: st.warning(t("err_no_url"))
        else:
            try:
                gc = get_gspread_client()
                if gc:
                    sh = gc.open_by_url(url_gs)
                    ws = get_worksheet_by_name(sh, "Auto Km")
                    with st.spinner(t("cleaning")):
                        data = ws.get_all_values()
                        def is_olx_cleanup(row):
                            if len(row) < 5: return False
                            status = row[4].strip() # Col E
                            return any(msg in status for msg in [
                                "⚠️ Anúncio já foi moderado ⚠️", 
                                "⚠️ Anúncio inactivo ⚠️",
                                "⚠️ Ad Already Moderated ⚠️",
                                "⚠️ Ad Inactive ⚠️",
                                "✅ KM corrigidos pelo user ✅",
                                "✅ Kilometers corrected by user ✅"
                            ])
                        
                        count = batch_clear_rows(ws, data, is_olx_cleanup)
                        if count > 0: st.success(t("rows_removed", count))
                        else: st.info(t("no_rows"))
            except Exception as e: st.error(f"Erro ao limpar: {e}")

st.divider()
st.caption(t("footer"))
