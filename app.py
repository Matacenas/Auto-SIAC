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
        "dica_rnal": "💡 **Processo de Validação:**\n\n1. Lê o ID do anúncio na Coluna A.\n2. Faz scraping da localização do anúncio em olx.pt e regista o resultado na Coluna C.\n3. Lê o Número de Alojamento Local da Coluna D e valida no site do RNAL:\n   https://rnt.turismodeportugal.pt/RNT/RNAL.aspx?nr=AdID\n4. Faz scraping do resultado da validação e regista a informação na Coluna E.\n5. Compara a localização do OLX com a do RNAL e regista a sugestão na Coluna F.",
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

# --- CUSTOM CSS ---
st.markdown("""
<style>
[data-testid="stSidebar"] { min-width: 280px; max-width: 320px; }
div[data-baseweb="tab-list"] { width: 500px !important; }
div[data-baseweb="tab-border"] { width: 500px !important; }
hr { width: 500px !important; margin-left: 0 !important; }
[data-testid="stNotification"] { width: 500px !important; }
.stAlert { width: 500px !important; }
button[data-baseweb="tab"] p { font-size: 24px !important; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header(t("sidebar_config"))
    c1, c2 = st.columns([1, 4])
    with c1: st.markdown("<img src='https://flagcdn.com/w80/pt.png' style='height: 20px;'>", unsafe_allow_html=True)
    with c2: 
        if st.button("Português", key="pt"): st.session_state.lang = "PT"; st.rerun()
    c3, c4 = st.columns([1, 4])
    with c3: st.markdown("<img src='https://flagcdn.com/w80/gb.png' style='height: 20px;'>", unsafe_allow_html=True)
    with c4:
        if st.button("English", key="en"): st.session_state.lang = "EN"; st.rerun()
    
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
        match = next((t for t in ts if t.strip().lower() == target_name.strip().lower()), None)
        return sh.worksheet(match) if match else None
    except: return None

def normalize_id(val: Any) -> str:
    if val is None: return ""
    s = str(val).strip()
    if not s or s.lower() == "nan": return ""
    if s.endswith(".0") or s.endswith(",0"): s = s[:-2]
    if "E+" in s.upper():
        try: return str(int(float(s.replace(',', '.'))))
        except: pass
    return s.replace('.', '').replace(',', '') if s.replace('.','').isdigit() and len(s) > 10 else s

# --- CHECKERS ---
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
            await page.evaluate(f"""(s, v) => {{
                const el = document.querySelector(s);
                if (el) {{ el.value = v; el.dispatchEvent(new Event('input', {{bubbles:true}})); el.focus(); return true; }}
                return false;
            }}""", sel, chip)
            await page.keyboard.press("Enter")
            log(f"Consultando {chip}...")
            await asyncio.sleep(5)
            
            res = await page.evaluate(f"""() => {{
                const t = document.body.innerText;
                if (t.includes("{SIAC_TEXT_MISSING}")) return "siac_missing";
                if (t.includes("{SIAC_TEXT_REGISTERED}")) return "siac_registered";
                if (t.includes("{SIAC_TEXT_NOT_REGISTERED}")) return "siac_not_registered";
                return "polling";
            }}""")
            if res != "polling": return res
            await asyncio.sleep(2)
        except Exception as e:
            log(f"Erro: {str(e)[:40]}")
            if attempt == 0: await page.reload()
    return "siac_error"

async def check_olx_km(page, ad_id: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[OLX] {m}")
    cid = normalize_id(ad_id)
    url = f"{OLX_BASE_URL}{cid}"
    try:
        log(f"Acedendo {cid}...")
        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
        await asyncio.sleep(4)
        km = await page.evaluate("""() => {
            const m = document.body.innerText.match(/(\\d[\\d\\s\\.,]*)\\s*km/i);
            return m ? m[0].trim() : null;
        }""")
        if km: return km
        content = (await page.content()).lower()
        if "não está disponível" in content: return "ERR_MODERATED"
        return "ERR_NOT_FOUND"
    except: return "⚠️ Erro OLX"

async def check_olx_location(page, ad_id: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[OLX] {m}")
    cid = normalize_id(ad_id)
    url = f"{OLX_BASE_URL}{cid}"
    try:
        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
        await asyncio.sleep(4)
        loc = await page.evaluate("""() => {
            const el = document.querySelector('span[data-testid="location-label"]');
            return el ? el.innerText.trim() : null;
        }""")
        return loc or "❓ Localização"
    except: return "⚠️ Erro OLX"

async def check_rnt_rnal_only(page, reg_id: str, log_func: Callable = None) -> str:
    def log(m):
        if log_func: log_func(f"[RNT] {m}")
    rid = normalize_id(reg_id)
    url = f"{RNT_AL_DIRECT_URL}{rid}"
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(6)
        morada = await page.evaluate("""() => {
            const labels = Array.from(document.querySelectorAll('.TableRecords_Label, td'));
            const l = labels.find(el => el.innerText.includes('Morada'));
            if (l) {
                if (l.tagName === 'TD' && l.nextElementSibling) return l.nextElementSibling.innerText.trim();
                return l.parentElement.innerText.replace('Morada', '').trim();
            }
            return null;
        }""")
        return morada or "❓ Sem Dados"
    except: return "⚠️ Erro RNT"

# --- ENGINE ---
async def process_list_incremental(items, checker_func, ws, col_mappings, init_url=None, refresh_every=50, existing_data=None, **extra_params):
    total = len(items)
    pb = st.progress(0)
    st.subheader("🖥️ Console Debug LIVE")
    console = st.code("Iniciando...", language="text")
    logs = []
    def log(m):
        logs.append(f"> {m}")
        console.code("\n".join(logs[-15:]))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--single-process"])
        context = await browser.new_context(user_agent="Mozilla/5.0 Chrome/120.0.0.0")
        page = await context.new_page()
        
        for i, val in enumerate(items):
            # Skip Logic
            if existing_data and i < len(existing_data):
                raw = existing_data[i]
                if (isinstance(raw, (list, tuple)) and len(raw) > 2 and raw[2] in [t("val_correct"), t("val_wrong")]) or (raw and raw not in ["...", "N/A", ""]):
                    log(f"Linha {i+2}: Ignorada."); pb.progress((i+1)/total); continue

            if i > 0 and i % refresh_every == 0:
                await context.close(); context = await browser.new_context(); page = await context.new_page()
            
            cid = normalize_id(val[0] if isinstance(val, (tuple, list)) else val)
            if not cid: res = "N/A"
            else:
                try:
                    res = await checker_func(page, cid if not isinstance(val, (tuple, list)) else val, log_func=log, **extra_params)
                except Exception as e:
                    log(f"ERRO: {str(e)[:50]}"); res = "Error"
            
            if res != "SKIP":
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
                        vw = t(res) if "siac_" in str(res) else res
                        tc = col_mappings[0] if i % 2 == 0 else col_mappings[1] if len(col_mappings) > 1 else col_mappings[0]
                        ws.update_cell(row, tc, vw)
                except: pass
            pb.progress((i+1)/total)
        await browser.close()

# --- UI ---
st.title(t("title"))
st.markdown(t("subtitle"))
t_siac, t_rnt, t_olx = st.tabs([t("siac_tab"), t("rnal_tab"), t("olx_tab")])

with t_siac:
    st.info(t("dica_siac"))
    if st.button(t("btn_start"), key="run_siac"):
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
                asyncio.run(process_list_incremental(items, check_siac_on_page, ws, [9, 10], existing_data=exists))
                st.success(t("status_done"))

with t_rnt:
    st.info(t("dica_rnal"))
    if st.button(t("btn_start"), key="run_rnt"):
        gc = get_gspread_client()
        if gc:
            sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "AUTO RNAL")
            if ws:
                ads, regs = ws.col_values(1)[1:], ws.col_values(4)[1:]
                locs_o, locs_r, vals = ws.col_values(3)[1:], ws.col_values(5)[1:], ws.col_values(6)[1:]
                items, exists = [], []
                for i in range(len(ads)):
                    items.append((ads[i], regs[i]))
                    exists.append((locs_o[i] if i < len(locs_o) else "", locs_r[i] if i < len(locs_r) else "", vals[i] if i < len(vals) else ""))
                
                async def al_checker(p, val, log_func):
                    ad, reg = val
                    lo = await check_olx_location(p, ad, log_func=log_func)
                    lr = await check_rnt_rnal_only(p, reg, log_func=log_func)
                    return lo, lr

                asyncio.run(process_list_incremental(items, al_checker, ws, [3, 5, 6], existing_data=exists))
                st.success(t("status_done"))

with t_olx:
    st.info(t("dica_olx"))
    if st.button(t("btn_start"), key="run_olx"):
        gc = get_gspread_client()
        if gc:
            sh = gc.open_by_url(url_gs); ws = get_worksheet_by_name(sh, "KM CARROS")
            if ws:
                ads = ws.col_values(1)[1:]
                kms, vals = ws.col_values(4)[1:], ws.col_values(5)[1:]
                items, exists = [], []
                for i in range(len(ads)):
                    items.append(ads[i])
                    exists.append((kms[i] if i < len(kms) else "", "", vals[i] if i < len(vals) else ""))
                asyncio.run(process_list_incremental(items, check_olx_km, ws, [4, 5], existing_data=exists))
                st.success(t("status_done"))
