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
GLOBAL_DEFAULT_URL = "https://docs.google.com/spreadsheets/d/17sq7E56TExN8Icw9Du2oUiuzhLDzfb4VmTgFmJGS1Do" # Corrected URL

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
    with open(LINKS_FILE, "w") as f:
        json.dump(links, f)

SIAC_URL = "https://www.siac.pt/pt"
OLX_BASE_URL = "https://www.olx.pt/"
RNT_AL_DIRECT_URL = "https://rnt.turismodeportugal.pt/RNT/RNAL.aspx?nr="
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
            
            content = await page.content()
            if "não se encontra disponível" in content.lower() or "anúncio removido" in content.lower():
                return "🚫 Inativo/Vendido"
            if attempt < retries: await asyncio.sleep(2); continue
            return "❓ Km não encontrado"
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
            # Only skip if we have a conclusive result (✅, ❌, 🚩). Skip 'N/A' or '...' or errors.
            has_result = isinstance(results[i], str) and any(icon in results[i] for icon in ["✅", "❌", "🚩"])
            if i < len(results) and results[i] != "..." and has_result:
                progress_bar.progress((i + 1) / total)
                continue

            if i > 0 and i % refresh_every == 0:
                status_text.text(f"♻️ Reiniciando navegador...")
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
            
            if not str(val).strip() or str(val).lower() == "nan": res = "N/A"
            else:
                status_text.text(f"🔍 [{i+1}/{total}] A Trabalhar: {status_display}")
                res = await checker_func(page, cleaned, **extra_params)
            
            results[i] = res
            progress_bar.progress((i + 1) / total)
            if callback and (i + 1) % batch_size == 0: await callback(results)
            
        if callback: await callback(results)
        if browser: await browser.close()
    return results

# --- UI LOGIC ---

st.title("🚀 Validação Automática")
st.markdown("Plataforma para validação de dados SIAC, AL e OLX.")

tab_siac, tab_rnt, tab_olx = st.tabs(["🐾 SIAC (Cães/Gatos)", "🏠 RNAL (AL)", "🚗 OLX (Km Carros)"])

# --- TAB: SIAC ---
with tab_siac:
    st.subheader("Validação SIAC")
    st.info("🔦 **DICA:** Esta tab valida microchips na plataforma SIAC. Lê os números das colunas G (Fêmea) e H (Cria) e grava o resultado nas colunas I e J.")
    
    saved_links = load_links()
    # Use GLOBAL_DEFAULT_URL if no saved link exists
    current_default = saved_links.get("siac") or GLOBAL_DEFAULT_URL
    url_siac = st.text_input("URL Google Sheet (SIAC)", value=current_default, key="url_siac")
    if url_siac != saved_links.get("siac", ""):
        save_link("siac", url_siac)

    if st.button("🚀 Iniciar Validação SIAC", key="btn_run_siac"):
        if not url_siac: st.warning("Insira o URL.")
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_siac)
                    ws = get_worksheet_by_name(sh, "AUTO SIAC")
                    if not ws:
                        st.error("ERRO: Aba 'AUTO SIAC' não encontrada no ficheiro!")
                        st.stop()
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
                            ws_i = get_worksheet_by_name(sh_i, "AUTO SIAC")
                            if ws_i:
                                ws_i.update(range_name=f"I2:I{1+len(sf)}", values=sf)
                                ws_i.update(range_name=f"J2:J{1+len(sc)}", values=sc)

                    with st.spinner("A validar o SIAC..."):
                        asyncio.run(process_list_incremental(interleaved, check_siac_on_page, init_url=SIAC_URL, callback=update_siac_gs))
                    st.success("Concluído!")
                except Exception as e: st.error(f"Erro: {e}")

    # --- BUTTON: CLEAR SIAC ---
    if st.button("🧹 Limpar Registados (Ambos ✅)", key="btns_clear_siac"):
        if not url_siac: st.warning("Insira o URL.")
        else:
            try:
                gc = get_gspread_client()
                if gc:
                    sh = gc.open_by_url(url_siac)
                    ws = get_worksheet_by_name(sh, "AUTO SIAC")
                    with st.spinner("Limpando linhas (Sincronizando com a folha)..."):
                        data = ws.get_all_values()
                        def is_siac_done(row):
                            if len(row) < 10: return False
                            # Exact match only, no alerts
                            return row[8].strip() == "✅ REGISTADO" and row[9].strip() == "✅ REGISTADO"
                        
                        count = batch_clear_rows(ws, data, is_siac_done)
                        if count > 0: st.success(f"Removidas {count} linhas!")
                        else: st.info("Nenhuma linha para remover.")
            except Exception as e: st.error(f"Erro ao limpar: {e}")

# --- TAB: RNT ---
with tab_rnt:
    st.subheader("Validação RNAL")
    st.info("🏠 **DICA:** Compara a localização do anúncio OLX com o registo RNAL. Lê o ID OLX na coluna A e o ID RNAL na coluna D. Devolve a localização do OLX na coluna C e o resultado na coluna F.")
    
    saved_links = load_links()
    current_default = saved_links.get("rnt") or GLOBAL_DEFAULT_URL
    url_rnt = st.text_input("URL Google Sheet (RNT)", value=current_default, key="url_rnt")
    if url_rnt != saved_links.get("rnt", ""):
        save_link("rnt", url_rnt)
    
    if st.button("🚀 Iniciar Validação RNAL", key="btn_run_rnt"):
        if not url_rnt: st.warning("Insira o URL.")
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_rnt)
                    ws = get_worksheet_by_name(sh, "AUTO RNAL")
                    if not ws:
                        st.error("ERRO: Aba 'AUTO RNAL' não encontrada no ficheiro!")
                        st.stop()
                    olx_ids = ws.col_values(1)[1:] # A
                    rnal_ids = ws.col_values(4)[1:] # D
                    
                    async def update_al_gs(results):
                        # results is a list of (olx_loc, rnal_loc)
                        gc_u = get_gspread_client()
                        if gc_u:
                            sh_u = gc_u.open_by_url(url_rnt)
                            ws_u = get_worksheet_by_name(sh_u, "AUTO RNAL")
                            if ws_u:
                                olx_formatted = [[r[0]] for r in results]
                                rnal_formatted = [[r[1]] for r in results]
                                val_formatted = []
                                for r in results:
                                    olx_l, rnt_l = str(r[0]).lower(), str(r[1]).lower()
                                    if any(s in str(r[0]) or s in str(r[1]) for s in ["...", "⚠️", "❓"]):
                                        val_formatted.append(["..."])
                                    elif olx_l != "n/a" and rnt_l != "n/a" and any(word in rnt_l for word in olx_l.split() if len(word) > 3): 
                                        val_formatted.append(["✅"])
                                    else: val_formatted.append(["❌"])
                                    
                                ws_u.update(range_name=f"C2:C{1+len(olx_formatted)}", values=olx_formatted) # OLX Loc
                                ws_u.update(range_name=f"E2:E{1+len(rnal_formatted)}", values=rnal_formatted) # RNAL Data
                                ws_u.update(range_name=f"F2:F{1+len(val_formatted)}", values=val_formatted) # Validation
                    
                    async def al_checker(page, ids_tuple):
                        o_id, r_id = ids_tuple
                        olx_loc = await check_olx_location(page, o_id)
                        rnt_data = await check_rnt_rnal_only(page, r_id)
                        return (olx_loc, rnt_data)

                    with st.spinner("A Validar o AL (OLX vs RNAL)..."):
                        combined_ids = list(zip(olx_ids, rnal_ids))
                        asyncio.run(process_list_incremental(combined_ids, al_checker, callback=update_al_gs))
                    st.success("Concluído!")
                    st.balloons()
                except Exception as e: st.error(f"Erro: {e}")

# --- TAB: OLX ---
with tab_olx:
    st.subheader("Validação de Km no OLX")
    st.info("🚗 **DICA:** Valida os Km de carros no OLX. Compara os Km do sistema na coluna C com os Km encontrados no anúncio (ID na coluna A). O resultado vai para a coluna E.")
    
    saved_links = load_links()
    current_default = saved_links.get("olx") or GLOBAL_DEFAULT_URL
    url_olx = st.text_input("URL Google Sheet (OLX)", value=current_default, key="url_olx")
    if url_olx != saved_links.get("olx", ""):
        save_link("olx", url_olx)
    
    if st.button("🚀 Iniciar Validação Km", key="btn_run_olx"):
        if not url_olx: st.warning("Insira o URL.")
        else:
            gc = get_gspread_client()
            if gc:
                try:
                    sh = gc.open_by_url(url_olx)
                    ws = get_worksheet_by_name(sh, "Auto Km")
                    if not ws:
                        st.error("ERRO: Aba 'Auto Km' não encontrada no ficheiro!")
                        st.stop()
                    ids = ws.col_values(1)[1:] # Column A
                    system_km = ws.col_values(3)[1:] # Col C (User provided)
                    
                    async def update_cars_gs(results):
                        # results is a list of (found_km, validation)
                        gc_u = get_gspread_client()
                        if gc_u:
                            sh_u = gc_u.open_by_url(url_olx)
                            ws_u = get_worksheet_by_name(sh_u, "Auto Km")
                            if ws_u:
                                bot_km_fmt = [[r[0]] for r in results]
                                val_fmt = [[r[1]] for r in results]
                                ws_u.update(range_name=f"D2:D{1+len(bot_km_fmt)}", values=bot_km_fmt) # Col D
                                ws_u.update(range_name=f"E2:E{1+len(val_fmt)}", values=val_fmt) # Col E
                    
                    async def cars_checker(page, idx_val):
                        idx, ad_id = idx_val
                        # Get found KM from OLX
                        found_km_str = await check_olx_km(page, ad_id)
                        
                        # Compare with system_km from Col C
                        sys_km = str(system_km[idx]).replace(' ', '').replace('.', '').replace(',', '').strip() if idx < len(system_km) else ""
                        found_km_clean = found_km_str.replace('km', '').replace(' ', '').replace('.', '').replace(',', '').strip().lower()
                        
                        validation = "..."
                        if found_km_str != "...":
                            if sys_km and found_km_clean and sys_km in found_km_clean: validation = "✅Km errados"
                            else: validation = "❌ Km corrigidos"
                        
                        return (found_km_str, validation)

                    with st.spinner("Validando anúncios OLX..."):
                        # We pass index to cars_checker to access system_km
                        asyncio.run(process_list_incremental(list(enumerate(ids)), cars_checker, callback=update_cars_gs, batch_size=10))
                    st.success("Concluído!")
                    st.balloons()
                except Exception as e: st.error(f"Erro: {e}")

st.divider()
st.caption("Validação Automática Multi-Project 2026")
