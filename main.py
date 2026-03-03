import pandas as pd
import asyncio
import re
from datetime import datetime
from playwright.async_api import async_playwright

# --- CONFIGURACIÓN ---
EXCEL_ENTRADA = "ejemplo_formato.xlsx"
EXCEL_SALIDA = "resultado_final.xlsx"

async def contar_actividad_videos(page, url_canal):
    try:
        await page.goto(f"{url_canal}/videos", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        v_largos = 0
        ya_encontro_antiguo = False
        
        while not ya_encontro_antiguo:
            locadores = page.locator("#metadata-line span.inline-metadata-item")
            items = await locadores.all_inner_texts()
            
            if not items:
                await page.mouse.wheel(0, 500)
                await page.wait_for_timeout(1000)
                items = await locadores.all_inner_texts()

            conteo_actual = 0
            hay_mas_recientes_al_final = False

            # Filtrar solo indicadores de tiempo
            tiempos = [t for t in items if "hace" in t.lower() or "ago" in t.lower()]
            
            if not tiempos:
                break

            for i, m in enumerate(tiempos):
                m_low = m.lower()
                es_reciente = any(x in m_low for x in ["segundo", "minuto", "hora", "día", "dia", "semana"])
                
                if "semana" in m_low:
                    num = re.findall(r'\d+', m_low)
                    if num and int(num[0]) > 4: 
                        es_reciente = False

                if es_reciente:
                    conteo_actual += 1
                    if i == len(tiempos) - 1:
                        hay_mas_recientes_al_final = True
                else:
                    ya_encontro_antiguo = True
                    break
            
            v_largos = conteo_actual
            
            if not ya_encontro_antiguo and hay_mas_recientes_al_final:
                prev_count = len(items)
                await page.mouse.wheel(0, 3000)
                await page.wait_for_timeout(2000)
                nuevo_count = await page.locator("#metadata-line span.inline-metadata-item").count()
                if nuevo_count <= prev_count:
                    break
            else:
                break

    except Exception as e:
        print(f"      [!] Error en el canal: {e}")
        v_largos = 0

    return v_largos

async def ejecutar_monitoreo_excel():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        
        try:
            df = pd.read_excel(EXCEL_ENTRADA)
            total_registros = len(df) # Obtener el total para el contador
        except Exception as e:
            print(f"❌ Error al leer el Excel: {e}")
            return

        # Preparar columnas si no existen
        if 'videos_30d' not in df.columns:
            df['videos_30d'] = 0
        if 'actividad_nivel' not in df.columns:
            df['actividad_nivel'] = ""

        print(f"\n{'='*60}")
        print(f"🚀 INICIANDO EXTRACCIÓN A EXCEL (SIN EMOJIS)")
        print(f"{'='*60}\n")

        for i, row in df.iterrows():
            url = str(row['url'])
            # Sello de tiempo para el registro actual
            hora_inicio = datetime.now().strftime("%H:%M:%S")
            
            # Salto por observación de inexistencia
            if "No encontrada" in str(row.get('observaciones', '')):
                print(f"[{hora_inicio}] [{i+1}/{total_registros}] ⏭️ Saltando canal inexistente")
                continue

            print(f"[{hora_inicio}] [{i+1}/{total_registros}] Analizando: {row.get('name', 'Canal')}")
            
            page = await context.new_page()
            v_l = await contar_actividad_videos(page, url)
            
            # Clasificación limpia
            if v_l >= 7: nivel = "ALTA"
            elif 3 <= v_l <= 6: nivel = "MEDIA"
            elif 1 <= v_l <= 2: nivel = "BAJA"
            else: nivel = "NULA"
            
            # Guardar en DataFrame
            df.at[i, 'videos_30d'] = v_l
            df.at[i, 'actividad_nivel'] = nivel
            
            print(f"      ✅ Resultado: {nivel} ({v_l} videos)")
            
            await page.close()

            # Guardado parcial cada 3 registros por seguridad
            if (i + 1) % 3 == 0:
                df.to_excel(EXCEL_SALIDA, index=False)

        await browser.close()
        # Guardado final
        df.to_excel(EXCEL_SALIDA, index=False)
        print(f"\n✨ PROCESO COMPLETADO. Archivo guardado como: {EXCEL_SALIDA}")

if __name__ == "__main__":
    asyncio.run(ejecutar_monitoreo_excel())