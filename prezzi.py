import pandas as pd
import requests
from io import StringIO
from datetime import datetime

# ────────────────────────────────────────────────
#  CONFIG
# ────────────────────────────────────────────────
ANAGRAFICA_URL = "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"
PREZZI_URL     = "https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv"
OUTPUT_HTML    = "prezzi_benzina_pavia.html"

PROVINCIA_DESIDERATA = "PV"

TIPI_CARBURANTE = ["Benzina", "Gasolio", "GPL"]
TIPI_CARBURANTE_LOWER = [x.lower() for x in TIPI_CARBURANTE]

print("Inizio ─", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# ────────────────────────────────────────────────
#  1. Leggo ANAGRAFICA → solo PV
# ────────────────────────────────────────────────
try:
    r_ana = requests.get(ANAGRAFICA_URL, timeout=25)
    r_ana.raise_for_status()
    df_ana = pd.read_csv(
        StringIO(r_ana.text),
        sep="|",
        header=None,
        on_bad_lines="skip",
        dtype=str,
        names=[
            "idImpianto", "Gestore", "Bandiera", "TipoImpianto", "NomeImpianto",
            "Indirizzo", "Comune", "Provincia", "Lat", "Lon"
        ]
    )

    # Pulizia
    df_ana = df_ana[df_ana["Provincia"].str.upper() == PROVINCIA_DESIDERATA].copy()
    df_ana["idImpianto"] = df_ana["idImpianto"].str.strip()
    df_ana = df_ana[df_ana["idImpianto"].str.isnumeric()]
    df_ana["Comune"] = df_ana["Comune"].str.title().str.strip()

    print(f"Impianti trovati in provincia {PROVINCIA_DESIDERATA}: {len(df_ana)}")

    if df_ana.empty:
        raise ValueError("Nessun impianto trovato in provincia PV")

except Exception as e:
    print("Errore scaricamento/filtro anagrafica:", str(e))
    exit(1)

# ────────────────────────────────────────────────
#  2. Leggo PREZZI
# ────────────────────────────────────────────────
try:
    r_prez = requests.get(PREZZI_URL, timeout=25)
    r_prez.raise_for_status()
    df_prez = pd.read_csv(
        StringIO(r_prez.text),
        sep="|",
        header=None,
        on_bad_lines="skip",
        names=["idImpianto", "Carburante", "Prezzo", "isSelf", "Data"],
        dtype={"idImpianto": str, "Prezzo": str},
    )

    # Pulizia
    df_prez["idImpianto"] = df_prez["idImpianto"].str.strip()
    df_prez["Carburante"] = df_prez["Carburante"].str.strip()
    df_prez["Prezzo_num"] = pd.to_numeric(df_prez["Prezzo"], errors="coerce")
    df_prez = df_prez.dropna(subset=["Prezzo_num"])

    # Solo i carburanti che ci interessano
    mask_carburante = df_prez["Carburante"].str.lower().isin(TIPI_CARBURANTE_LOWER)
    df_prez = df_prez[mask_carburante].copy()

    # Solo impianti che esistono in PV
    impianti_pv = set(df_ana["idImpianto"])
    df_prez = df_prez[df_prez["idImpianto"].isin(impianti_pv)].copy()

    print(f"Prezzi filtrati (PV + Benzina/Gasolio/GPL): {len(df_prez)} righe")

    if df_prez.empty:
        raise ValueError("Nessun prezzo trovato per gli impianti PV")

except Exception as e:
    print("Errore scaricamento/filtro prezzi:", str(e))
    exit(1)

# ────────────────────────────────────────────────
#  3. Join
# ────────────────────────────────────────────────
df = df_prez.merge(
    df_ana[["idImpianto", "Bandiera", "Gestore", "NomeImpianto", "Indirizzo", "Comune", "Lat", "Lon"]],
    on="idImpianto",
    how="left"
)

df["NomeVisualizzato"] = df.apply(
    lambda r: f"{r['Bandiera']} – {r['Gestore'] or r['NomeImpianto'] or '—'}", axis=1
)

df["Servizio"] = df["isSelf"].map({1: "Self", 0: "Servito", "1": "Self", "0": "Servito"}).fillna("—")

# Per ordinamento e visualizzazione
df = df.sort_values(["Carburante", "Prezzo_num", "Comune", "NomeVisualizzato"])

# ────────────────────────────────────────────────
#  4. HTML con filtri <select>  (senza titolo e descrizioni lunghe)
# ────────────────────────────────────────────────
now = datetime.now()
ultimo_agg = df["Data"].max() if "Data" in df.columns else "—"

html = f"""<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Prezzi Carburanti Pavia - {now.strftime("%d/%m/%Y")}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 1.5rem; line-height: 1.6; background:#fafafa; }}
    .controls {{ margin: 1.5rem 0; padding: 1rem; background:#fff; border:1px solid #ddd; border-radius:8px; }}
    select {{ padding: 0.5rem 0.8rem; font-size:1.05rem; margin-right:1.5rem; min-width:180px; }}
    table {{ border-collapse: collapse; width:100%; max-width:1100px; background:white; box-shadow:0 2px 8px rgba(0,0,0,0.1); }}
    th, td {{ border:1px solid #e5e7eb; padding:0.75rem 1rem; text-align:left; }}
    th {{ background:#dc2626; color:white; }}
    tr:nth-child(even) {{ background:#f9fafb; }}
    tr:hover {{ background:#fef2f2; }}
    .prezzo {{ font-weight:bold; font-size:1.1em; }}
    .self {{ color:#15803d; font-weight:bold; }}
    .servito {{ color:#b91c1c; font-weight:bold; }}
    .small {{ font-size:0.85rem; color:#4b5563; }}
    a.coords {{ color:#1d4ed8; text-decoration:none; }}
    a.coords:hover {{ text-decoration:underline; }}
  </style>
</head>
<body>

<div class="controls">
  <label>Tipo carburante: </label>
  <select id="carburante" onchange="filtra()">
    <option value="">Tutti</option>
    {"".join(f'<option value="{c}">{c}</option>' for c in TIPI_CARBURANTE)}
  </select>

  <label>Comune: </label>
  <select id="comune" onchange="filtra()">
    <option value="">Tutti</option>
    {"".join(f'<option value="{c}">{c}</option>' for c in sorted(df["Comune"].dropna().unique()))}
  </select>
</div>

<table id="tabella">
  <thead>
    <tr>
      <th>Prezzo €/L</th>
      <th>Carburante</th>
      <th>Stazione</th>
      <th>Indirizzo</th>
      <th>Comune</th>
      <th>Servizio</th>
      <th>Coordinate</th>
    </tr>
  </thead>
  <tbody id="tbody">
"""

# Popoliamo tutte le righe
for _, r in df.iterrows():
    prezzo = f"{r['Prezzo_num']:.3f}".replace(".", ",")
    coords_link = f"https://www.google.com/maps?q={r['Lat']},{r['Lon']}" if pd.notna(r["Lat"]) and pd.notna(r["Lon"]) else "—"
    html += f"""
    <tr data-carburante="{r['Carburante']}" data-comune="{r['Comune']}">
      <td><span class="prezzo">{prezzo}</span></td>
      <td>{r['Carburante']}</td>
      <td>{r['NomeVisualizzato']}</td>
      <td>{r['Indirizzo'] or '—'}</td>
      <td>{r['Comune'] or '—'}</td>
      <td class="{'self' if r['Servizio']=='Self' else 'servito'}">{r['Servizio']}</td>
      <td><a class="coords" href="{coords_link}" target="_blank">{r['Lat'] or '—'}, {r['Lon'] or '—'}</a></td>
    </tr>
"""

html += """
  </tbody>
</table>

<p class="small" style="margin-top:3rem;">
  Dati MIMIT – Aggiornati alle 8:00 • Ultimo rilevamento: """ + str(ultimo_agg) + """<br>
  Generato il """ + now.strftime("%d/%m/%Y %H:%M") + """ • 
  <a href="https://www.mimit.gov.it/it/open-data/elenco-dataset/carburanti-prezzi-praticati-e-anagrafica-degli-impianti">Fonte MIMIT Open Data</a>
</p>

<script>
function filtra() {
  const carb = document.getElementById("carburante").value.toLowerCase();
  const com  = document.getElementById("comune").value;
  const rows = document.querySelectorAll("#tbody tr");

  rows.forEach(row => {
    const matchCarb = !carb || row.dataset.carburante.toLowerCase() === carb;
    const matchCom  = !com  || row.dataset.comune === com;
    row.style.display = (matchCarb && matchCom) ? "" : "none";
  });
}
</script>

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\nHTML generato → {OUTPUT_HTML}")
print(f"Righe totali mostrate: {len(df)}")
