# Vols au-dessus de l'Afrique et du Brésil — traitement des archives ADS-B

Ce dépôt exécute, dans GitHub Actions, le tri quotidien des archives ouvertes
[adsb.lol globe_history](https://github.com/adsblol/globe_history_2025) : pour chaque journée, il ne garde que les
vols liés à l'espace aérien étudié (pays et régions d'information de vol), avec leurs traversées de zones et leurs
trajectoires. L'archive (2 à 4 Go par jour) est lue en flux, jamais stockée.
Il sert désormais aussi au Brésil : la branche `bresil` porte le même outillage adapté aux cinq FIR brésiliennes
et son plan `plans/bresil-2025.json` (367 journées, marges de fin et de début d'année comprises), la branche `main`
restant la chaîne africaine.

- `traiter_jour.py` : traitement d'une journée (découpage des traces en vols, positions aberrantes, zones traversées).
- `lot.py` : enchaîne plusieurs journées dans un job et range les sorties.
- `plans/` : listes des journées à traiter, par lots.
- `ref/` : référentiel embarqué (grilles des pays, des FIR, du Brésil et de leurs secteurs, aéroports, contours,
  indicatifs).

Variables d'environnement : `VOLS_SORTIE` choisit le disque de sortie (défaut `out/` à côté de `lot.py`) ;
`VOLS_REF` choisit le référentiel (défaut `ref/`). Les sorties sont rangées à plat, une journée par dossier :
`<VOLS_SORTIE>/<date>/` avec `vols.parquet`, `passages.parquet`, `carte.bin`, `bilan.json`, `points.parquet` et
`brut.tar` (traces d'origine, retirées avant publication de l'artefact). `bilan.json` est écrit en dernier : sa
présence signale une journée complète.

Travail d'étude sur la conception de blocs fonctionnels d'espace aérien ; les résultats sont récupérés puis
analysés hors de ce dépôt.

## Sources et licences des données

- Positions ADS-B : [adsb.lol](https://adsb.lol), © contributeurs adsb.lol, licence ODbL 1.0.
- Aéroports : [OurAirports](https://ourairports.com/data/) (domaine public).
- Frontières : [Natural Earth](https://www.naturalearthdata.com/) (domaine public).
- FIR et secteurs : [DECEA](https://geoaisweb.decea.mil.br/) (Departamento de Controle do Espaço Aéreo, Brésil),
  couches WFS publiques `ICA:fir` et `ICA:SETOR_FIR` — limites officielles, qui remplacent le découpage
  communautaire VATSpy depuis le 07/10/2026.
- Routes par indicatif : [Virtual Radar Server standing data](https://github.com/vradarserver/standing-data).
