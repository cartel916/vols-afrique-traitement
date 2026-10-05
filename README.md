# Vols au-dessus de l'Afrique — traitement des archives ADS-B

Ce dépôt exécute, dans GitHub Actions, le tri quotidien des archives ouvertes
[adsb.lol globe_history](https://github.com/adsblol/globe_history_2025) : pour chaque journée, il ne garde que les
vols liés à l'espace aérien africain (pays africains et régions d'information de vol de la région AFI), avec leurs
traversées de zones et leurs trajectoires. L'archive (2 à 3,5 Go par jour) est lue en flux, jamais stockée.

- `traiter_jour.py` : traitement d'une journée (découpage des traces en vols, positions aberrantes, zones traversées).
- `lot.py` : enchaîne plusieurs journées dans un job et range les sorties.
- `plans/` : listes des journées à traiter, par lots.
- `ref/`, `grilles.zip` : référentiel (grilles des pays, des FIR et de leurs secteurs, aéroports, indicatifs).

Travail d'étude sur la conception de blocs fonctionnels d'espace aérien en Afrique ; les résultats sont récupérés
puis analysés hors de ce dépôt.

## Sources et licences des données

- Positions ADS-B : [adsb.lol](https://adsb.lol), © contributeurs adsb.lol, licence ODbL 1.0.
- Aéroports : [OurAirports](https://ourairports.com/data/) (domaine public).
- Frontières : [Natural Earth](https://www.naturalearthdata.com/) (domaine public).
- FIR et secteurs : [VATSpy Data Project](https://github.com/vatsimnetwork/vatspy-data-project), approximation
  communautaire des limites OACI.
- Routes par indicatif : [Virtual Radar Server standing data](https://github.com/vradarserver/standing-data).
