"""Traite une journée d'archive adsb.lol (globe_history) : ne garde que les vols liés au Brésil.

(Noms de colonnes internes conservés à l'identique de la chaîne africaine : touche_afrique, km_afrique,
route_afrique… désignent ici le Brésil ; renommage prévu à la livraison — voir NOTES-BRESIL.md.)

Lecture en flux : l'archive (~4 Go) n'est jamais écrite sur le disque. Le processus principal
lit le tar au fil du téléchargement et distribue les traces aux processus de calcul.

Sorties (Parquet zstd, dans sortie/<date>/) :
  vols.parquet      une ligne par vol (tronçon de trace entre deux escales)
  passages.parquet  une ligne par traversée de zone (pays, FIR, région) avec heures d'entrée/sortie
  points.parquet    les positions observées pleine résolution des vols retenus (positions aberrantes écartées)
  carte.bin         trajectoires allégées pour la page interactive
  brut.tar          traces readsb d'origine des avions retenus (pour retraiter sans retélécharger)
  bilan.json        compteurs ; écrit en dernier : sa présence signale une journée complète

Usage :  traiter_jour.py 2026-09-30 --urls <parts…> --taille <octets attendus>
         traiter_jour.py 2026-09-30 --fichiers brut.tar --source brut     (retraitement, tests)

Version 3 (04/10/2026, après relecture) : flux vérifié (taille, fin d'archive, pas de relance curl),
hauteur au-dessus de l'aéroport, positions aberrantes, moitiés de vol hors Afrique gardées à minuit,
roulages au sol écartés, part observée pondérée par les km, temps depuis minuit UTC, DP point-segment.
Version 4 (seconde relecture) : île Nulle et brouillage GPS (excursions bornées dans le temps, têtes et
queues de trace, coupure à tout saut impossible restant), demi-tours dans un trou détectés par le cap,
objets quasi immobiles écartés, durée exacte, simplification spatio-temporelle (animation fidèle),
référentiel v4 (La Réunion, Mayotte… africaines).
Version 5 : second niveau de zones, les SECTEURS (sous-secteurs VATSpy à l'intérieur des FIR ; depuis le
07/10/2026, secteurs officiels DECEA — 84 dans les 5 FIR brésiliennes — la chaîne ne change pas : la grille
porte un code numérique par cellule, résolu dans `meta.json`).
Version 6 (troisième relecture) : excursions terminées par un vrai saut de retour, vitesse de référence robuste,
escales cachées par temps inexpliqué / atterrissage vu au bord du trou / changement d'indicatif / trou > 16 h,
départ et arrivée observés seulement si le sens vertical est cohérent, aéroport choisi selon le cap,
moitiés de vol gardées jusqu'à 4 h de minuit, écritures atomiques, temps des points à la sous-seconde.
Version 7 : longueur de CROISIÈRE (au-dessus du FL245, espace aérien supérieur) par traversée de zone, pour la
conception des FAB (d_ij du papier SBPO) ; altitude interpolée dans le temps sur les points estimés.
Version 8 (quatrième relecture) : dans un trou de plus de 30 min et 600 km, altitude des points estimés au moins
égale à un profil de montée / descente à 3° depuis les bords du trou, plafonné à l'altitude maximale observée du
vol (l'interpolation linéaire mettait sous le FL245 toute une traversée commencée en montée) ; à l'arrivée,
aéroport choisi sur la pente d'approche (finale vers la piste 05 du Caire attribuée à Almaza, survolé trop haut).
Version 9 (cinquième relecture) : profil à 3° dans TOUS les trous densifiés (dans les trous courts, l'interpolation
seule sous-estimait la croisière de 20 à 140 km par trou) ; « posé au bord du trou » exige un sens vertical cohérent
(une approche vue après un trou n'est pas un décollage : SFR120 descendant vers Johannesburg coupé en deux) ;
altitude maximale observée du vol (alt_max) pour borner la croisière des arcs complétés.
"""
import argparse
import calendar
import csv
import io
import json
import os
import math
import struct
import subprocess
import sys
import tarfile
import time
import zlib
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import orjson

ICI = Path(__file__).resolve().parent
REF = Path(os.environ.get("VOLS_REF", ICI / "ref"))  # VOLS_REF : référentiel de test
SORTIE = ICI / "sortie"

META = json.loads((REF / "meta.json").read_text())
G = META["grille"]
LAT0, LON0, RES, NROWS, NCOLS = G["lat0"], G["lon0"], G["res"], G["nrows"], G["ncols"]
LAT1, LON1 = LAT0 + NROWS * RES, LON0 + NCOLS * RES
PAYS_REGION = np.array([p["region"] for p in META["pays"]], dtype=object)
REGIONS = ["", "South America", "Central America", "Western Europe"]
REGION_IDX = np.array([REGIONS.index(r) if r in REGIONS else 0 for r in PAYS_REGION], dtype=np.uint8)
PAYS_AFRICAIN = np.array([p["bresil"] for p in META["pays"]], dtype=bool)

TRAITEMENT_VERSION = 9
R_TERRE_KM = 6371.0
SAUT_DENSIFIE_S = 120       # au-delà, on comble le trou par un arc de grand cercle (marqué « estimé »)
PAS_DENSIFIE_KM = 25.0
ALT_SOL_FT = 2500           # hauteur AU-DESSUS DE L'AÉROPORT sous laquelle décollage / atterrissage est observé
RAYON_AERO_KM = 12.0
VITESSE_MAX_KMH = 1300.0    # au-delà, un saut de position est une erreur (MLAT, décodage), pas un vol
SAUT_MIN_KM = 3.0           # en dessous, un écart de position est du bruit, quelle que soit la vitesse
EXCURSION_MAX_S = 1800      # durée maximale d'une excursion aberrante (aller puis retour)
TETE_MAX_PTS = 30           # suite aberrante en tête / en queue de trace retirée jusqu'à ce nombre de points
ILE_NULLE_DEG = 0.5         # positions à moins de 0,5° de (0, 0) : valeur par défaut d'un récepteur, retirées
DEPLACEMENT_MIN_KM = 5.0    # tronçon qui ne s'éloigne jamais de plus de 5 km de son départ : pas un vol
DEMI_TOUR_DEG = 120         # cap après un trou opposé à la direction du trou : l'avion est revenu
ESCALE_VITESSE_KT = 90      # un trou dont la vitesse implicite est plus faible cache une escale
ESCALE_TROU_S = 1200        # trous examinés pour une escale cachée
ESCALE_RATIO = 0.55         # vitesse implicite < 55 % de la vitesse observée autour du trou : escale
ESCALE_BAS_FT = 10000       # trou encadré par deux points bas ET vitesse implicite faible : escale
SOL_VITESSE_KT = 40         # tronçon dont la vitesse ne dépasse jamais ce seuil : roulage, pas un vol
TOL_SIMPLIF_DEG = 0.02      # ~2 km : tolérance de l'allègement des trajectoires pour la carte
MINUIT_S = 60               # un avion en vol à moins de 60 s de minuit continue sur l'autre journée
# carte.bin : positions quantifiées sur le monde entier (int16 : 0,0055° en longitude, 0,0026° en latitude).
QLON0, QLAT0, QLON1, QLAT1 = -180.0, -85.0, 180.0, 85.0

# Adresses OACI par défaut de transpondeurs mal configurés : plusieurs avions les partagent.
ADRESSES_BIDON = {"000000", "000001", "123456", "ffffff", "abcdef", "111111", "222222", "aaaaaa",
                  "012345", "999999", "249249", "fffffe", "800000", "c00000", "abc123"}
TROU_MAX_S = 16 * 3600       # aucun vol ne traverse un trou de couverture de plus de 16 h
TEMPS_INEXPLIQUE_S = 5400    # trou plus long que le vol direct (+15 %) de plus d'1 h 30 : escale cachée
TENDANCE_FTMIN = 300         # sens vertical : premier point qui descend / dernier point qui monte -> pas un départ / une arrivée
MINUIT_GARDE_S = 4 * 3600    # moitié de vol hors Afrique gardée si elle commence / finit en vol à moins de 4 h de minuit
CAP_AERO_DEG = 60            # aéroport devant l'avion à l'arrivée, derrière lui au départ
SEUIL_CROISIERE_FT = 24500   # espace aérien supérieur (FL245) : un segment au-dessus compte en « croisière »
PENTE_FT_KM = 172.0          # pente de 3° (montée, descente, approche) : 172 ft par km
PENTE_MARGE_FT = 500         # à l'arrivée, hauteur admise au-dessus de la pente de 3° vers l'aéroport
RAYON_ARRIVEE_KM = 20.0      # rayon de recherche d'un autre aéroport d'arrivée, sur la pente celui-là
TROU_PROFIL_S = 120          # trous où l'altitude estimée suit un profil de montée / descente (tous les trous
TROU_PROFIL_KM = 0           # densifiés : v8 se limitait à 30 min et 600 km)

_G_PAYS = _G_FIR = _G_AFR = _G_SEC = None
_AERO = None
_ROUTES_AFR = frozenset()


def init_worker():
    global _G_PAYS, _G_FIR, _G_AFR, _G_SEC, _AERO, _ROUTES_AFR
    f = REF / "indicatifs_bresil.txt"
    _ROUTES_AFR = frozenset(f.read_text().split()) if f.exists() else frozenset()
    _G_PAYS = np.load(REF / "grille_pays.npy", mmap_mode="r")
    _G_FIR = np.load(REF / "grille_fir.npy", mmap_mode="r")
    _G_AFR = np.load(REF / "grille_br.npy", mmap_mode="r")
    _G_SEC = np.load(REF / "grille_secteur.npy", mmap_mode="r")
    codes, lats, lons, rangs, isos, elevs = [], [], [], [], [], []
    for r in csv.DictReader(open(REF / "aeroports.csv", encoding="utf-8")):
        codes.append(r["code"]); lats.append(float(r["lat"])); lons.append(float(r["lon"]))
        rangs.append(int(r["rang"])); isos.append(r["iso"]); elevs.append(float(r.get("elev") or 0))
    lats = np.array(lats); lons = np.array(lons)
    seaux = {}
    for i, (la, lo) in enumerate(zip(lats, lons)):
        seaux.setdefault((int(math.floor(la)), int(math.floor(lo))), []).append(i)
    _AERO = {"code": codes, "lat": lats, "lon": lons, "rang": np.array(rangs), "iso": isos, "elev": np.array(elevs),
             "seaux": {k: np.array(v) for k, v in seaux.items()}}


def lookup(grille, lat, lon):
    r = ((lat - LAT0) / RES).astype(np.int64)
    c = ((lon - LON0) / RES).astype(np.int64)
    ok = (r >= 0) & (r < NROWS) & (c >= 0) & (c < NCOLS)
    out = np.zeros(lat.shape, dtype=np.uint8)
    out[ok] = grille[r[ok], c[ok]]
    return out


def dist_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dl = np.radians(lon2 - lon1)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R_TERRE_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def aeroport_proche(lat, lon, cap=np.nan, sens=None, alt=np.nan):
    """Meilleur aéroport à moins de RAYON_AERO_KM : (code, iso, distance km, altitude ft) ou None.
    Avec un cap : à l'arrivée (sens « arr ») l'aéroport devant l'avion, au départ (« dep ») derrière lui, à
    ±60° (sinon un terrain voisin plus proche de l'axe d'approche l'emporte : Almaza au lieu du Caire).
    À l'arrivée avec une altitude : si l'avion est à plus de 500 ft au-dessus de la pente de 3° de l'aéroport
    retenu, un autre aéroport à moins de 20 km, sous cette limite et dans l'axe, le remplace (sinon on garde le
    premier). Almaza est survolé à 2 300 ft à 9 km par les finales vers la piste 05 du Caire, à 13 km : trop
    haut pour Almaza, sur la pente du Caire."""
    cand = []
    for dla in (-1, 0, 1):
        for dlo in (-1, 0, 1):
            s = _AERO["seaux"].get((int(math.floor(lat)) + dla, int(math.floor(lon)) + dlo))
            if s is not None:
                cand.append(s)
    if not cand:
        return None
    idx = np.concatenate(cand)
    d = dist_km(lat, lon, _AERO["lat"][idx], _AERO["lon"][idx])
    axe = None
    if sens and not np.isnan(cap):
        p1 = np.radians(lat); p2 = np.radians(_AERO["lat"][idx]); dl = np.radians(_AERO["lon"][idx] - lon)
        brg = np.degrees(np.arctan2(np.sin(dl) * np.cos(p2), np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)))
        vise = cap if sens == "arr" else (cap + 180) % 360
        axe = (np.abs((brg - vise + 180) % 360 - 180) <= CAP_AERO_DEG) | (d < 3)
    ok = d <= RAYON_AERO_KM
    if not ok.any():
        return None
    if axe is not None and (ok & axe).any():
        ok = ok & axe
    score = np.where(ok, d + _AERO["rang"][idx] * 5.0, np.inf)   # à distance voisine, préférer le grand aéroport
    k = int(np.argmin(score))
    if sens == "arr" and alt is not None and np.isfinite(alt):
        sous_pente = alt - _AERO["elev"][idx] <= PENTE_FT_KM * d + PENTE_MARGE_FT
        if not sous_pente[k]:
            autre = (d <= RAYON_ARRIVEE_KM) & sous_pente
            if axe is not None:
                autre &= axe
            if autre.any():
                k = int(np.argmin(np.where(autre, d + _AERO["rang"][idx] * 5.0, np.inf)))
    i = int(idx[k])
    return _AERO["code"][i], _AERO["iso"][i], float(d[k]), float(_AERO["elev"][i])


def grand_cercle(la1, lo1, la2, lo2, n):
    """n points intermédiaires (extrémités exclues) sur l'arc de grand cercle."""
    p1, l1, p2, l2 = map(math.radians, (la1, lo1, la2, lo2))
    v1 = np.array([math.cos(p1) * math.cos(l1), math.cos(p1) * math.sin(l1), math.sin(p1)])
    v2 = np.array([math.cos(p2) * math.cos(l2), math.cos(p2) * math.sin(l2), math.sin(p2)])
    om = math.acos(max(-1.0, min(1.0, float(v1 @ v2))))
    if om < 1e-9:
        return np.full(n, la1), np.full(n, lo1)
    f = np.arange(1, n + 1) / (n + 1)
    a = np.sin((1 - f) * om) / math.sin(om)
    b = np.sin(f * om) / math.sin(om)
    v = a[:, None] * v1 + b[:, None] * v2
    return np.degrees(np.arcsin(np.clip(v[:, 2], -1, 1))), np.degrees(np.arctan2(v[:, 1], v[:, 0]))


def rdp(x, y, t, tol):
    """Douglas-Peucker spatio-temporel : écart mesuré au point INTERPOLÉ DANS LE TEMPS entre les deux bouts
    (un avion qui attend, ralentit ou fait demi-tour garde ses points : l'animation reste fidèle)."""
    n = len(x)
    garde = np.zeros(n, dtype=bool)
    if n <= 2:
        garde[:] = True
        return garde
    garde[0] = garde[-1] = True
    pile = [(0, n - 1)]
    while pile:
        a, b = pile.pop()
        if b - a < 2:
            continue
        xs, ys = x[a + 1:b], y[a + 1:b]
        u = (t[a + 1:b] - t[a]) / max(t[b] - t[a], 1e-9)
        d = np.hypot(xs - (x[a] + u * (x[b] - x[a])), ys - (y[a] + u * (y[b] - y[a])))
        k = int(np.argmax(d))
        if d[k] > tol:
            m = a + 1 + k
            garde[m] = True
            pile.append((a, m)); pile.append((m, b))
    return garde


def vitesse_kmh(t, lat, lon, i, j):
    d = float(dist_km(lat[i], lon[i], lat[j], lon[j]))
    return d / max(1.0, t[j] - t[i]) * 3600, d


def ecarter_aberrants(t, lat, lon):
    """Masque des positions à garder.

    1. Positions à (0, 0) : valeur par défaut d'un récepteur mal réglé (« île Nulle »), retirées d'office.
    2. EXCURSION : un saut impossible (> 1 300 km/h), puis, dans les 30 min, un point qui redevient cohérent
       avec celui d'avant le saut (erreur de multilatération, brouillage GPS) : l'excursion est retirée.
    3. Tête ou queue de trace incohérente avec le reste (jusqu'à 30 points) : retirée.
    Les sauts impossibles qui subsistent coupent le vol (voir troncons) : aucun arc n'est tracé à travers."""
    n = len(t)
    garde = ~((np.abs(lat) < ILE_NULLE_DEG) & (np.abs(lon) < ILE_NULLE_DEG))
    k = np.flatnonzero(garde)
    if len(k) < 3:
        return garde
    tk, la, lo = t[k], lat[k], lon[k]
    m = len(k)
    g2 = np.ones(m, dtype=bool)
    d = dist_km(la[:-1], lo[:-1], la[1:], lo[1:])
    v = d / np.maximum(1.0, np.diff(tk)) * 3600
    saut = np.concatenate(([False], (v > VITESSE_MAX_KMH) & (d > SAUT_MIN_KM)))   # saut[i] : entre i-1 et i
    i = 1
    while i < m:
        if not saut[i]:
            i += 1
            continue
        fin = None
        for j in range(i + 1, m):
            if tk[j] - tk[i - 1] > EXCURSION_MAX_S:
                break
            if not saut[j]:
                continue   # l'excursion ne finit que par un saut de RETOUR (un décalage persistant n'est pas « fini »)
            vv, dd = vitesse_kmh(tk, la, lo, i - 1, j)
            if vv <= VITESSE_MAX_KMH or dd <= SAUT_MIN_KM:
                fin = j
                break
        if fin is not None:
            g2[i:fin] = False
            i = fin + 1
            continue
        if i <= TETE_MAX_PTS and i + 1 < m:
            vv, dd = vitesse_kmh(tk, la, lo, i, i + 1)
            if vv <= VITESSE_MAX_KMH or dd <= SAUT_MIN_KM:   # la tête [0, i) est l'erreur
                g2[:i] = False
        elif m - i <= TETE_MAX_PTS:
            g2[i:] = False                                    # la queue [i, m) est l'erreur
            break
        i += 1
    garde[k[~g2]] = False
    return garde


def troncons(t, lat, lon, alt, sol, flags, gs, trk, cs_pt=None):
    """Coupe la trace du jour en vols : drapeau readsb « nouveau tronçon » + escales cachées dans un trou.

    Un avion en croisière dans un trou de couverture garde sa vitesse : la vitesse implicite
    (distance / durée du trou) reste proche de la vitesse observée. Une escale la fait chuter,
    un demi-tour aussi (aller-retour Pologne-Égypte vu seulement au-dessus de la mer Égée).
    """
    n = len(t)
    coupures, teleport = [0], set()

    def vz_bord(k, sens):
        """Vitesse verticale (ft/min) sur la minute qui suit k (« dep ») ou qui le précède (« arr ») ; 0 si inconnue."""
        j = k
        if sens == "dep":
            while j + 1 < n and t[j + 1] - t[k] <= 60:
                j += 1
            a_, b_ = k, j
        else:
            while j - 1 >= 0 and t[k] - t[j - 1] <= 60:
                j -= 1
            a_, b_ = j, k
        if b_ <= a_ or np.isnan(alt[a_]) or np.isnan(alt[b_]) or t[b_] - t[a_] < 10:
            return 0.0
        return (alt[b_] - alt[a_]) / (t[b_] - t[a_]) * 60

    def cap_valide(i, sens):
        """Dernier cap renseigné avant i (sens -1) ou premier à partir de i (sens +1), à 30 points près."""
        r = range(i, max(-1, i - 30), -1) if sens < 0 else range(i, min(n, i + 30))
        for k in r:
            if not np.isnan(trk[k]):
                return trk[k]
        return np.nan

    for i in range(1, n):
        if flags[i] & 2:
            coupures.append(i)
            continue
        dt = t[i] - t[i - 1]
        d = float(dist_km(lat[i - 1], lon[i - 1], lat[i], lon[i]))
        if d > SAUT_MIN_KM and d / max(1.0, dt) * 3600 > VITESSE_MAX_KMH:
            coupures.append(i); teleport.add(i)   # saut impossible resté après filtrage : pas d'arc à travers
            continue
        if dt > TROU_MAX_S:
            coupures.append(i)
            continue
        if dt > ESCALE_TROU_S:
            v_kt = d / (dt / 3600) / 1.852
            fen = gs[max(0, i - 30):min(n, i + 30)]
            fen = fen[np.isfinite(fen) & (fen <= 650)]           # une vitesse sol aberrante ne fausse pas la référence
            vref = max(float(np.percentile(fen, 90)) if len(fen) else 400.0, 100.0)
            bas_av = sol[i - 1] or (not np.isnan(alt[i - 1]) and alt[i - 1] < ESCALE_BAS_FT)
            bas_ap = sol[i] or (not np.isnan(alt[i]) and alt[i] < ESCALE_BAS_FT)
            c_av, c_ap = cap_valide(i - 1, -1), cap_valide(i, +1)
            ecart = abs(((c_ap - c_av) + 180) % 360 - 180) if not (np.isnan(c_av) or np.isnan(c_ap)) else 0.0
            # Direction du trou (du dernier point avant au premier point après) contre le cap après le trou.
            p1, p2, dl = map(math.radians, (lat[i - 1], lat[i], lon[i] - lon[i - 1]))
            dir_trou = math.degrees(math.atan2(math.sin(dl) * math.cos(p2),
                                               math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)))
            retour = (not np.isnan(c_ap) and d > 300 and dt > 1800 and v_kt < 0.8 * vref
                      and abs(((c_ap - dir_trou) + 180) % 360 - 180) > DEMI_TOUR_DEG)
            # Temps inexpliqué : le trou dure bien plus que le vol direct à la vitesse de croisière observée.
            v_att = min(max(vref * 1.852, 300.0), 900.0)
            inexplique = dt - 1.15 * d / v_att * 3600 > TEMPS_INEXPLIQUE_S
            # Atterrissage vu juste avant le trou, ou décollage vu juste après.
            pose = False
            for k_, sens in ((i - 1, "arr"), (i, "dep")):
                if sol[k_] or (not np.isnan(alt[k_]) and alt[k_] < ESCALE_BAS_FT and (np.isnan(gs[k_]) or gs[k_] < 220)):
                    ap = aeroport_proche(float(lat[k_]), float(lon[k_]))
                    # Sens vertical : un avion qui descend après le trou est en approche (pas un décollage), un
                    # avion qui monte avant le trou vient de décoller (pas un atterrissage).
                    vz_ = 0.0 if sol[k_] else vz_bord(k_, sens)
                    sens_ok = not ((sens == "dep" and vz_ < -TENDANCE_FTMIN) or (sens == "arr" and vz_ > TENDANCE_FTMIN))
                    if ap and sens_ok and (sol[k_] or alt[k_] - ap[3] < ALT_SOL_FT):
                        pose = True
            # Changement d'indicatif à travers le trou : un autre vol.
            autre_vol = False
            if cs_pt is not None:
                av = next((cs_pt[k_] for k_ in range(i - 1, max(-1, i - 400), -1) if cs_pt[k_]), None)
                ap_ = next((cs_pt[k_] for k_ in range(i, min(n, i + 400)) if cs_pt[k_]), None)
                autre_vol = bool(av and ap_ and av != ap_)
            if (v_kt < ESCALE_VITESSE_KT or (bas_av and bas_ap and v_kt < 0.8 * vref)
                    or (dt > 2700 and v_kt < ESCALE_RATIO * vref)
                    or (dt > 1800 and ecart > 135 and v_kt < 0.8 * vref)
                    or retour or inexplique or pose or autre_vol):
                coupures.append(i)
    coupures.append(n)
    out = []
    for a, b in zip(coupures[:-1], coupures[1:]):
        if b - a < 2:
            continue
        # Fragment isolé par un saut impossible : erreur de position (brouillage GPS), pas un vol — s'il est
        # court, ou s'il est encadré de sauts impossibles des deux côtés pendant moins de 2 h.
        if (a in teleport or b in teleport) and b - a < 30:
            continue
        if a in teleport and b in teleport and t[b - 1] - t[a] < 7200:
            continue
        out.append((a, b))
    return out


def segments_zone(code):
    """Suites de valeurs identiques -> [(valeur, i_debut, i_fin)]."""
    if len(code) == 0:
        return []
    ch = np.flatnonzero(np.diff(code.astype(np.int16)) != 0) + 1
    debuts = np.concatenate(([0], ch))
    fins = np.concatenate((ch, [len(code)]))
    return [(int(code[a]), int(a), int(b)) for a, b in zip(debuts, fins)]


def traiter_trace(blob):
    try:
        d = orjson.loads(zlib.decompress(blob, 47) if blob[:2] == b"\x1f\x8b" else blob)
    except Exception:
        return None
    tr = d.get("trace")
    if not tr or len(tr) < 2 or d.get("icao", "").lower() in ADRESSES_BIDON:
        return None
    # Pré-filtre rapide sur un point sur huit : l'avion est-il passé dans le cadre de la grille ?
    if not any(LAT0 < p[1] < LAT1 and LON0 < p[2] < LON1 for p in tr[::8]) and not \
            (LAT0 < tr[-1][1] < LAT1 and LON0 < tr[-1][2] < LON1):
        return None
    base = d["timestamp"]
    n = len(tr)
    t = np.empty(n); lat = np.empty(n); lon = np.empty(n); alt = np.empty(n); gs = np.empty(n)
    trk = np.empty(n); flags = np.zeros(n, dtype=np.int64); sol = np.zeros(n, dtype=bool)
    cs_pt = [None] * n
    for i, p in enumerate(tr):
        t[i] = p[0]; lat[i] = p[1]; lon[i] = p[2]
        a = p[3]
        if a == "ground":
            sol[i] = True; alt[i] = 0
        else:
            alt[i] = a if a is not None else np.nan
        gs[i] = p[4] if p[4] is not None else np.nan
        trk[i] = p[5] if p[5] is not None else np.nan
        flags[i] = p[6] or 0
        if p[8] and p[8].get("flight"):
            cs = p[8]["flight"].strip()
            if cs:
                cs_pt[i] = cs
    # Positions aberrantes : retirées avant tout calcul (le drapeau « nouveau tronçon » est reporté).
    g = ecarter_aberrants(t, lat, lon)
    n_aberrants = int((~g).sum())
    if n_aberrants:
        nl = np.flatnonzero(flags & 2)
        for k in nl:
            if not g[k]:
                suiv = np.flatnonzero(g[k:])
                if len(suiv):
                    flags[k + suiv[0]] |= 2
        t, lat, lon, alt, gs, trk, flags, sol = (x[g] for x in (t, lat, lon, alt, gs, trk, flags, sol))
        cs_pt = [c for c, k in zip(cs_pt, g) if k]
        if len(t) < 2:
            return None
    vols, passages, points, cartes = [], [], [], []
    touche = False
    for (a, b) in troncons(t, lat, lon, alt, sol, flags, gs, trk, cs_pt):
        tt, la, lo, al, sl = t[a:b], lat[a:b], lon[a:b], alt[a:b], sol[a:b]
        gmax = np.nanmax(gs[a:b]) if np.isfinite(gs[a:b]).any() else 0.0
        if sl.all() or gmax < SOL_VITESSE_KT:
            continue   # roulage, avion stationné, émetteur de tour : pas un vol
        if float(np.max(dist_km(la[0], lo[0], la, lo))) < DEPLACEMENT_MIN_KM:
            continue   # objet quasi immobile (véhicule, tour, avion au parking) : pas un vol
        veille = bool(tt[0] < MINUIT_S and not sl[0])
        lendemain = bool(tt[-1] > 86400 - MINUIT_S and not sl[-1])
        garde_minuit = bool((tt[0] < MINUIT_GARDE_S and not sl[0]) or (tt[-1] > 86400 - MINUIT_GARDE_S and not sl[-1]))
        # Densification des trous par grand cercle (pour savoir quelles zones ont été survolées).
        dla, dlo, dtt, dob, dsol = [la[:1]], [lo[:1]], [tt[:1]], [np.ones(1, dtype=bool)], [sl[:1]]
        trous = 0.0
        n_d = 1                # nombre de points densifiés déjà posés
        trous_longs = []       # (indice du bord avant, points estimés, km) des trous à profil vertical
        for i in range(1, len(tt)):
            dt = tt[i] - tt[i - 1]
            if dt > SAUT_DENSIFIE_S:
                km = float(dist_km(la[i - 1], lo[i - 1], la[i], lo[i]))
                k = int(km // PAS_DENSIFIE_KM)
                if k > 0:
                    gla, glo = grand_cercle(la[i - 1], lo[i - 1], la[i], lo[i], k)
                    dla.append(gla); dlo.append(glo)
                    dtt.append(tt[i - 1] + dt * np.arange(1, k + 1) / (k + 1))
                    dob.append(np.zeros(k, dtype=bool)); dsol.append(np.zeros(k, dtype=bool))
                    if dt > TROU_PROFIL_S and km > TROU_PROFIL_KM:
                        trous_longs.append((n_d - 1, k, km))
                    n_d += k
                trous += dt
            dla.append(la[i:i + 1]); dlo.append(lo[i:i + 1]); dtt.append(tt[i:i + 1])
            dob.append(np.ones(1, dtype=bool)); dsol.append(sl[i:i + 1])
            n_d += 1
        DLa, DLo, DT, DOb, DSol = (np.concatenate(x) for x in (dla, dlo, dtt, dob, dsol))
        # Altitude le long de la trace densifiée : observée sur les vrais points, interpolée dans le temps ailleurs.
        alt_obs = np.where(sl, 0.0, al)
        ok_alt = np.isfinite(alt_obs)
        DAlt = np.interp(DT, tt[ok_alt], alt_obs[ok_alt]) if ok_alt.any() else np.full(len(DT), np.nan)
        # Long trou : l'avion ne reste pas à l'altitude de la montée ou de l'approche où on l'a perdu. Profil de
        # montée / descente à 3° depuis chaque bord, plafonné à l'altitude maximale observée du vol.
        if trous_longs and ok_alt.any():
            alt_max = float(np.max(alt_obs[ok_alt]))
            for j0, k, km in trous_longs:
                s = km * np.arange(1, k + 1) / (k + 1)
                prof = np.minimum(alt_max, np.minimum(DAlt[j0] + PENTE_FT_KM * s, DAlt[j0 + k + 1] + PENTE_FT_KM * (km - s)))
                DAlt[j0 + 1:j0 + k + 1] = np.maximum(DAlt[j0 + 1:j0 + k + 1], prof)
        dans = lookup(_G_AFR, DLa, DLo).astype(bool)
        ind = [(i, cs_pt[a + i]) for i in range(b - a) if cs_pt[a + i]]
        callsign = ind[0][1] if ind else ""
        # Vol jamais capté au-dessus de l'Afrique mais dont la route annoncée y mène : gardé, la consolidation
        # complète la trajectoire jusqu'à l'aéroport africain (estimée).
        route_afr = bool(callsign) and callsign in _ROUTES_AFR
        if not dans.any() and not (garde_minuit or route_afr):
            continue
        autres_cs = sorted({cs for _, cs in ind} - {callsign})
        t0, t1 = base + tt[0], base + tt[-1]
        vid = f"{d['icao']}-{int(t0)}"

        def tendance(i):
            """Vitesse verticale (ft/min) sur la minute qui suit le premier point (i = 0) ou précède le dernier."""
            if i == 0:
                j = int(np.searchsorted(tt, tt[0] + 60)); j = min(max(j, 1), len(tt) - 1); k, l = 0, j
            else:
                j = int(np.searchsorted(tt, tt[-1] - 60)) - 1; j = max(0, min(j, len(tt) - 2)); k, l = j, len(tt) - 1
            if l <= k or np.isnan(al[k]) or np.isnan(al[l]) or tt[l] - tt[k] < 10:
                return 0.0
            return (al[l] - al[k]) / (tt[l] - tt[k]) * 60

        def extremite(i, sens):
            c = trk[a + i]
            ap = aeroport_proche(float(la[i]), float(lo[i]), c, None if sl[i] else sens, float(al[i]))
            if sl[i]:
                bas = True
            else:
                hauteur = al[i] - (ap[3] if ap else 0.0)
                bas = not np.isnan(al[i]) and hauteur < ALT_SOL_FT
            if not bas:
                return None, None, "inconnu"
            # Sens vertical : un « départ » qui descend est une fin d'approche, une « arrivée » qui monte un décollage.
            v = tendance(i)
            if not sl[i] and ((sens == "dep" and v < -TENDANCE_FTMIN) or (sens == "arr" and v > TENDANCE_FTMIN)):
                return None, None, "inconnu"
            if ap is None:
                return None, None, "bas-sans-aeroport"
            return ap[0], ap[1], "observe"

        dep, dep_iso, dep_src = extremite(0, "dep")
        arr, arr_iso, arr_src = extremite(len(tt) - 1, "arr")
        duree = tt[-1] - tt[0]
        pas = np.diff(DT, append=DT[-1])
        km_seg = np.concatenate((dist_km(DLa[:-1], DLo[:-1], DLa[1:], DLo[1:]), [0.0]))
        # Segment observé = entre deux positions réelles rapprochées dans le temps.
        seg_obs = np.append(DOb[:-1] & DOb[1:] & (np.diff(DT) <= SAUT_DENSIFIE_S), False)
        seg_crois = np.append((DAlt[:-1] + DAlt[1:]) / 2 >= SEUIL_CROISIERE_FT, False)
        en_vol = ~(DSol & np.append(DSol[1:], True))
        vols.append({
            "vol_id": vid, "icao": d["icao"], "immat": d.get("r") or "", "type": d.get("t") or "",
            "desc": d.get("desc") or "", "militaire": bool((d.get("dbFlags") or 0) & 1),
            "indicatif": callsign, "autres_indicatifs": ",".join(autres_cs),
            "debut": int(t0), "fin": int(t1), "duree_s": int(t1) - int(t0),
            "lat_debut": float(la[0]), "lon_debut": float(lo[0]), "alt_debut": float(al[0]) if not np.isnan(al[0]) else None,
            "lat_fin": float(la[-1]), "lon_fin": float(lo[-1]), "alt_fin": float(al[-1]) if not np.isnan(al[-1]) else None,
            "vz_debut": float(tendance(0)), "vz_fin": float(tendance(len(tt) - 1)),
            "alt_max": float(np.nanmax(al[~sl])) if np.isfinite(al[~sl]).any() else None,
            "cap_debut": float(trk[a]) if not np.isnan(trk[a]) else None,
            "cap_fin": float(trk[b - 1]) if not np.isnan(trk[b - 1]) else None,
            "dep_aeroport": dep, "dep_pays": dep_iso, "dep_source": dep_src,
            "arr_aeroport": arr, "arr_pays": arr_iso, "arr_source": arr_src,
            "suite_veille": veille, "suite_lendemain": lendemain, "touche_afrique": bool(dans.any()),
            "route_afrique": route_afr,
            "n_points": int(len(tt)), "part_observee": float(1 - trous / duree) if duree > 0 else 1.0,
            "trou_max_s": float(np.max(np.diff(tt))) if len(tt) > 1 else 0.0,
            "km_afrique": float(km_seg[dans].sum()), "s_afrique": float(pas[dans & en_vol].sum()),
            "km_afrique_observe": float(km_seg[dans & seg_obs].sum()),
            "km_afrique_croisiere": float(km_seg[dans & seg_crois].sum()),
            "km_terre_afrique": float(km_seg[PAYS_AFRICAIN[lookup(_G_PAYS, DLa, DLo)]].sum()) if dans.any() else 0.0,
            "n_aberrants": n_aberrants,
        })
        if not dans.any() and not route_afr:
            continue   # moitié de vol hors Afrique gardée pour le recollage de minuit : pas de trajectoire
        touche = True
        cpays = lookup(_G_PAYS, DLa, DLo)
        cfir = lookup(_G_FIR, DLa, DLo)
        creg = REGION_IDX[cpays]
        for typ, code in (("pays", cpays), ("fir", cfir), ("region", creg), ("secteur", lookup(_G_SEC, DLa, DLo))):
            for val, i0, i1 in segments_zone(code):
                if val == 0:
                    continue
                k_tot = float(km_seg[i0:i1].sum())
                obs = float(km_seg[i0:i1][seg_obs[i0:i1]].sum() / k_tot) if k_tot > 0 else float(DOb[i0:i1].mean())
                passages.append({"vol_id": vid, "type_zone": typ, "zone": int(val),
                                 "entree": int(base + DT[i0]), "sortie": int(base + DT[min(i1, len(DT) - 1)]),
                                 "km": k_tot, "part_observee": obs, "km_croisiere": float(km_seg[i0:i1][seg_crois[i0:i1]].sum())})
        points.append((vid, base + tt, la, lo, al, gs[a:b], trk[a:b]))
        # Trajectoire allégée pour la carte : points observés simplifiés + arcs estimés tous les ~100 km.
        gk = rdp(DLo, DLa, DT, TOL_SIMPLIF_DEG)
        gk |= (~DOb) & (np.arange(len(DOb)) % 4 == 0)
        cartes.append((vid, DT[gk] + base, DLa[gk], DLo[gk], DOb[gk]))
    if not vols:
        return None
    # Les traces brutes ne sont gardées que pour les avions passés par l'Afrique ce jour-là.
    return vols, passages, points, cartes, d["icao"], blob if touche else None


class Compteur:
    """Enveloppe de lecture qui compte les octets consommés par tarfile."""

    def __init__(self, f):
        self.f, self.n = f, 0

    def read(self, k=-1):
        b = self.f.read(k)
        self.n += len(b)
        return b


def lire_flux(fichiers, urls):
    if fichiers:
        cmd = ["cat", *fichiers]
    else:
        # Pas de --retry : une relance repartirait de l'octet 0 et dupliquerait le début dans le flux.
        # flux.py relance la journée entière en cas d'échec.
        cmd = ["sh", "-c", " && ".join(f"curl -sfL --speed-limit 100000 --speed-time 120 '{u}'" for u in urls)]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1 << 20)


def _entete_valide(h):
    """En-tête tar (512 octets) valide : somme de contrôle juste et marque « ustar »."""
    if len(h) < 512 or not any(h) or h[257:262] != b"ustar":
        return False
    try:
        return int(h[148:156].rstrip(b"\0 "), 8) == sum(h[:148]) + 32 * 8 + sum(h[156:])
    except ValueError:
        return False


def membres(proc, compteur):
    """Lecteur tar en flux, tolérant. Sur un en-tête invalide (archive publiée dont les parts ne se suivent
    pas, comme le 04/05/2026), cherche le prochain en-tête valide et reprend : les octets sautés sont comptés
    (octets_ignores) et la journée n'est acceptée que s'ils restent sous 2 % (sinon erreur_tar)."""
    f = proc.stdout
    lus = ignores = resync = doubles = 0
    nom_long = None
    vus = set()
    while True:
        h = f.read(512)
        lus += len(h)
        if len(h) < 512:
            break
        if not any(h):
            continue                      # blocs nuls (fin d'archive, bourrage) : on continue jusqu'à la fin du flux
        if not _entete_valide(h):
            # Resynchronisation : blocs de 512 octets jusqu'au prochain en-tête valide.
            resync += 1
            saute = 512
            while True:
                h = f.read(512)
                lus += len(h)
                if len(h) < 512:
                    break
                if _entete_valide(h):
                    break
                saute += 512
            ignores += saute
            if len(h) < 512:
                break
        try:
            taille = int(h[124:136].rstrip(b"\0 ") or b"0", 8)
        except ValueError:
            continue
        typ = h[156:157]
        nom = nom_long or h[:100].rstrip(b"\0").decode("utf-8", "replace")
        nom_long = None
        donnees = f.read(taille)
        lus += len(donnees)
        reste = (-taille) % 512
        if reste:
            lus += len(f.read(reste))
        if len(donnees) < taille:
            compteur["erreur_tar"] = "fin de flux au milieu d'un fichier"
            break
        if typ == b"L":                   # nom long GNU : s'applique à l'en-tête suivant
            nom_long = donnees.rstrip(b"\0").decode("utf-8", "replace")
            continue
        if typ in (b"0", b"\0") and "trace_full_" in nom:
            if nom in vus:                 # même fichier deux fois : parts issues de deux générations de l'archive
                doubles += 1               # (le flux n'a plus de relance interne : le doublon vient de la source)
                continue
            vus.add(nom)
            compteur["traces"] += 1
            compteur["octets"] += len(donnees)
            yield donnees
    compteur["octets_flux"] = lus
    compteur["octets_ignores"] = ignores
    compteur["resynchronisations"] = resync
    compteur["membres_en_double"] = doubles
    if doubles or resync:
        compteur["archive_incoherente"] = True   # signalée comme journée partielle dans le rapport
    if ignores > 0.02 * max(1, lus):
        compteur["erreur_tar"] = f"archive incohérente : {ignores} octets illisibles sur {lus}"


def ecrire(date, resultats, compteur, dossier):
    import pyarrow as pa
    import pyarrow.parquet as pq
    dossier.mkdir(parents=True, exist_ok=True)
    (dossier / "bilan.json").unlink(missing_ok=True)   # écrit en dernier : marque une journée complète
    vols = [v for r in resultats for v in r[0]]
    passages = [p for r in resultats for p in r[1]]
    def ecrit_parquet(table, nom, **kw):
        tmp = dossier / (nom + ".tmp")
        pq.write_table(table, tmp, **kw)
        tmp.replace(dossier / nom)
    ecrit_parquet(pa.Table.from_pylist(vols), "vols.parquet", compression="zstd")
    schema_p = pa.schema([("vol_id", pa.string()), ("type_zone", pa.string()), ("zone", pa.int64()),
                          ("entree", pa.int64()), ("sortie", pa.int64()), ("km", pa.float64()), ("part_observee", pa.float64()),
                          ("km_croisiere", pa.float64())])
    ecrit_parquet(pa.Table.from_pylist(passages, schema=schema_p), "passages.parquet", compression="zstd")
    with tarfile.open(dossier / "brut.tar.tmp", "w") as tb:
        for r in resultats:
            if r[5] is None:
                continue
            ti = tarfile.TarInfo(f"traces/trace_full_{r[4]}.json")
            ti.size = len(r[5]); ti.mtime = 0
            tb.addfile(ti, io.BytesIO(r[5]))
    (dossier / "brut.tar.tmp").replace(dossier / "brut.tar")
    cols = {"vol_id": [], "t": [], "lat": [], "lon": [], "alt": [], "vitesse": [], "cap": []}
    for r in resultats:
        for vid, t, la, lo, al, v, c in r[2]:
            cols["vol_id"].append(np.full(len(t), vid, dtype=object)); cols["t"].append(t.astype(np.float64))
            cols["lat"].append(la.astype(np.float32)); cols["lon"].append(lo.astype(np.float32))
            cols["alt"].append(al.astype(np.float32)); cols["vitesse"].append(v.astype(np.float32))
            cols["cap"].append(c.astype(np.float32))
    if cols["t"]:
        tab = pa.table({k: pa.array(np.concatenate(v)) for k, v in cols.items()})
        ecrit_parquet(tab, "points.parquet", compression="zstd", compression_level=9)
    # carte.bin : en-tête JSON + blocs binaires compacts. Par point : lon, lat en int16 (cadre mondial),
    # temps uint16 = ((secondes depuis minuit UTC) // 4) << 1 | estimé. Version 3.
    jour0 = calendar.timegm(time.strptime(date, "%Y-%m-%d"))
    index, lons, lats, tps = [], [], [], []
    cur = 0
    for r in resultats:
        for (vid, t, la, lo, ob) in r[3]:
            k = len(t)
            qx = np.round((lo - QLON0) / (QLON1 - QLON0) * 65535 - 32768).clip(-32768, 32767).astype(np.int16)
            qy = np.round((la - QLAT0) / (QLAT1 - QLAT0) * 65535 - 32768).clip(-32768, 32767).astype(np.int16)
            q = (np.clip(t - jour0, 0, 86399) // 4).astype(np.uint32)
            qt = (q << 1) | (~ob).astype(np.uint32)
            index.append([vid, cur, k]); cur += k
            lons.append(qx); lats.append(qy); tps.append(qt.astype(np.uint16))
    meta = {"date": date, "jour0": jour0, "cadre": [QLON0, QLAT0, QLON1, QLAT1], "version": TRAITEMENT_VERSION,
            "pas_temps_s": 4, "vols": index}
    entete = json.dumps(meta, separators=(",", ":")).encode()
    with open(dossier / "carte.bin.tmp", "wb") as fo:
        fo.write(struct.pack("<I", len(entete))); fo.write(entete)
        fo.write(b"\0" * ((4 - (4 + len(entete)) % 4) % 4))
        for arr in (lons, lats, tps):
            if arr:
                fo.write(np.concatenate(arr).tobytes())
    (dossier / "carte.bin.tmp").replace(dossier / "carte.bin")
    # Activité horaire des vols retenus (repère des journées partielles à la source : pannes de quelques heures).
    hist = np.zeros(24, dtype=int)
    for v_ in vols:
        if v_["touche_afrique"]:
            h0, h1 = int((v_["debut"] - jour0) // 3600), int((v_["fin"] - jour0) // 3600)
            hist[max(0, h0):min(23, h1) + 1] += 1
    compteur["activite_horaire"] = hist.tolist()
    compteur["vols"] = sum(1 for v in vols if v["touche_afrique"])
    compteur["moities_hors_afrique"] = sum(1 for v in vols if not v["touche_afrique"])
    compteur["passages"] = len(passages)
    compteur["aberrants"] = sum(r[0][0]["n_aberrants"] for r in resultats)
    compteur["version"] = TRAITEMENT_VERSION
    compteur["carte_version"] = TRAITEMENT_VERSION
    tmp = dossier / "bilan.json.tmp"
    tmp.write_text(json.dumps(compteur, indent=1))
    tmp.replace(dossier / "bilan.json")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("date")
    ap.add_argument("--fichiers", nargs="*")
    ap.add_argument("--urls", nargs="*")
    ap.add_argument("--taille", type=int, help="octets attendus (somme des parts)")
    ap.add_argument("--tag", default="")
    ap.add_argument("--source", default="archive", help="archive (téléchargement complet) ou brut (retraitement)")
    ap.add_argument("--processus", type=int, default=3)
    ap.add_argument("--sortie", default=str(SORTIE))
    a = ap.parse_args()
    t_debut = time.time()
    compteur = {"date": a.date, "traces": 0, "octets": 0, "tag": a.tag, "source": a.source, "taille_attendue": a.taille}
    proc = lire_flux(a.fichiers, a.urls)
    resultats = []
    with Pool(a.processus, initializer=init_worker) as pool:
        for r in pool.imap_unordered(traiter_trace, membres(proc, compteur), chunksize=32):
            if r is not None:
                resultats.append(r)
    code = proc.wait()
    compteur["code_telechargement"] = code
    compteur["duree_s"] = round(time.time() - t_debut, 1)
    if a.taille and abs(compteur.get("octets_flux", 0) - a.taille) > 2 * 1048576:
        compteur["erreur_tar"] = f"taille lue {compteur.get('octets_flux')} ≠ attendue {a.taille}"
    if code != 0 or "erreur_tar" in compteur:
        print(json.dumps(compteur), file=sys.stderr)
        sys.exit(2)
    ecrire(a.date, resultats, compteur, Path(a.sortie) / a.date)
    print(json.dumps(compteur))


if __name__ == "__main__":
    main()
