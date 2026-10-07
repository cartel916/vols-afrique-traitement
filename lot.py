"""Traite un lot de journées d'archives adsb.lol (globe_history), sur le Mac comme dans un job GitHub Actions.

Usage : python lot.py <identifiant du lot> 2025-03-01 2025-03-02 …

Pour chaque journée : choix de la variante d'archive (prod, staging, puis les variantes « -0tmp »), taille exacte
des parts par requête HEAD, traiter_jour.py en flux (l'archive n'est jamais écrite sur le disque), puis rangement
des sorties, à plat, dans le dossier de la journée :
  <VOLS_SORTIE>/<date>/   vols.parquet, passages.parquet, carte.bin, bilan.json, points.parquet, brut.tar
traiter_jour.py écrit lui-même dans <VOLS_SORTIE>/<date>/ et pose bilan.json en dernier : sa présence signale une
journée complète, et une journée déjà faite est sautée (reprise après interruption). Une journée en échec n'arrête
pas le lot : <VOLS_SORTIE>/etat-<lot>.json la signale et le job finit en erreur.

VOLS_SORTIE (variable d'environnement) choisit le disque de sortie ; défaut inchangé : out/ à côté de ce script.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ICI = Path(__file__).resolve().parent
REF = ICI / "ref"
OUT = Path(os.environ.get("VOLS_SORTIE", str(ICI / "out")))   # VOLS_SORTIE : disque de sortie (défaut : out/)
PREFERENCE = ("prod", "staging", "prodtmp", "stagingtmp")
DELAI_JOUR_S = 5400


def releases():
    """date -> {"prod": (dépôt, tag, MiB), "staging": …, "prodtmp": …, "stagingtmp": …}, d'après RELEASES.md."""
    out = {}
    motif = re.compile(r"\[planes-readsb-(prod|staging)-(\d)(tmp)? \((\d+) MiB\)\]\(https://github.com/adsblol/(globe_history_\d{4})/releases/tag/([^#)]+)")
    for f in sorted(REF.glob("RELEASES_*.md")):
        for ligne in f.read_text().splitlines():
            m = re.match(r"- (\d{4}-\d{2}-\d{2}) ", ligne)
            if not m:
                continue
            choix = {}
            for kind, n, tmp, mib, depot, tag in motif.findall(ligne):
                choix.setdefault(kind + ("tmp" if tmp else ""), (depot, tag, int(mib)))
            if choix:
                out[m.group(1)] = choix
    return out


def candidats(variantes):
    """Variantes par ordre d'essai : d'abord celles dont la taille atteint la plus grosse à 5 % près."""
    maxi = max(v[2] for v in variantes.values())
    pleines = [k for k in PREFERENCE if k in variantes and 1.05 * variantes[k][2] >= maxi]
    return pleines + [k for k in PREFERENCE if k in variantes and k not in pleines]


def tete(u):
    """(statut final, taille) d'une requête HEAD ; réessaie les erreurs passagères (tout sauf 200 et 404)."""
    for essai in range(5):
        r = subprocess.run(["curl", "-sIL", "--max-time", "60", u], capture_output=True, text=True)
        statuts = re.findall(r"^HTTP/\S+ (\d{3})", r.stdout, re.M)
        tailles = re.findall(r"^content-length:\s*(\d+)", r.stdout, re.M | re.I)
        if statuts and statuts[-1] in ("200", "404"):
            return statuts[-1], int(tailles[-1]) if tailles and statuts[-1] == "200" else 0
        time.sleep(10 * (essai + 1))
    raise RuntimeError(f"pas de réponse stable pour {u}")


def parts_exactes(depot, tag, mib=None):
    """[(url, octets)] des parts .tar.aa, .tar.ab, … (ou « .tar » unique sous 2 Go) ; total contrôlé contre RELEASES.md."""
    base = f"https://github.com/adsblol/{depot}/releases/download/{tag}/{tag}"
    parts = []
    for i in range(26):
        u = f"{base}.tar.a{chr(ord('a') + i)}"
        st, n = tete(u)
        if st == "404":
            break
        parts.append((u, n))
    if not parts:
        st, n = tete(f"{base}.tar")
        if st == "200":
            parts.append((f"{base}.tar", n))
    if not parts:
        raise RuntimeError(f"aucune part pour {tag}")
    total = sum(n for _, n in parts)
    if mib is not None and not (mib * 1048576 - 2 * 1048576 <= total <= (mib + 2) * 1048576):
        raise RuntimeError(f"{tag} : {len(parts)} part(s), {total} octets, incohérent avec {mib} MiB annoncés")
    return parts


def deja_faite(date):
    """Bilan d'une journée déjà traitée (« vols » > 0), sinon None. Sert à la reprise : une journée faite est sautée."""
    try:
        b = json.loads((OUT / date / "bilan.json").read_text())
    except Exception:
        return None
    return b if (b.get("vols") or 0) > 0 else None


def ecrire_etat(chemin, etat):
    """Écrit l'état du lot d'un coup (temporaire puis renommage) : jamais de JSON tronqué en cours d'écriture."""
    tmp = Path(str(chemin) + ".tmp")
    tmp.write_text(json.dumps(etat, ensure_ascii=False, indent=1))
    tmp.replace(chemin)


def traiter(date, rel, processus):
    b = deja_faite(date)
    if b:
        return {"date": date, "ok": True, "deja": True, "vols": b.get("vols"), "traces": b.get("traces"),
                "passages": b.get("passages"), "duree_s": b.get("duree_s")}
    if date not in rel:
        return {"date": date, "ok": False, "erreur": "absente des archives adsb.lol"}
    erreur = ""
    for variante in candidats(rel[date])[:2]:
        depot, tag, mib = rel[date][variante]
        try:
            parts = parts_exactes(depot, tag, mib)
        except RuntimeError as e:
            erreur = str(e)
            continue
        taille = sum(n for _, n in parts)
        for essai in range(2):
            t0 = time.time()
            shutil.rmtree(OUT / date, ignore_errors=True)   # journée partielle éventuelle : on repart de zéro
            cmd = [sys.executable, str(ICI / "traiter_jour.py"), date, "--urls", *[u for u, _ in parts],
                   "--taille", str(taille), "--tag", tag, "--source", "archive", "--sortie", str(OUT),
                   "--processus", str(processus)]
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=DELAI_JOUR_S)
                code, sortie, err = r.returncode, r.stdout, r.stderr
            except subprocess.TimeoutExpired:
                code, sortie, err = -1, "", f"délai de {DELAI_JOUR_S} s dépassé"
            if code == 0:
                b = json.loads(sortie.strip().splitlines()[-1])
                return {"date": date, "ok": True, "variante": variante, "tag": tag, "octets": b.get("octets_flux"),
                        "vols": b.get("vols"), "passages": b.get("passages"), "traces": b.get("traces"),
                        "version": b.get("version"), "duree_s": round(time.time() - t0)}
            erreur = f"{variante}, essai {essai + 1}, code {code} : {err.strip()[-300:]}"
            print(f"{date} : {erreur}", flush=True)
            time.sleep(20)
    return {"date": date, "ok": False, "erreur": erreur}


def main():
    lot, dates = sys.argv[1], sys.argv[2:]
    rel = releases()
    processus = max(2, os.cpu_count() or 2)
    OUT.mkdir(parents=True, exist_ok=True)
    chemin_etat = OUT / f"etat-{lot}.json"
    etat = []
    for date in dates:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            etat.append({"date": date, "ok": False, "erreur": "date invalide"})
            continue
        r = traiter(date, rel, processus)
        etat.append(r)
        print(json.dumps(r, ensure_ascii=False), flush=True)
        ecrire_etat(chemin_etat, etat)
    resume = os.environ.get("GITHUB_STEP_SUMMARY")
    if resume:
        with open(resume, "a") as f:
            f.write(f"### Lot {lot}\n\n| Journée | Résultat | Vols | Go lus | Durée |\n|---|---|---|---|---|\n")
            for r in etat:
                f.write(f"| {r['date']} | {'ok (' + r['variante'] + ')' if r['ok'] else 'ÉCHEC : ' + r['erreur'][:120]} | "
                        f"{r.get('vols', '')} | {round((r.get('octets') or 0) / 1e9, 2) or ''} | {r.get('duree_s', '')} s |\n")
    sys.exit(0 if all(r["ok"] for r in etat) else 1)


if __name__ == "__main__":
    main()
