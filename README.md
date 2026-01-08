# Pipeline PySpark ETL — IMDb

Pipeline complet Raw → Staging → DW Core → BI Marts pour les datasets officiels IMDb.

## Données
- Source officielle : https://datasets.imdbws.com/
- Fichiers utilisés : `title.basics.tsv.gz`, `title.ratings.tsv.gz` (déposés dans `raw/`).

## Prérequis
- Python 3.10+
- PySpark (`pip install pyspark`)
- `requests` uniquement si vous laissez le script télécharger les données (`--download`).

## Exécution rapide
```bash
python src/etl_imdb.py --raw-dir raw --dw-dir dw --marts-dir marts --download
```
Options :
- `--download` : télécharge les deux fichiers dans `raw/` si absents.
- `--overwrite-download` : force le re-téléchargement.
- `--show-counts` : affiche le nombre de lignes après chaque étape (coût en temps de calcul).

Le script crée les dossiers `dw/` et `marts/` si besoin, écrase en mode overwrite et partitionne `fact_ratings` par `yearkey`.

## Détails du pipeline
1. Extraction : lecture des TSV.GZ (séparateur tabulation, header activé).
2. Staging : remplacement des `\N`, cast des colonnes numériques, filtrage sur `titleType = movie`, déduplication `tconst`.
3. Dimensions :
   - `dim_year` (clé year).
   - `dim_title` (clé naturelle `tconst`).
   - `dim_genre` et `bridge_title_genre` (split/explode des genres, trim/lower).
4. Fait : `fact_ratings` (yearkey, avg_rating, num_votes, runtime_min).
5. Marts BI :
   - `mart_year_kpi` (n_movies, mean_rating, total_votes par année).
   - `mart_top_genre_year` (top 10 par genre/année selon num_votes, avec rang `rk`).
6. Export : Parquet overwrite; partition sur `yearkey` pour le fait.

## Schéma des dossiers de sortie
- `dw/dim_year`, `dw/dim_title`, `dw/dim_genre`, `dw/bridge_title_genre`, `dw/fact_ratings`
- `marts/mart_year_kpi`, `marts/mart_top_genre_year`

## Vérifications de qualité recommandées
- Lancer avec `--show-counts` pour contrôler les volumes après nettoyage.
- Vérifier l’absence de `\N` résiduels : `df.where(col == "\\N").count() == 0`.
- S’assurer que `num_votes` et `avg_rating` ne sont pas nuls sur le fait ; appliquer un seuil de votes si nécessaire.

## Visualisation
- Importer `marts/mart_year_kpi` ou `mart_top_genre_year` dans Power BI, Superset ou matplotlib pour analyses.

## Démo Colab (rapide)
1. Ouvrir un nouveau notebook Colab (runtime Python par défaut, Java et Spark déjà présents).
2. Installer les dépendances : `!pip install pyspark requests`.
3. Télécharger les fichiers :
   ```bash
   !wget -O raw/title.basics.tsv.gz https://datasets.imdbws.com/title.basics.tsv.gz
   !wget -O raw/title.ratings.tsv.gz https://datasets.imdbws.com/title.ratings.tsv.gz
   ```
4. Copier le contenu de `src/etl_imdb.py` dans une cellule (ou l’importer depuis ce dépôt) sous `/content/src/etl_imdb.py`.
5. Exécuter :
   ```bash
   !python /content/src/etl_imdb.py --raw-dir /content/raw --dw-dir /content/dw --marts-dir /content/marts --show-counts
   ```
6. Explorer les Parquet générés depuis `/content/dw` et `/content/marts`.

## Vérification étape par étape (après dézip/Colab)
- **1. Dézipper** le projet ou cloner, puis monter dans Colab : `!unzip projet.zip -d /content/projet` (ou `git clone`).
- **2. Installer dépendances** dans le notebook : `!pip install pyspark requests`.
- **3. Télécharger les données** dans `/content/projet/raw` avec les commandes `wget` ci-dessus.
- **4. Lancer le pipeline** :
   ```bash
   !python /content/projet/src/etl_imdb.py --raw-dir /content/projet/raw --dw-dir /content/projet/dw --marts-dir /content/projet/marts --show-counts
   ```
- **5. Contrôler les sorties** :
   ```python
   import glob, pandas as pd
   print(glob.glob('/content/projet/dw/*'))
   print(glob.glob('/content/projet/marts/*'))
   pd.read_parquet('/content/projet/marts/mart_year_kpi').head()
   ```
- **6. Points d’attention** :
   - Les comptes affichés par `--show-counts` permettent de valider le nettoyage.
   - Les Parquet doivent exister dans `dw/` et `marts/`; `fact_ratings` est partitionné par `yearkey`.
   - Si une erreur JVM survient en local, privilégier Colab ou un JDK 11+.
