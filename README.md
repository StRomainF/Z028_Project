# Z028_Project : Documentation Technique

## Vue d'ensemble métier
- **Objectif** : Trouver la pire/meilleur route de process du debut jusqu'au PT intermediare
- **Techno** : [Z028]
- **Mesures de succès** : Validation metier + reproductibilitée 

## Architecture technique
- **Technologies** : Spotfire, python, Databricks, pySpark, SQL
- **Ressources cluster** : [Voir Section Driver](#DriverNode)

# État de l'Art : Optimisation des Routes de Process

## 1. Problématique et Contexte 

### 1.1 Définition du problème
L'optimisation des routes de processus (de semi-conducteurs) vise à identifier les séquences d'équipements et d'étapes qui maximisent le rendement (yield) et et par consequent minimisent les défauts. Une "route" représente le chemin exact qu'un lot/wafer suit à travers les différentes etapes de fabrication.

### 1.2 Enjeux industriels
Dans le cadre de la technologie Z028 au cours de la route jusqu'au PT intermediaire on retrouve:
- **Complexité des fabs** : 134 opérations, 222 étapes, 1 a 5 équipements par étape
- **Impact économique** : 1% à 2% d'amélioration du yield serait non negligable
- **Variabilité équipements** : Performances différentes selon l'équipement choisi pour chaque étape
- **Contraintes temps réel** : Décisions de routage nécessaires en continu

## 2. Approches Algorithmiques pour l'Optimisation de Routes

### 2.1 Approches de Data Mining

#### 2.1.1 Règles d'Association (Approche retenue)
**Principe** : Identifier les patterns fréquents entre équipements et résultats qualité.

APRIORI : "Si ETCH-XXX01 ET LITHO-XXX2 alors 89% chance défaut"
→ Explication claire et actionnable

FP-Growth : Résultat identique mais processus "boîte noire"  
→ Plus difficile à expliquer aux experts métier

Volume Données vs Performance Réelle
l'approche fenêtre glissante **6 semaines** change la donne :
|Algorithme|Dataset Complet (25M)|Fenêtre 6 sem (~2M)|Verdict|
|----------|---------------------|--------------------|-------|
|APRIORI|Lent (~20 min)|Acceptable (~2.5 min)|Optimal|
|FP-Growth|Rapide (~5 min)|Très rapide (~1 min)|Excessif|


## 6. Perspectives et Améliorations Futures

### 6.1 Hybridation des approches
- Combinaison des règles d'association avec des méthodes ML pour améliorer la précision et la robustesse des recommandations.

### 6.2 Temps Réel et Streaming
- Adaptation Kafka pour données équipements en temps réel

## Implémentation code détaillé

---
Pipeline Unity Catalog
---
<img width="1615" height="765" alt="image" src="https://github.com/user-attachments/assets/dddd34db-a18e-42af-b4d7-e13d26724eeb" />

---

#  Job et Pipeline Databricks

## **Vue d'ensemble du Job**

###  **Principe clé :**
- **Un seul notebook** → Deux comportements différents
- **Paramètre `type_route`** → Détermine le flux de données
- **Logique conditionnelle** → `"good"` vs `"bad"` routes
- **Planification** → Execution par période.

---

| Phase | Processus | Durée |
|-------|-----------|--------|
| **Phase 1** | Data Prep | 6 min |
| **Phase 2** | APRIORI | 20 min |
| **Phase 3** | Validation | 30 sec |

# **Job Principal : `PT_Z028_Worst_Route`**

[Section Cluster et configuration inchangée...]

### **Performance**

| Tâche | Durée | Notes |
|:-------:|:-------:|:-------:|
| **Data_Preparation** | 10 min | Traitement Unity Catalog |
| **AR_BAD/GOOD** | 20 min | APRIORI fenêtre glissante |
| **Validation** | 6 min | Tests statistiques |
| **Total Pipeline** | **~30 min** | Auto-scaling |


# Phase 1 : DATA_PREP.py - Rapport d'état des lieux

## Vue d'ensemble

Le script `DATA_PREP.py` constitue la phase de préparation des données du projet Z028. Il traite les données brutes du PT intermediaire pour produire les tables nécessaires à l'analyse des règles d'association.

*Note technique : Une version d'initialisation `Data_Preparation_initialisation_6_month_data.py` existe avec des clauses WHERE étendues à 182 jours. Cette version est utilisée **une seule fois** lors de l'initialisation initiale des tables de référence avec un historique de 6 mois de données de production.*

### 1. Extraction SQL depuis Unity Catalog

#### LOT_LIST_INFORMATION_DF - Liste des lots éligibles
```sql
SELECT pt_kdf_lot_number AS LOT, 
       pt_kdf_cam_location AS LOCATION, 
       pt_kdf_product_code AS PRODUCT,
       pt_kdf_test_start_datetime AS START_DATE,
       pt_kdf_test_end_datetime AS END_DATE,
       pt_kdf_spec_name AS SPEC_NAME,
       pt_kdf_spec_version AS SPEC_VERSION

FROM sem1.pt_kdf_lot_normalized 
WHERE pt_kdf_cam_location = 'M2SICN-ADT02' 
  AND fab_name = 'CROLLES 300'
  AND LEFT(pt_kdf_product_code, 5) = 'KVB98'
  AND pt_kdf_test_end_datetime > (
      SELECT MAX(T84_TEST_DATE) 
      FROM mds_prod_gold_experiment.datasciences_dev.pt_z028_input_bad_for_ar
  )
  AND length(pt_kdf_lot_number) <= 7
```
**Filtres appliqués** :
- **Localisation** : M2SICN-ADT02 (équipement de test spécifique)
- **Fab** : CROLLES 300 uniquement
- **Produit** : Code produit commençant par 'KVB98' (technologie Z028)
- **Période** : Depuis la dernière date traitée dans la table de sortie
- **Format lot** : Maximum 7 caractères

**Nouveau mécanisme** : Le script vérifie maintenant s'il y a des données à traiter via un flag `can_process` qui est mis à `False` si moins de 2 lots sont disponibles ou si les tables d'entrée sont vides.


# Phase 2 : AR_FAMILIES_EQPT.py - Analyse des Règles d'Association

##  Objectif
Identifier les équipements problématiques par famille de paramètres en utilisant l'algorithme APRIORI sur des fenêtres glissantes de **6 semaines**.

## Données d'entrée
- **pt_z028_input_bad_for_ar** : Table de sortie de Data_Prep.py, dataset avec encodage équipements + indicateur BAD
- **Df_Family_Ref.csv** : Référentiel familles de paramètres
- **Période d'analyse** : 182 jours (6 mois) de données

## Pipeline principal

### 1. Préparation des données
- **Optimisation mémoire** : Conversion dtypes (downcast integers/floats, catégorisation strings)
- **Fenêtres glissantes** : Découpage en périodes de **6 semaines** avec décalage hebdomadaire
- **Filtrage par famille** : Sélection des étapes pertinentes selon le référentiel
- **Filtrage équipements constants** : Nouvelle fonction `filter_variable_equipment_steps` qui exclut les étapes où un seul équipement est utilisé

### 2. Analyse APRIORI par étape
Pour chaque (OPERATION, STEP) :
- **Encodage** : Colonnes `STEP_EQPT_[STEP]-[EQUIPMENT]` + `BAD`/`GOOD`
- **Filtrage fréquence** : Garder équipements présents >5% du temps
- **Support adaptatif** : `max(min_support, FIXED_MIN_OCCURRENCES / len(df))` pour capturer les règles sur les semaines avec peu de lots
- **APRIORI** : Recherche itemsets fréquents (min_support=0.005)
- **Règles d'association** : Calcul confidence, lift, support
- **Filtrage ciblé** : 
  - Règles avec conséquent = `BAD`/`GOOD`
  - **Antécédents multiples** : Jusqu'à 2 équipements (ex: "EQPT1 + EQPT2")
  - Confidence minimum : 0.30
  - Lift minimum : 1.10

### 3. Scoring composite amélioré
```python
# Normalisation des métriques (0-1)
conf_normalized = (confidence - min) / (max - min)
lift_normalized = (lift - 1) / (max - 1)  # lift-1 car lift=1 = indépendance
support_normalized = (support - min) / (max - min)

# Score composite pondéré
composite_score = 0.4 * conf_normalized + 0.4 * lift_normalized + 0.2 * support_normalized

# Pénalité pour patterns trop fréquents
frequency_penalty = 1 - (0.1 * support_normalized)
composite_score *= frequency_penalty
```

### 4. Agrégation temporelle
- **Gestion des combinaisons** : Nouvelle fonction `ranking_with_combinations` pour gérer les règles avec équipements multiples
- **Pivot** : Fenêtres en colonnes, scores en valeurs
- **Ranking final** : Rang de chaque équipement/combinaison par fenêtre et par étape

## Output
Deux tables stocker dans :
***mds_prod_gold_experiment.datasciences_dev***

- **pt_z028_ar_bad_route_results** : Resultats des regles d'associations pour chaque famille a travers les fenêtres de 6 semaines.
- **pt_z028_ar_good_route_results** : Resultats des regles d'associations pour chaque famille a travers les fenêtres de 6 semaines.
- **Structure** : FAMILY | OPERATION | STEP | EQPT | window_0 | window_1 | ... | window_n
- **Valeurs** : Rang de l'équipement (1 = le plus problématique/performant)

##  Résultat métier
**Identification des équipements et combinaisons d'équipements** les plus corrélés aux défauts/succès par famille, avec une évolution temporelle pour détecter les dérives.

→ **Prêt pour la validation** dans VALIDATION_AR_FAMILIES.py

# Phase 3 : VALIDATION_AR_FAMILIES - Validation des Routes avec Scoring Pondéré

## Objectif
Valider statistiquement les routes identifiées comme problématiques ou bonnes dans la Phase 2 en utilisant un **système de scoring pondéré dynamique** qui s'adapte au type de route analysé.

##  Évolution majeure : Configuration dynamique par type de route

### Principe du nouveau système
Le système utilise maintenant une configuration différente selon qu'on analyse une worst route (BAD) ou une golden route (GOOD) :

#### Configuration WORST ROUTE (BAD)
```python
{
    "min_appearances": 2,        # Détection rapide des problèmes émergents
    "min_score": 100,            
    "recency_weight": 1.5,       # 150% - Focus sur problèmes récents
    "continuity_weight": 0.15    # 15% - Continuité moins importante
}
```
**Justification** : Prioriser les problèmes récents et émergents qui nécessitent une action immédiate

#### Configuration GOLDEN ROUTE (GOOD)
```python
{
    "min_appearances": 3,        # Plus d'occurrences pour prouver la stabilité
    "min_score": 100,
    "recency_weight": 0.5,       # 50% - Récence modérée
    "continuity_weight": 0.5     # 50% - Focus sur stabilité/continuité
}
```
**Justification** : Identifier les équipements stables et performants dans la durée

### Système de scoring révisé
- **Score de base par rang** : `p_weight = [5, 2, 1, 0.5]` pour les rangs 1, 2, 3, 4+
- **Progression linéaire pour continuité** : `l × (1 + α × (l - 1))` avec α=0.20
- **Décroissance exponentielle pour récence** : λ=0.95 par défaut
- **Calcul par run** : Analyse des séquences continues d'apparitions

## Données d'entrée

### 1. Données équipements (Fenêtre 365 jours)
```python
df_EQPT_spark = spark.sql(f"""
    SELECT *
    FROM mds_prod_gold_experiment.datasciences_dev.pt_z028_input_{type_route.lower()}_for_ar
    WHERE T84_TEST_DATE BETWEEN DATE_SUB(CURRENT_DATE(), 365) AND CURRENT_DATE()
""")
```

### 2. Résultats AR les plus récents
```python
df_ar_result = spark.sql(f"""
    SELECT * 
    FROM mds_prod_gold_experiment.datasciences_dev.pt_z028_ar_{type_route.lower()}_route_results 
    WHERE Processed_Date_Job = ( 
        SELECT MAX(Processed_Date_Job) FROM ... 
    )
""")
```

## Pipeline de validation

### 1. Fonction `calculate_weighted_score()` - Calcul du score pondéré

Le score analyse les **runs** (séquences continues d'apparitions) :

1. **Extraction des runs** : Identification des séquences continues où l'équipement apparaît
2. **Score de base** : Pondération selon le rang moyen dans le run
3. **Bonus de récence** : Décroissance exponentielle basée sur la distance temporelle
4. **Bonus de continuité** : Progression linéaire favorisant les longues séquences
5. **Combinaison** : `base_score × run_length × weighted_recency + weighted_continuity`

### 2. Sélection des worst/best routes

**Critères de sélection** :
1. **Double filtrage** : 
   - Nombre d'apparitions minimum (2 pour BAD, 3 pour GOOD)
   - Score pondéré minimum (100 dans les deux cas)
2. **Sélection par OP_STEP** : Équipement avec le score pondéré maximum
3. **Ajustement par taille** : Entre 4 et 6 étapes retenues selon la taille de la famille

### 3. Tests statistiques

#### Sélection automatique du test selon la taille d'échantillon
- **n ≥ 30** : Test Z (proportions)
- **10 ≤ n < 30** : Test exact de Fisher
- **5 ≤ n < 10** : Test binomial exact
- **n < 5** : Pas de test (échantillon insuffisant)

## Output

### Table de sortie
**Nom** : `pt_z028_ar_{type_route}_validation_summary`  
**Localisation** : `mds_prod_gold_experiment.datasciences_dev`

### Structure DataFrame enrichie
| Colonne | Type | Description |
|:-------:|:----:|:------------|
| FAMILY | String | Famille de paramètres analysée |
| WORST_ROUTE_STEPS | Integer | Nombre d'étapes dans la route |
| MATCHES | Integer | Nombre d'équipements problématiques utilisés |
| WAFER_COUNT | Integer | Volume de wafers dans ce segment |
| BAD_RATE_PCT | Float | Taux de défaut observé (%) |
| BASELINE_PCT | Float | Taux moyen famille (%) |
| DELTA_PCT | Float | Impact relatif (+/-%) |
| P_VALUE | Float | Significativité statistique |
| TOP_5_EQUIPMENT | String | Top 5 équipements format "OP_STEP:EQPT(score)" |
| Processed_Date_Job | Date | Date d'exécution |

## Résultats et interprétation

### Exemple de sortie console
```
Traitement famille : NMOS-GO1-LVT
Paramètres appliqués :
  • Type de route : BAD
  • Pondération - Récence : 150%
  • Pondération - Continuité : 15%

Top 5 équipements :
  • O_STRIP|STRIP_DRY -> SGAMA04
    Score: 145.2, Apparitions: 8
    Pattern: strong_continuous, Max consécutif: 5 fenêtres
  • O_DEP_HK|DEP_HK -> TCENT11
    Score: 128.7, Apparitions: 6
    Pattern: moderate_continuous, Max consécutif: 3 fenêtres

Route finale : 4 étapes retenues

Baseline : 15.2%
  0/4 matches: 12.1% (-3.1%) [1234 wafers] p=0.023(Z)
  1/4 matches: 14.8% (-0.4%) [856 wafers] p=0.412(Z)
  2/4 matches: 18.7% (+3.5%) [423 wafers] p=0.008(Z)
  3/4 matches: 24.3% (+9.1%) [187 wafers] p<0.001(Fisher)
  4/4 matches: 31.2% (+16.0%) [52 wafers] p<0.001(Binomial)
```

### Impact des configurations dynamiques

| Aspect | WORST ROUTE (BAD) | GOLDEN ROUTE (GOOD) |
|:------:|:------------------:|:--------------------:|
| **Focus principal** | Problèmes récents/émergents | Stabilité long terme |
| **Récence** | 150% de poids | 50% de poids |
| **Continuité** | 15% de poids | 50% de poids |
| **Seuil apparitions** | 2 (détection rapide) | 3 (validation robuste) |
| **Résultat attendu** | Alertes précoces | Routes fiables |

## Impact

### 1. **Adaptation au contexte**
- **Worst route** : Détection rapide des dérives pour maintenance préventive
- **Golden route** : Identification des meilleures pratiques stables

### 3. **Traçabilité complète**
- Configuration explicite dans les logs
- Justification des choix de pondération

# Conclusion Global

