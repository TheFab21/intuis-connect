# Énergie et retard du cloud : analyse et structure cible

Analyse du 17 novembre 2026, à partir de `fix/hourly-scheduler-event-loop`.
Contexte : le cloud Intuis ne confirme une heure de consommation qu'avec un
retard variable, parfois de plusieurs heures, parfois de plusieurs jours selon
la pièce. Toute la chaîne énergie doit être pensée autour de cette latence.

## 0. Correction préalable

`_replace_external_in_transaction` (`recorder_import.py:216-235`) crée la ligne
`StatisticsMeta` au premier passage, puis se contente de **valider** `source`,
`unit_of_measurement` et `has_sum` sur une ligne existante. Aucun `UPDATE` de
métadonnées n'existe dans le paquet.

Conséquence : les séries externes n'héritent **jamais** d'un changement de
métadonnées. Un correctif sur `unit_class`, un renommage de pièce, un
changement de devise ne touchent que les séries créées après coup. Les séries
existantes gardent leurs valeurs d'origine jusqu'à suppression manuelle.

C'est la première chose à corriger : sans cela, aucun correctif de métadonnées
n'a d'effet sur une installation en service.

## 1. Le constat de fond : deux représentations de la même mesure

| | Capteur d'entité | Série externe |
|---|---|---|
| Identifiant | `sensor.<pièce>_energy` | `intuis_connect:energy_<hash>` |
| Écriture | recorder, depuis les états | transaction directe |
| Granularité | journalière | horaire |
| Sémantique | `TOTAL_INCREASING`, maximum du jour logique | somme révisable |
| Passé | non réécrivable sans ancre | réécrivable librement |

Les deux décrivent la même consommation, par deux chemins qui ne se
réconcilient jamais. C'est la source de la plupart des défauts qui suivent, et
c'est ce qui a produit la corruption de cumul observée lors d'une bascule de
version.

Or **la représentation journalière est redondante** : Home Assistant agrège
lui-même une série horaire en jour, semaine et mois. Le tableau de bord Énergie
ne consomme rien d'autre. Maintenir une seconde série journalière n'apporte
aucune vue que l'horaire ne donne pas, et coûte toute la fragilité ci-dessous.

## 2. Les défauts, par gravité

### 2.1 La fenêtre périodique est ancrée sur la dernière heure écrite

`async_update_window` (`hourly_stats.py:72-82`) part de `end - 48 h`, puis
recule jusqu'à `dernière heure + 1 h` pour la pièce la plus en retard, borné à
7 jours. `async_last_hour` renvoie la ligne **la plus récente**.

Quand la série est à jour, la fenêtre vaut donc exactement les 48 dernières
heures. **Une heure confirmée par le cloud plus de 48 h après coup n'est jamais
rattrapée** : elle est derrière la dernière ligne écrite, donc hors fenêtre pour
toujours. C'est précisément le cas que la latence Intuis produit.

L'ancrage devrait porter sur le **plus ancien trou**, pas sur la dernière
ligne.

### 2.2 L'horizon de clôture est de quinze minutes

`closed_hour_end` (`hourly_series.py:27-31`) considère une heure close quinze
minutes après sa fin. Avec un cloud en retard, la quasi-totalité des heures
récentes de la fenêtre sont absentes à chaque passage. Elles seront comblées
plus tard, mais chaque cycle refait le travail pour rien.

Cet horizon devrait être configurable et refléter la latence réelle du
fournisseur.

### 2.3 Une période sans mesure est traitée comme une erreur

`stage_hourly` lève `APIError("No measured hour in the period")` quand le cloud
ne renvoie rien. Pour une pièce dont les données ont plus de 48 h de retard,
c'est le cas **à chaque cycle**. Le résultat passe en `partial` ou `failed`, la
liste d'erreurs se remplit, et `manager.last_error` affiche en permanence une
panne qui n'en est pas une.

« Aucune heure confirmée pour l'instant » est un état normal, pas une erreur.

### 2.4 Le total maison est tout-ou-rien

`sum_hourly` (`hourly_series.py:70-79`) ne somme que les heures confirmées par
**toutes** les pièces. Avec des latences échelonnées par pièce, l'intersection
est chroniquement vide sur les heures récentes : le total maison reste
`partial` en permanence, comme observé.

Deux sorties possibles : renoncer au total maison et laisser le tableau de bord
Énergie sommer les sources, ou maintenir un filigrane propre au total qui
n'avance que sur les heures complètes, sans marquer le reste en échec.

### 2.5 Le capteur journalier se bat contre le modèle de Home Assistant

`IntuisEnergySensor` (`sensor.py:261`) est `TOTAL_INCREASING` et publie le
maximum du jour logique. Trois conséquences :

- une révision à la baisse par le cloud est invisible, le maximum ne redescend
  jamais dans la journée ;
- une révision à la hausse d'un jour clos n'est jamais reflétée, l'état ne
  décrit que le jour courant ;
- à la remise à zéro, HA interprète la baisse comme un redémarrage de compteur
  et poursuit la somme, ce qui rend toute incohérence de source indétectable.

Un compteur qui dépend de données révisables en amont ne peut pas être un
`TOTAL_INCREASING` alimenté par les états.

### 2.6 L'import journalier exige une ancre vivante

`_replace_in_transaction` (`recorder_import.py:155-195`) refuse l'import s'il
n'existe pas de statistique court terme postérieure à la période. La contrainte
est juste — on ne peut pas réécrire le passé d'un `TOTAL_INCREASING` sans
ancre — mais elle n'existe que parce qu'on écrit dans l'identifiant d'une
entité. Les séries externes n'ont pas ce problème, et le code le dit lui-même.

### 2.7 Les coûts ne couvrent que les heures de la passe courante

Dans `async_import_hourly_history`, `hourly_costs` n'est appliqué qu'à
`confirmed[room_id]`, les heures confirmées **pendant ce passage**. Une heure
dont l'énergie était connue mais dont le prix manquait à ce moment ne sera
jamais recalculée automatiquement : il faut appeler `recalculate_cost_history`
à la main.

Le coût devrait suivre son propre filigrane : toute heure ayant une énergie et
pas de coût valide est à (re)calculer.

## 3. Structure cible

### 3.1 Une seule source de vérité

Les séries externes horaires deviennent la seule histoire. Le capteur d'entité
redevient ce qu'il est bien : un **affichage temps réel**.

Deux façons de le faire, et il faut en choisir une **sans jamais les mélanger** :

- le capteur garde `state_class` et le recorder compile son histoire, mais
  **rien** n'écrit jamais dans son `statistic_id` ;
- ou le capteur perd `state_class` et n'a aucune statistique — et alors il ne
  faut écrire aucune série sous son identifiant, sinon Home Assistant signale
  une classe d'état disparue.

C'est exactement l'erreur commise par une version antérieure : retirer la
`state_class` **et** écrire les statistiques long terme sous l'identifiant de
l'entité. L'une ou l'autre, jamais les deux.

La première option est préférable : elle garde l'entité utilisable dans les
cartes natives et les automatisations, sans contrainte sur le passé.

### 3.2 Un filigrane par série

Chaque série porte deux repères persistés :

- `confirmed_through` : toutes les heures jusque-là sont définitivement closes,
  plus jamais relues ;
- `open_from` : la plus ancienne heure encore susceptible d'arriver.

Le cycle relit `[open_from, closed_hour_end(now))`, borné par une profondeur
maximale configurable. `confirmed_through` n'avance que lorsqu'une heure est
confirmée **et** plus ancienne que l'horizon de décantation. Une heure qui
arrive en retard est donc rattrapée tant qu'elle est devant `confirmed_through`,
quelle que soit la date de la dernière ligne écrite.

Cela remplace à la fois `UPDATE_WINDOW`, `CATCH_UP_WINDOW` et le `min(last+1h)`
actuel, et règle 2.1 d'un seul coup.

### 3.3 Un horizon de décantation explicite

Une option, par défaut généreuse (24 h), qui dit au bout de combien de temps on
considère qu'une heure absente ne viendra plus. Elle pilote l'avancée de
`confirmed_through` et remplace les quinze minutes en dur.

### 3.4 Métadonnées idempotentes

À chaque écriture, les champs mutables — `name`, `unit_class`, `mean_type`,
`has_mean` — sont mis à jour sur la ligne existante. Les champs immuables
— `source`, `unit_of_measurement`, `has_sum` — restent validés et refusés en cas
d'écart, comme aujourd'hui.

### 3.5 Les coûts pilotés par leur propre filigrane

Le calcul de coût ne dépend plus de la passe d'énergie. Il parcourt les heures
ayant une énergie confirmée et pas de coût valide, applique le prix de l'heure,
et avance son filigrane. Un prix indisponible laisse l'heure ouverte au lieu de
la perdre.

## 4. Contraintes de Home Assistant à respecter

- **Toute écriture de statistiques passe par le fil du recorder.** La
  `RecorderTask` acquittée actuelle est la bonne approche, à conserver.
- **Les statistiques externes doivent porter un identifiant `domaine:xxx`** et
  ne jamais ressembler à un `entity_id`.
- **`unit_class` doit valoir exactement `energy`** pour qu'une série apparaisse
  dans les sélecteurs du tableau de bord Énergie, qui filtrent sur
  `includeUnitClass=["energy"]`.
- **Les clés de `StatisticMetaData` varient selon la version du recorder.**
  Chaque champ doit être conditionné à son propre support, jamais à celui d'un
  autre.
- **Écrire la ligne de métadonnées directement contourne le cache du
  gestionnaire de HA.** C'est délibéré ici pour que le cache n'apprenne pas un
  identifiant qu'un `ROLLBACK` effacerait, mais cela impose de rester cohérent
  avec le schéma à la main.
- **Home Assistant agrège lui-même** une série horaire en jour, semaine et
  mois. Aucune série journalière n'est nécessaire.

## 5. Migration suggérée

1. **Mise à jour des métadonnées existantes.** Sans elle, rien de ce qui suit
   n'atteint une installation en service.
2. **Filigranes et horizon de décantation**, en gardant les séries existantes.
   Gain immédiat sur les trous dus à la latence.
3. **Statut honnête** : « aucune heure confirmée » cesse d'être une erreur.
4. **Coûts découplés**, pilotés par leur filigrane.
5. **Retrait de la voie journalière** : plus aucune écriture dans
   l'identifiant des entités, l'import journalier devient un import horaire sur
   une profondeur choisie. C'est l'étape qui supprime l'exigence d'ancre vivante
   et le risque de cumul corrompu.

Les quatre premières sont additives et sans rupture. La cinquième demande une
décision sur le sort de l'historique déjà écrit sous les identifiants
d'entités.
