# projet-pybd

Modifier docker-compose.yml pour mettre vos répertoires à la place de $/ qui est le repertoire courrant. N'ounliez de faire `docker login` pour ne pas avoir de problemes avec les images.

1- Move boursorama dans docker/data
2- Lancer ./launch_project -o start pour commencer le loading du database
3- Aller sur localhost:8050 pour tester
4- Si le loading crash a cause de la connection ou de la machine, effacer les images et relancer le script ./launch_project -o start
5- Si localhost:8050 n'a pas de données alors que le loading est fini, lancer ./launch_project -o reload

## Analyzer

Les données sont les cours d'entreprises de différentes bourses relevées toutes les 10 mn. Un fihcier est un DataFrame stocké en mode pickle (pd.read_pickle) qui correspond à toutes les entreprises d'un marché à un instant donné par le titre du fichier.

Chaque annees est stockee au cours de l'__Analyzer__ dans la DataBase __SQL__.

## Dash

Dashboard représentant les données de la base __SQL__.
### Premiere partie:
Liste deroulante des entreprise: on peut choisir une ou plusieurs entreprises, reprentes avec leur nom et leur symbol.

### Deuxieme partie:
Graphique de l'evolution du cours de ou des entreprises selectionnées logarithmiquement au cours du temps. On peut cocher decocher le visuel des entreprises.

L'utilisateur peut choisir entre le visuel ligne ou graphique en chandelier.
On peut egalement choisir d'afficher ou non des les lignes de bollinger.
On peut aussi choisir les lignes moyennes mobiles.

En dessous grapique connectes des volumes d'echanges.

### Troisieme partie:
Tableau des donnees brutes de ou des entreprises selectionnées, chaque entreprise est dans un onglet selectionnable.
Plusieurs variables sont montrees:
- Date
- Min
- Max
- Début
- Fin
- Volume
- Moyenne
- Écart type


### Contact:

N'hesitez pas à nous contacter pour plus d'informations:

 - Discord
    - heldeee
    - dangphuhung
    - 4xoloto

- Mail
    - leo.devin@epita.fr
    - phu-hung.dang@epita.fr
    - alexandre1.huynh@epita.fr
