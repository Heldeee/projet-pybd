# Projet-PYBD

Modifier docker-compose.yml pour mettre vos répertoires à la place de $/ qui est le repertoire courrant. N'oubliez de faire `docker login` pour ne pas avoir de problemes avec les images.

## Telechargement des données

Afin de telecharger boursorama.tar, utiliser ce lien:

https://acxpcq.db.files.1drv.com/y4m1rZorPP2kN8W6PGekjlKsY5CbZ4L2jhg4iXMGWWhe8xMIbmnDBYlZYJTnrH4_T_eCrjM_Ree83dgW-GkXhuyMHEuM5jMXnO4qewwjyOl_jXJMxiNvLI0QXu7Nau9Y4ynzQVg3TaEkQJTH9x6YP9FijI04pJsgGfhk-vCu2nWDqeA5CsFyfQG5Ks0JY6AyO918TiEm_IOxtt4yIiHsdhrQQ ou sur le site du cours.

## Installation

- Move boursorama.tar dans docker/data, puis le decompresser `sudo tar -xvf bourosrama.tar`
- Lancer `./launch_project -o start` pour commencer le loading du database, lorsque vous voyer le Dashboard crash avec code 3, Ctlr+C pour arreter le loading
- Relancer
- Si le loading crash a cause de la connexion ou de la machine, effacer les images et `/docker/timescaldb/` et relancer le script `./launch_project -o start`
- Si localhost:8050 n'a pas de données alors que le loading est fini, lancer `./launch_project -o reload`

## Analyzer

Les données sont les cours d'entreprises de différentes bourses relevées toutes les 10 mn. Un fihcier est un DataFrame stocké en mode pickle (pd.read_pickle) qui correspond à toutes les entreprises d'un marché à un instant donné par le titre du fichier.

Chaque annee est stockee au cours de l'__Analyzer__ dans la DataBase __Timescaldb__.

## Dash

Dashboard représentant les données de la base __Timescaldb__.
### Premiere partie
Liste deroulante des entreprise: on peut choisir une ou plusieurs entreprises, reprentes avec leur nom et leur symbol.

### Deuxieme partie
Graphique de l'evolution du cours de ou des entreprises selectionnées logarithmiquement au cours du temps. On peut cocher decocher le visuel des entreprises.

L'utilisateur peut choisir entre le visuel ligne ou graphique en chandelier.
On peut egalement choisir d'afficher ou non des les lignes de bollinger.
On peut aussi choisir les lignes moyennes mobiles.

En dessous grapique connectes des volumes d'echanges.

### Troisieme partie
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

### Feature Bonus

Notre feature bonus est le telechargement de la `Data Table` sous format csv. Il suffit de se placer sur la `Data Table` de l'entreprise puis de clicker sur le bouton `Download CSV`.

D'autre features sont disponible comme le choix de date au format de plage de temps, la ligne `Average` pour une entreprise selectionnee et le filtrage des enteprises par appartenance aux differents marches.

### Contact

N'hesitez pas à nous contacter pour plus d'informations ou lors de problemes d'initialisation ou de fonctionnement du projet :

 - Discord
    - heldeee
    - dangphuhung
    - 4xoloto

- Mail
    - leo.devin@epita.fr
    - phu-hung.dang@epita.fr
    - alexandre1.huynh@epita.fr
