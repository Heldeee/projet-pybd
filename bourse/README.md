Modifier docker-compose.yml pour mettre vos répertoires à la place de /home/ricou

1- Move boursorama dans docker/data
2- Lancer ./launch_project -o start pour commencer le loading du database
3- Aller sur localhost:8050 pour tester
4- Si le loading crash a cause de la connection ou de la machine, effacer les images et relancer le script ./launch_project -o start
5- Si localhost:8050 n'a pas de données alors que le loading est fini, lancer ./launch_project -o reload